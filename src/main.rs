use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryStreamExt;
use google_gmail1::api::Message;
use google_gmail1::{api::Scope, hyper_rustls, hyper_util, yup_oauth2, Gmail};
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Pool, Row, Sqlite, SqliteExecutor, Transaction};

type GmailHub =
    Gmail<hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>>;

lazy_static! {
    static ref EMAIL_RE_1: Regex =
        Regex::new(r"^[^<]*<?([\w\-\.]+@([\w-]+\.)+[\w-]{2,4}).*$").unwrap();
    static ref EMAIL_RE_2: Regex = Regex::new(r"^([\w\-\.]+@([\w-]+\.)+[\w-]{2,4})$").unwrap();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO: use tokio::spawn and sqlite transactions to make fetching concurrent
    // TODO: there's a rate limit on google's side, so we should have some kind of backpressure
    // Also add DB schema and migrations
    let options = SqliteConnectOptions::from_str("sqlite://./stats.db")?;
    // WAL mode should be much faster for concurrent reads and writes
    // .journal_mode(SqliteJournalMode::Wal)
    // Synchronous mode is OK because a transaction may roll back during a crash, however
    // all mail listings are re-fetched during each run.
    // .synchronous(SqliteSynchronous::Normal)
    // .shared_cache(true);

    // let pool = Pool::<Sqlite>::connect_with(options).await?;
    let pool = SqlitePoolOptions::new()
        .max_connections(100)
        .connect_with(options)
        .await?;

    // Read application OAuth secret from a file.
    let secret = yup_oauth2::read_application_secret("credentials.json")
        .await
        .expect("credentials.json");

    // Create an authenticator that uses an InstalledFlow to authenticate. The
    // authentication tokens are persisted to a file named tokencache.json. The
    // authenticator takes care of caching tokens to disk and refreshing tokens once
    // they've expired.
    let auth = yup_oauth2::InstalledFlowAuthenticator::builder(
        secret,
        yup_oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk("tokencache.json")
    .build()
    .await
    .unwrap();

    let mut hub = Gmail::new(
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()?
                .https_or_http()
                .enable_http2()
                .build(),
        ),
        auth,
    );

    // Retry transient failures with exponential backoff; fail fast on everything else.
    let mut retries = 0;
    let mut resume_token: Option<String> = None;
    loop {
        let token_before_attempt = resume_token.clone();
        match work(&pool, &mut hub, &mut resume_token).await {
            Ok(()) => break,
            Err(err) if !is_transient(&err) => {
                return Err(err.context("giving up: error is not retryable"));
            }
            Err(err) => {
                // If this attempt advanced the resume point (i.e. fully processed
                // at least one page) before failing, the previous errors were
                // intermittent, not persistent — start the budget over so
                // MAX_RETRIES bounds *consecutive* failures, not failures
                // accumulated over the whole run.
                if resume_token != token_before_attempt {
                    retries = 0;
                }
                retries += 1;
                if retries > MAX_RETRIES {
                    return Err(err.context(format!("giving up after {MAX_RETRIES} retries")));
                }
                let delay = backoff_delay(retries);
                println!(
                    "Transient error (attempt {retries}/{MAX_RETRIES}), retrying in {delay:?}: {err:?}"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }

    Ok(())
}

const MAX_RETRIES: u32 = 3;

/// Exponential backoff (1s, 2s, 4s, ... capped at 32s) plus up to 1s of jitter.
fn backoff_delay(attempt: u32) -> Duration {
    let base = Duration::from_secs(1 << attempt.saturating_sub(1).min(5));
    let jitter_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_millis()))
        .unwrap_or(0);
    base + Duration::from_millis(jitter_ms)
}

/// Only transient failures are worth retrying: SQLite busy/locked (the deadlocks
/// that motivated the retry loop) and Gmail API rate limits / server errors.
/// Auth failures, other 4xx responses, IO errors, etc. fail fast.
fn is_transient(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(sqlx_err) = cause.downcast_ref::<sqlx::Error>() {
            return match sqlx_err {
                sqlx::Error::Database(db_err) => {
                    // SQLITE_BUSY (5) and SQLITE_LOCKED (6); extended result codes
                    // (e.g. SQLITE_BUSY_SNAPSHOT = 517) keep the primary code in
                    // the low byte.
                    db_err
                        .code()
                        .and_then(|code| code.parse::<i64>().ok())
                        .is_some_and(|code| matches!(code & 0xff, 5 | 6))
                }
                // Timed out waiting for a pool connection while the DB is busy.
                sqlx::Error::PoolTimedOut => true,
                _ => false,
            };
        }
        if let Some(gmail_err) = cause.downcast_ref::<google_gmail1::Error>() {
            return match gmail_err {
                // Non-success HTTP responses whose body parses as Google's JSON
                // error envelope (the normal case for Gmail 429s and 5xxs) are
                // surfaced as `BadRequest(value)`, not `Failure`; the HTTP
                // status is the numeric `error.code` field in the envelope.
                google_gmail1::Error::BadRequest(value) => value
                    .pointer("/error/code")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|code| code == 429 || (500..=599).contains(&code)),
                // Non-JSON error responses (e.g. an HTML 502 from a proxy).
                google_gmail1::Error::Failure(response) => {
                    let status = response.status();
                    status.as_u16() == 429 || status.is_server_error()
                }
                _ => false,
            };
        }
    }
    false
}

async fn work(
    pool: &Pool<Sqlite>,
    hub: &mut GmailHub,
    resume_token: &mut Option<String>,
) -> anyhow::Result<()> {
    // Fetch 500 messages at a time, starting from the last page we fully
    // processed (so a retry doesn't re-list the whole mailbox from page one).
    loop {
        let mut call = hub
            .users()
            .messages_list("me")
            .max_results(500)
            .include_spam_trash(false);
        if let Some(token) = resume_token.as_deref() {
            call = call.page_token(token);
        }
        let result = call.doit().await?;

        let next_page_token = result.1.next_page_token;
        parse_messages(pool, result.1.messages.unwrap_or_default(), hub).await?;

        // Only advance the resume point once the page has been fully processed.
        *resume_token = next_page_token;
        if resume_token.is_none() {
            return Ok(());
        }
    }
}

async fn parse_messages(
    pool: &Pool<Sqlite>,
    messages: Vec<Message>,
    hub: &mut GmailHub,
) -> anyhow::Result<()> {
    // Then fetch each individual message and increment the counter for the sender.
    // let mut handles = Vec::new();
    // TODO: this results in DB deadlocks :(
    for message_meta in messages {
        let pool = pool.clone();
        let hub = hub.clone();
        // let handle = task::spawn(async move {
        // Begin a new transaction for each message, to avoid concurrent reads/writes on the same message IDs.
        let mut tx = pool.begin().await?;
        if !seen_mail(
            message_meta.id.as_ref().expect("message missing id"),
            &mut *tx,
        )
        .await?
        {
            let message = hub
                .users()
                .messages_get("me", &message_meta.id.expect("message missing id"));

            let message = message.add_scope(Scope::Readonly);

            let message = message.doit().await?.1;
            println!(
                "sender: {:?}",
                message
                    .clone()
                    .payload
                    .unwrap_or_default()
                    .headers
                    .unwrap_or_default()
                    .iter()
                    .filter(|header| header.name == Some("From".to_string()))
                    .collect::<Vec<_>>()
            );

            mark_seen(&message, &mut *tx).await?;
            increment_sender_mails(&message, &mut tx).await?;
        }
        tx.commit().await?;

        // Ok::<(), anyhow::Error>(())
        // });
        // handles.push(handle);
    }

    // join each handle
    // for handle in handles {
    //     handle.await??;
    // }

    Ok(())
}

async fn seen_mail(message_id: &str, executor: impl SqliteExecutor<'_>) -> anyhow::Result<bool> {
    let mut res = sqlx::query("SELECT count(1) AS ct FROM seen_mails WHERE mail_id = ?")
        .bind(message_id)
        .fetch(executor);
    while let Some(row) = res.try_next().await? {
        let count: u32 = row.try_get("ct")?;
        if count > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn mark_seen(message: &Message, executor: impl SqliteExecutor<'_>) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO seen_mails (mail_id) VALUES (?)")
        .bind(message.id.as_ref().expect("message missing id"))
        .execute(executor)
        .await?;
    Ok(())
}

async fn increment_sender_mails(
    message: &Message,
    tx: &mut Transaction<'_, Sqlite>,
) -> anyhow::Result<()> {
    let sender = cleanup_sender(get_sender(message)?);
    let row = sqlx::query("SELECT mails_sent FROM senders WHERE sender = ?")
        .bind(&sender)
        .fetch_optional(&mut **tx)
        .await?;
    if row.is_none() {
        // no match
        sqlx::query("INSERT INTO senders (sender, mails_sent) VALUES (?, 1)")
            .bind(&sender)
            .execute(&mut **tx)
            .await?;

        return Ok(());
    }

    let row = row.unwrap();
    let mut mails_sent = 0;
    let count = row.try_get("mails_sent");

    let count = count?;

    if count > 0 {
        mails_sent = count;
    }

    mails_sent += 1;
    sqlx::query("UPDATE senders SET mails_sent = ? WHERE sender = ?")
        .bind(mails_sent)
        .bind(&sender)
        .execute(&mut **tx)
        .await?;

    Ok(())
}

// Attempt to extract a formatted email address, or just return the original value
fn cleanup_sender(sender: String) -> String {
    let mut clean_sender = sender.clone();
    if sender.contains("<") {
        for cap in EMAIL_RE_1.captures_iter(&sender) {
            clean_sender = cap[1].to_string();
        }
    } else {
        for cap in EMAIL_RE_2.captures_iter(&sender) {
            clean_sender = cap[1].to_string();
        }
    }

    clean_sender
}

fn get_sender(message: &Message) -> anyhow::Result<String> {
    let mut from_headers = message
        .clone()
        .payload
        .unwrap_or_default()
        .headers
        .unwrap_or_default()
        .iter()
        .filter(|header| header.name == Some("From".to_string()))
        .cloned()
        .collect::<Vec<_>>();

    if from_headers.is_empty() {
        from_headers = message
            .clone()
            .payload
            .unwrap_or_default()
            .headers
            .unwrap_or_default()
            .iter()
            .filter(|header| header.name == Some("FROM".to_string()))
            .cloned()
            .collect::<Vec<_>>();

        // TODO: lol this is dumb, should have a Vec<String> of headers instead of this weird mess
        if from_headers.is_empty() {
            from_headers = message
                .clone()
                .payload
                .unwrap_or_default()
                .headers
                .unwrap_or_default()
                .iter()
                .filter(|header| header.name == Some("Return-Path".to_string()))
                .cloned()
                .collect::<Vec<_>>();

            if from_headers.is_empty() {
                println!("weird email without from header: {:?}", message);
                return Ok("".to_string());
            }
        }
        return Ok(from_headers[0]
            .value
            .as_ref()
            .expect("expected sender for mail")
            .to_string());
    }

    Ok(from_headers[0]
        .value
        .as_ref()
        .expect("expected sender for mail")
        .to_string())
}

#[cfg(test)]
mod tests {
    use super::is_transient;
    use serde_json::json;

    fn envelope(code: u64) -> anyhow::Error {
        // Google's standard JSON error envelope, as parsed into
        // `google_gmail1::Error::BadRequest` by the generated doit() code.
        anyhow::Error::new(google_gmail1::Error::BadRequest(json!({
            "error": { "code": code, "message": "boom", "status": "UNKNOWN" }
        })))
    }

    #[test]
    fn json_429_and_5xx_are_transient() {
        assert!(is_transient(&envelope(429)));
        assert!(is_transient(&envelope(500)));
        assert!(is_transient(&envelope(503)));
    }

    #[test]
    fn json_4xx_other_than_429_is_not_transient() {
        assert!(!is_transient(&envelope(400)));
        assert!(!is_transient(&envelope(401)));
        assert!(!is_transient(&envelope(403)));
        assert!(!is_transient(&envelope(404)));
    }

    #[test]
    fn bad_request_without_error_code_is_not_transient() {
        let err = anyhow::Error::new(google_gmail1::Error::BadRequest(json!({
            "message": "no envelope here"
        })));
        assert!(!is_transient(&err));
    }

    #[test]
    fn transient_cause_is_found_through_context_chain() {
        let err = envelope(429).context("while listing messages");
        assert!(is_transient(&err));
    }
}
