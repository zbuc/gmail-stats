use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use futures::{stream, TryStreamExt};
use google_gmail1::api::Message;
use google_gmail1::{api::Scope, hyper_rustls, hyper_util, yup_oauth2, Gmail};
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Connection, Pool, Row, Sqlite, SqliteConnection, SqliteExecutor, Transaction};
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use tokio::time::{Interval, MissedTickBehavior};

type GmailHub =
    Gmail<hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>>;

/// Default for how many messages_get calls may be in flight at once.
/// Override at runtime with the GMAIL_STATS_FETCH_CONCURRENCY env var.
const DEFAULT_FETCH_CONCURRENCY: usize = 8;
/// Default minimum spacing between Gmail API calls, in milliseconds. Gmail's
/// per-user quota is ~250 quota units/sec and messages.get costs 5 units, so
/// ~50 requests/sec is the ceiling; 25ms spacing (~40 req/sec) stays
/// comfortably under it. Override at runtime with the GMAIL_STATS_RATE_LIMIT_MS
/// env var (useful for projects with a lower per-user quota).
const DEFAULT_RATE_LIMIT_MS: u64 = 25;
/// How many times to retry a single API call (messages.list or messages.get)
/// on a rate-limit response.
const MAX_FETCH_RETRIES: u32 = 5;

/// A fetched message's result, sent to the single DB writer task.
struct SeenMail {
    message_id: String,
    sender: String,
}

lazy_static! {
    static ref EMAIL_RE_1: Regex =
        Regex::new(r"^[^<]*<?([\w\-\.]+@([\w-]+\.)+[\w-]{2,4}).*$").unwrap();
    static ref EMAIL_RE_2: Regex = Regex::new(r"^([\w\-\.]+@([\w-]+\.)+[\w-]{2,4})$").unwrap();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let fetch_concurrency: usize =
        env_or("GMAIL_STATS_FETCH_CONCURRENCY", DEFAULT_FETCH_CONCURRENCY)?;
    anyhow::ensure!(
        fetch_concurrency >= 1,
        "GMAIL_STATS_FETCH_CONCURRENCY must be at least 1"
    );
    let rate_limit_ms: u64 = env_or("GMAIL_STATS_RATE_LIMIT_MS", DEFAULT_RATE_LIMIT_MS)?;
    anyhow::ensure!(
        rate_limit_ms >= 1,
        "GMAIL_STATS_RATE_LIMIT_MS must be at least 1"
    );

    let options = SqliteConnectOptions::from_str("sqlite://./stats.db")?
        // Create the database file on first run; init_schema below creates the
        // tables, so a fresh install needs no manual setup.
        .create_if_missing(true)
        // WAL mode allows the seen-mail reads to proceed concurrently with the
        // single writer task's commits.
        .journal_mode(SqliteJournalMode::Wal)
        // Synchronous mode is OK because a transaction may roll back during a crash, however
        // all mail listings are re-fetched during each run.
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));

    // Small pool used only for the read-side seen-mail checks; all writes go
    // through the single writer task below, which eliminates writer-vs-writer
    // deadlocks entirely.
    let pool = SqlitePoolOptions::new()
        .max_connections(fetch_concurrency as u32)
        .connect_with(options.clone())
        .await?;

    // The writer task owns the only connection that ever writes.
    let mut writer_conn = SqliteConnection::connect_with(&options).await?;
    init_schema(&mut writer_conn).await?;
    let (mut write_tx, write_rx) = mpsc::channel::<SeenMail>(100);
    let mut writer_handle = task::spawn(db_writer(writer_conn, write_rx));

    // Simple client-side rate limiter: each API call waits for the next tick.
    let mut interval = tokio::time::interval(Duration::from_millis(rate_limit_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let rate_limiter = Arc::new(Mutex::new(interval));

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

    let hub = Gmail::new(
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new()).build(
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()?
                .https_or_http()
                .enable_http2()
                .build(),
        ),
        auth,
    );

    // Retry a failed run a few times, backing off exponentially between
    // attempts (see the sleep at the bottom of the loop body).
    let mut retries = 0;
    loop {
        match work(&pool, &hub, &write_tx, &rate_limiter, fetch_concurrency).await {
            Ok(()) => break,
            Err(e) => {
                retries += 1;
                if retries > 3 {
                    return Err(e.context("too many retries"));
                }
                println!("Error encountered, retrying: {:?}", e);
            }
        }

        // Quiesce the writer before retrying: close the channel and wait for
        // the writer to drain and commit every queued result. Without this
        // barrier the retry's seen-mail checks race the writer's backlog —
        // a message that is queued but not yet committed reads as unseen and
        // gets fetched, sent, and counted a second time. Awaiting the writer
        // here guarantees the retry observes every prior result. This also
        // covers the case where the writer task itself died (e.g. a transient
        // "database is locked"): surface its error and spawn a fresh writer
        // so the retry has a chance of working.
        drop(write_tx);
        if let Err(e) = writer_handle.await? {
            println!("DB writer task failed, restarting it: {:?}", e);
        }
        let writer_conn = SqliteConnection::connect_with(&options).await?;
        let (tx, rx) = mpsc::channel::<SeenMail>(100);
        write_tx = tx;
        writer_handle = task::spawn(db_writer(writer_conn, rx));

        // Back off before retrying: errors that reach this loop are typically
        // sustained conditions (e.g. quota exhaustion that outlasted the
        // per-call retries), so hammering the API again immediately would
        // just burn through the retry budget in seconds.
        let delay = Duration::from_secs(1 << retries);
        println!("waiting {:?} before retrying", delay);
        tokio::time::sleep(delay).await;
    }

    // Close the channel so the writer task drains its queue and exits, then
    // surface any error it hit.
    drop(write_tx);
    writer_handle.await??;

    Ok(())
}

/// Read a value from an environment variable, falling back to a default when
/// the variable is unset and failing loudly when it is set but unparseable.
fn env_or<T: FromStr>(name: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("invalid {} value {:?}", name, value)),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(anyhow::Error::from(e).context(format!("reading {}", name))),
    }
}

/// Create the tables if they don't exist yet and enforce uniqueness of
/// seen_mails.mail_id so that replaying a message (e.g. after a mid-run retry)
/// is idempotent instead of double-counting. Pre-existing duplicate rows from
/// earlier versions are collapsed before the unique index is created.
async fn init_schema(conn: &mut SqliteConnection) -> anyhow::Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS seen_mails (mail_id string)")
        .execute(&mut *conn)
        .await?;
    sqlx::query("CREATE TABLE IF NOT EXISTS senders (sender string, mails_sent int)")
        .execute(&mut *conn)
        .await?;
    sqlx::query(
        "DELETE FROM seen_mails WHERE rowid NOT IN \
         (SELECT MIN(rowid) FROM seen_mails GROUP BY mail_id)",
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query(
        "CREATE UNIQUE INDEX IF NOT EXISTS seen_mails_mail_id_unique ON seen_mails (mail_id)",
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The single DB writer: receives fetched results over the channel and commits
/// them one transaction at a time on its own dedicated connection.
///
/// The insert-and-count pair is idempotent per message id: the sender counter
/// is only incremented when the INSERT OR IGNORE actually inserts the row, so
/// a message that slips through the read-side seen check twice (its first
/// result still queued and uncommitted when it is re-listed) is still counted
/// exactly once.
async fn db_writer(
    mut conn: SqliteConnection,
    mut rx: mpsc::Receiver<SeenMail>,
) -> anyhow::Result<()> {
    while let Some(mail) = rx.recv().await {
        let mut tx = conn.begin().await?;
        let newly_seen = mark_seen(&mail.message_id, &mut *tx).await?;
        if newly_seen {
            increment_sender_mails(&mail.sender, &mut tx).await?;
        }
        tx.commit().await?;
    }
    Ok(())
}

async fn work(
    pool: &Pool<Sqlite>,
    hub: &GmailHub,
    write_tx: &mpsc::Sender<SeenMail>,
    rate_limiter: &Arc<Mutex<Interval>>,
    fetch_concurrency: usize,
) -> anyhow::Result<()> {
    // Fetch 500 messages at a time...
    let mut page_token: Option<String> = None;
    loop {
        let result = list_messages(hub, page_token.as_deref(), rate_limiter).await?;

        let next_page_token = result.next_page_token;
        parse_messages(
            pool,
            result.messages.unwrap_or_default(),
            hub,
            write_tx,
            rate_limiter,
            fetch_concurrency,
        )
        .await?;

        match next_page_token {
            Some(token) => page_token = Some(token),
            None => break,
        }
    }

    Ok(())
}

/// List one page of messages, waiting for a rate-limit tick before each
/// attempt and backing off exponentially on 429/403 rate-limit responses —
/// the same treatment fetch_message gives messages.get. Without this, a
/// rate-limited list call would propagate out of work() and restart the
/// entire mailbox scan from page one.
async fn list_messages(
    hub: &GmailHub,
    page_token: Option<&str>,
    rate_limiter: &Mutex<Interval>,
) -> anyhow::Result<google_gmail1::api::ListMessagesResponse> {
    let mut attempts = 0;
    let mut backoff = Duration::from_secs(1);
    loop {
        rate_limiter.lock().await.tick().await;

        let mut call = hub
            .users()
            .messages_list("me")
            .max_results(500)
            .include_spam_trash(false);
        if let Some(token) = page_token {
            call = call.page_token(token);
        }

        match call.doit().await {
            Ok((_, response)) => return Ok(response),
            Err(ref e) if attempts < MAX_FETCH_RETRIES && is_rate_limit_error(e) => {
                attempts += 1;
                println!(
                    "rate limited listing messages (attempt {}), backing off for {:?}",
                    attempts, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn parse_messages(
    pool: &Pool<Sqlite>,
    messages: Vec<Message>,
    hub: &GmailHub,
    write_tx: &mpsc::Sender<SeenMail>,
    rate_limiter: &Arc<Mutex<Interval>>,
    fetch_concurrency: usize,
) -> anyhow::Result<()> {
    // Fetch each individual message concurrently (bounded), then hand the
    // result to the writer task to increment the counter for the sender.
    stream::iter(messages.into_iter().map(Ok::<_, anyhow::Error>))
        .try_for_each_concurrent(fetch_concurrency, |message_meta| {
            let hub = hub.clone();
            let write_tx = write_tx.clone();
            let rate_limiter = rate_limiter.clone();
            async move {
                let message_id = message_meta.id.expect("message missing id");
                if seen_mail(&message_id, pool).await? {
                    return Ok(());
                }

                let message = fetch_message(&hub, &message_id, &rate_limiter).await?;
                let sender = cleanup_sender(get_sender(&message)?);
                println!("sender: {:?}", sender);

                write_tx
                    .send(SeenMail { message_id, sender })
                    .await
                    .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;

                Ok(())
            }
        })
        .await
}

/// Fetch a single message, waiting for a rate-limit tick before each attempt
/// and backing off exponentially on 429/403 rate-limit responses.
async fn fetch_message(
    hub: &GmailHub,
    message_id: &str,
    rate_limiter: &Mutex<Interval>,
) -> anyhow::Result<Message> {
    let mut attempts = 0;
    let mut backoff = Duration::from_secs(1);
    loop {
        rate_limiter.lock().await.tick().await;

        let res = hub
            .users()
            .messages_get("me", message_id)
            .add_scope(Scope::Readonly)
            .doit()
            .await;

        match res {
            Ok((_, message)) => return Ok(message),
            Err(ref e) if attempts < MAX_FETCH_RETRIES && is_rate_limit_error(e) => {
                attempts += 1;
                println!(
                    "rate limited fetching {} (attempt {}), backing off for {:?}",
                    message_id, attempts, backoff
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Whether an API error is a rate-limit response that warrants backoff and retry.
///
/// The client library returns `Error::BadRequest(json)` when a non-success
/// response body parses as JSON, and `Error::Failure(response)` only when it
/// does not. Gmail's 429 (`rateLimitExceeded`/`RESOURCE_EXHAUSTED`) and
/// rate-limit 403 (`userRateLimitExceeded`) responses carry Google's standard
/// JSON error envelope, so they arrive as `BadRequest` and must be recognized
/// by inspecting the embedded error code/status/reason.
fn is_rate_limit_error(err: &google_gmail1::Error) -> bool {
    match err {
        google_gmail1::Error::BadRequest(value) => {
            let error = &value["error"];
            if error["code"].as_u64() == Some(429)
                || error["status"].as_str() == Some("RESOURCE_EXHAUSTED")
            {
                return true;
            }
            // Rate-limit 403s are distinguished from permission 403s by their
            // reason field.
            error["errors"].as_array().into_iter().flatten().any(|e| {
                matches!(
                    e["reason"].as_str(),
                    Some("rateLimitExceeded") | Some("userRateLimitExceeded")
                )
            })
        }
        // Non-JSON error bodies: fall back to the HTTP status code.
        google_gmail1::Error::Failure(response) => {
            let status = response.status().as_u16();
            status == 429 || status == 403
        }
        _ => false,
    }
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

/// Record a message id as seen. Returns whether the id was newly inserted;
/// false means it was already present and must not be counted again.
async fn mark_seen(message_id: &str, executor: impl SqliteExecutor<'_>) -> anyhow::Result<bool> {
    let result = sqlx::query("INSERT OR IGNORE INTO seen_mails (mail_id) VALUES (?)")
        .bind(message_id)
        .execute(executor)
        .await?;
    Ok(result.rows_affected() > 0)
}

async fn increment_sender_mails(
    sender: &str,
    tx: &mut Transaction<'_, Sqlite>,
) -> anyhow::Result<()> {
    let row = sqlx::query("SELECT mails_sent FROM senders WHERE sender = ?")
        .bind(sender)
        .fetch_optional(&mut **tx)
        .await?;
    if row.is_none() {
        // no match
        sqlx::query("INSERT INTO senders (sender, mails_sent) VALUES (?, 1)")
            .bind(sender)
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
        .bind(sender)
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
