use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
/// on a transient error (rate limit, 5xx, or network failure).
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
    // attempts (see the sleep at the bottom of the loop body). Only transient
    // failures (rate limits and server errors that outlasted the per-call
    // retries, SQLite busy/locked) are retried; everything else fails fast.
    // The retry budget is per plateau, not per process: whenever an attempt
    // makes forward progress (completes at least one page) the counter
    // resets, so sporadic transient errors spread across a long scan can
    // never accumulate into an abort — only repeated failures with no
    // progress in between exhaust the budget.
    let mut retries = 0;
    let mut pages_done: u64 = 0;
    // The page token to resume from, advanced only once a page has been
    // fully processed: a retry re-lists the page that failed instead of
    // restarting the whole mailbox scan from page one.
    let mut resume_token: Option<String> = None;
    loop {
        let pages_before = pages_done;
        match work(
            &pool,
            &hub,
            &write_tx,
            &rate_limiter,
            fetch_concurrency,
            &mut resume_token,
            &mut pages_done,
        )
        .await
        {
            Ok(()) => break,
            Err(e) => {
                // Quiesce the writer before deciding what to do next: close
                // the channel and wait for the writer to drain and commit
                // every queued result. Without this barrier a retry's
                // seen-mail checks race the writer's backlog — a message that
                // is queued but not yet committed reads as unseen and gets
                // fetched, sent, and counted a second time — and on the
                // give-up path already-fetched messages would be discarded by
                // runtime shutdown and re-fetched (paid quota) on the next
                // run. This also recovers the writer's own error: when the
                // writer died, work() only saw an opaque "channel closed"
                // send error, and the root-cause DB error lives in the join
                // handle.
                drop(write_tx);
                let writer_err = match writer_handle.await {
                    Ok(Ok(())) => None,
                    Ok(Err(writer_err)) => Some(writer_err),
                    Err(join_err) => Some(anyhow::anyhow!("DB writer task panicked: {join_err:?}")),
                };

                let transient = is_transient(&e) || writer_err.as_ref().is_some_and(is_transient);
                // If this attempt advanced the resume point (i.e. fully
                // processed at least one page) before failing, the previous
                // errors were intermittent, not persistent — start the budget
                // over so MAX_RETRIES bounds *consecutive* failures.
                if pages_done > pages_before {
                    retries = 0;
                }
                retries += 1;

                if !transient || retries > MAX_RETRIES {
                    let final_err = if transient {
                        e.context(format!("giving up after {MAX_RETRIES} retries"))
                    } else {
                        e.context("giving up: error is not retryable")
                    };
                    return match writer_err {
                        None => Err(final_err),
                        Some(writer_err) => {
                            Err(final_err.context(format!("DB writer task failed: {writer_err:?}")))
                        }
                    };
                }

                if let Some(writer_err) = &writer_err {
                    println!("DB writer task failed, restarting it: {writer_err:?}");
                }
                // Spawn a fresh writer so the retry has a chance of working.
                let writer_conn = SqliteConnection::connect_with(&options).await?;
                let (tx, rx) = mpsc::channel::<SeenMail>(100);
                write_tx = tx;
                writer_handle = task::spawn(db_writer(writer_conn, rx));

                // Back off before retrying: errors that reach this loop are
                // typically sustained conditions (e.g. quota exhaustion that
                // outlasted the per-call retries), so hammering the API again
                // immediately would just burn through the retry budget in
                // seconds.
                let delay = backoff_delay(retries);
                println!(
                    "Transient error (attempt {retries}/{MAX_RETRIES}), retrying in {delay:?}: {e:?}"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }

    // Close the channel so the writer task drains its queue and exits, then
    // surface any error it hit.
    drop(write_tx);
    writer_handle.await??;

    Ok(())
}

/// How many consecutive no-progress failures of a whole attempt to tolerate
/// before giving up.
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
    resume_token: &mut Option<String>,
    pages_done: &mut u64,
) -> anyhow::Result<()> {
    // Fetch 500 messages at a time, starting from the last page we fully
    // processed (so a retry doesn't re-list the whole mailbox from page one).
    loop {
        let result = list_messages(hub, resume_token.as_deref(), rate_limiter).await?;

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

        // Completing a page is forward progress: only now advance the resume
        // point and the page counter the caller uses to reset its retry
        // budget, so a long scan isn't aborted by transient errors
        // accumulated across otherwise-successful attempts.
        *pages_done += 1;
        *resume_token = next_page_token;
        if resume_token.is_none() {
            return Ok(());
        }
    }
}

/// List one page of messages, waiting for a rate-limit tick before each
/// attempt and backing off exponentially on transient errors (rate limits,
/// 5xx responses, network failures) — the same treatment fetch_message gives
/// messages.get. Without this, a transient list failure would propagate out
/// of work() and restart the entire mailbox scan from page one.
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
            Err(ref e) if attempts < MAX_FETCH_RETRIES && is_transient_gmail(e) => {
                attempts += 1;
                println!(
                    "transient error listing messages (attempt {}), backing off for {:?}: {}",
                    attempts, backoff, e
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
/// and backing off exponentially on transient errors (rate limits, 5xx
/// responses, network failures).
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
            Err(ref e) if attempts < MAX_FETCH_RETRIES && is_transient_gmail(e) => {
                attempts += 1;
                println!(
                    "transient error fetching {} (attempt {}), backing off for {:?}: {}",
                    message_id, attempts, backoff, e
                );
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

/// Only transient failures are worth retrying: Gmail API rate limits, server
/// errors and network hiccups, plus SQLite busy/locked (the deadlocks that
/// motivated the outer retry loop). Auth failures, other 4xx responses, etc.
/// fail fast. Walks the whole anyhow context chain so a wrapped root cause is
/// still recognized.
fn is_transient(err: &anyhow::Error) -> bool {
    for cause in err.chain() {
        if let Some(sqlx_err) = cause.downcast_ref::<sqlx::Error>() {
            return is_transient_sqlx(sqlx_err);
        }
        if let Some(gmail_err) = cause.downcast_ref::<google_gmail1::Error>() {
            return is_transient_gmail(gmail_err);
        }
    }
    false
}

fn is_transient_sqlx(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            // SQLITE_BUSY (5) and SQLITE_LOCKED (6); extended result codes
            // (e.g. SQLITE_BUSY_SNAPSHOT = 517) keep the primary code in the
            // low byte.
            db_err
                .code()
                .and_then(|code| code.parse::<i64>().ok())
                .is_some_and(|code| matches!(code & 0xff, 5 | 6))
        }
        // Timed out waiting for a pool connection while the DB is busy.
        sqlx::Error::PoolTimedOut => true,
        _ => false,
    }
}

/// Whether a Gmail API error is transient and warrants backoff and retry: a
/// rate-limit response, a retryable 5xx server error, or a network-level
/// failure (connection reset, TLS hiccup, truncated body). A long scan makes
/// hundreds of thousands of requests, so sporadic errors from all three
/// classes are expected and must be retried in place — propagating them
/// aborts the page and restarts the whole mailbox scan from page one.
///
/// The client library returns `Error::BadRequest(json)` when a non-success
/// response body parses as JSON, and `Error::Failure(response)` only when it
/// does not. Gmail's 429 (`rateLimitExceeded`/`RESOURCE_EXHAUSTED`),
/// rate-limit 403 (`userRateLimitExceeded`), and sporadic 5xx
/// (`UNAVAILABLE`/`backendError`) responses all carry Google's standard JSON
/// error envelope, so they arrive as `BadRequest` and must be recognized by
/// inspecting the embedded error code/status/reason. Other 403 reasons (e.g.
/// `insufficientPermissions`, `dailyLimitExceeded`) are genuinely fatal and
/// fail fast.
fn is_transient_gmail(err: &google_gmail1::Error) -> bool {
    match err {
        google_gmail1::Error::BadRequest(value) => {
            let error = &value["error"];
            // 429s and 5xx server errors are standard retry-with-backoff
            // material per Google's API guidance.
            if matches!(error["code"].as_u64(), Some(429) | Some(500..=599)) {
                return true;
            }
            if matches!(
                error["status"].as_str(),
                Some("RESOURCE_EXHAUSTED") | Some("UNAVAILABLE") | Some("INTERNAL")
            ) {
                return true;
            }
            // Rate-limit 403s are distinguished from permission 403s by their
            // reason field.
            error["errors"].as_array().into_iter().flatten().any(|e| {
                matches!(
                    e["reason"].as_str(),
                    Some("rateLimitExceeded")
                        | Some("userRateLimitExceeded")
                        | Some("backendError")
                )
            })
        }
        // Non-JSON error bodies: fall back to the HTTP status code.
        google_gmail1::Error::Failure(response) => {
            let status = response.status();
            status.as_u16() == 429 || status.as_u16() == 403 || status.is_server_error()
        }
        // Network-level transients: connection resets, TLS handshake
        // failures, and I/O errors reading the response body.
        google_gmail1::Error::HttpError(_) | google_gmail1::Error::Io(_) => true,
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
    // Header names are case-insensitive (RFC 5322); check candidates in priority order.
    const CANDIDATES: [&str; 2] = ["from", "return-path"];
    let headers = message
        .payload
        .as_ref()
        .and_then(|p| p.headers.as_deref())
        .unwrap_or(&[]);

    for candidate in CANDIDATES {
        if let Some(value) = headers.iter().find_map(|header| {
            header
                .name
                .as_deref()
                .filter(|name| name.eq_ignore_ascii_case(candidate))
                .and(header.value.as_deref())
        }) {
            return Ok(value.to_string());
        }
    }

    println!("weird email without from header: {:?}", message.id);
    Ok(String::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use google_gmail1::api::{MessagePart, MessagePartHeader};
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

    fn envelope_403(reason: &str) -> anyhow::Error {
        anyhow::Error::new(google_gmail1::Error::BadRequest(json!({
            "error": {
                "code": 403,
                "message": "boom",
                "errors": [
                    { "domain": "usageLimits", "reason": reason, "message": "boom" }
                ]
            }
        })))
    }

    #[test]
    fn json_403_rate_limit_reasons_are_transient() {
        assert!(is_transient(&envelope_403("rateLimitExceeded")));
        assert!(is_transient(&envelope_403("userRateLimitExceeded")));
    }

    #[test]
    fn json_403_resource_exhausted_status_is_transient() {
        let err = anyhow::Error::new(google_gmail1::Error::BadRequest(json!({
            "error": { "code": 403, "message": "boom", "status": "RESOURCE_EXHAUSTED" }
        })));
        assert!(is_transient(&err));
    }

    #[test]
    fn json_403_non_rate_limit_reasons_are_not_transient() {
        assert!(!is_transient(&envelope_403("insufficientPermissions")));
        assert!(!is_transient(&envelope_403("dailyLimitExceeded")));
        assert!(!is_transient(&envelope_403("domainPolicy")));
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

    fn header(name: &str, value: Option<&str>) -> MessagePartHeader {
        MessagePartHeader {
            name: Some(name.to_string()),
            value: value.map(|v| v.to_string()),
        }
    }

    fn message_with_headers(headers: Vec<MessagePartHeader>) -> Message {
        Message {
            id: Some("msg-1".to_string()),
            payload: Some(MessagePart {
                headers: Some(headers),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    // --- cleanup_sender ---

    #[test]
    fn cleanup_sender_display_name_angle_brackets() {
        assert_eq!(
            cleanup_sender("Jane Doe <jane@example.com>".to_string()),
            "jane@example.com"
        );
    }

    #[test]
    fn cleanup_sender_angle_brackets_only() {
        assert_eq!(
            cleanup_sender("<jane@example.com>".to_string()),
            "jane@example.com"
        );
    }

    #[test]
    fn cleanup_sender_bare_address() {
        assert_eq!(
            cleanup_sender("jane@example.com".to_string()),
            "jane@example.com"
        );
    }

    #[test]
    fn cleanup_sender_address_with_dots_and_dashes() {
        assert_eq!(
            cleanup_sender("first.last-name@mail-server.example.org".to_string()),
            "first.last-name@mail-server.example.org"
        );
        assert_eq!(
            cleanup_sender("Team <first.last-name@mail-server.example.org>".to_string()),
            "first.last-name@mail-server.example.org"
        );
    }

    #[test]
    fn cleanup_sender_unparseable_passes_through() {
        assert_eq!(cleanup_sender("not an email".to_string()), "not an email");
        assert_eq!(
            cleanup_sender("Weird Sender <no-at-sign>".to_string()),
            "Weird Sender <no-at-sign>"
        );
        assert_eq!(cleanup_sender(String::new()), "");
    }

    // --- get_sender ---

    #[test]
    fn get_sender_prefers_from_over_return_path() {
        let message = message_with_headers(vec![
            header("Return-Path", Some("bounce@example.com")),
            header("From", Some("sender@example.com")),
        ]);
        assert_eq!(get_sender(&message).unwrap(), "sender@example.com");
    }

    #[test]
    fn get_sender_is_case_insensitive() {
        for name in ["From", "FROM", "from", "fRoM"] {
            let message = message_with_headers(vec![header(name, Some("sender@example.com"))]);
            assert_eq!(
                get_sender(&message).unwrap(),
                "sender@example.com",
                "failed for header name {name:?}"
            );
        }

        let message = message_with_headers(vec![header("RETURN-PATH", Some("bounce@example.com"))]);
        assert_eq!(get_sender(&message).unwrap(), "bounce@example.com");
    }

    #[test]
    fn get_sender_skips_valueless_headers() {
        let message = message_with_headers(vec![
            header("From", None),
            header("Return-Path", Some("bounce@example.com")),
        ]);
        assert_eq!(get_sender(&message).unwrap(), "bounce@example.com");
    }

    #[test]
    fn get_sender_falls_back_to_empty_string() {
        // Headers present but none relevant.
        let message = message_with_headers(vec![header("Subject", Some("hi"))]);
        assert_eq!(get_sender(&message).unwrap(), "");

        // No payload at all.
        let message = Message::default();
        assert_eq!(get_sender(&message).unwrap(), "");
    }

    // --- SQLite counting logic ---

    async fn test_pool() -> Pool<Sqlite> {
        let options = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        // A single connection so every query sees the same in-memory database.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();

        // Run the production DDL so the tests exercise the same schema the
        // real database has, including the `string` type name (SQLite gives
        // such columns NUMERIC affinity rather than TEXT) and the unique
        // index on seen_mails.mail_id that makes mark_seen idempotent.
        let mut conn = pool.acquire().await.unwrap();
        init_schema(&mut conn).await.unwrap();
        drop(conn);

        pool
    }

    #[tokio::test]
    async fn seen_mail_dedup() {
        let pool = test_pool().await;

        assert!(!seen_mail("mail-1", &pool).await.unwrap());

        // First sighting inserts the row and reports it as newly seen.
        assert!(mark_seen("mail-1", &pool).await.unwrap());

        assert!(seen_mail("mail-1", &pool).await.unwrap());
        // Other ids remain unseen.
        assert!(!seen_mail("mail-2", &pool).await.unwrap());

        // Replaying the same id is ignored and reports it as already seen.
        assert!(!mark_seen("mail-1", &pool).await.unwrap());
        let rows: i64 = sqlx::query("SELECT count(1) AS ct FROM seen_mails")
            .fetch_one(&pool)
            .await
            .unwrap()
            .try_get("ct")
            .unwrap();
        assert_eq!(rows, 1);
    }

    #[tokio::test]
    async fn increment_sender_mails_inserts_then_increments() {
        let pool = test_pool().await;

        // First mail from a sender inserts a row with mails_sent = 1.
        let mut tx = pool.begin().await.unwrap();
        increment_sender_mails(
            &cleanup_sender("Jane <jane@example.com>".to_string()),
            &mut tx,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        let count: i64 =
            sqlx::query("SELECT mails_sent FROM senders WHERE sender = 'jane@example.com'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .try_get("mails_sent")
                .unwrap();
        assert_eq!(count, 1);

        // A second mail from the same (cleaned-up) sender increments it.
        let mut tx = pool.begin().await.unwrap();
        increment_sender_mails(&cleanup_sender("jane@example.com".to_string()), &mut tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let count: i64 =
            sqlx::query("SELECT mails_sent FROM senders WHERE sender = 'jane@example.com'")
                .fetch_one(&pool)
                .await
                .unwrap()
                .try_get("mails_sent")
                .unwrap();
        assert_eq!(count, 2);

        // Only one row exists for the sender.
        let rows: i64 = sqlx::query("SELECT count(1) AS ct FROM senders")
            .fetch_one(&pool)
            .await
            .unwrap()
            .try_get("ct")
            .unwrap();
        assert_eq!(rows, 1);
    }
}
