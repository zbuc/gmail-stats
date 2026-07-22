use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, TryStreamExt};
use google_gmail1::api::Message;
use google_gmail1::{api::Scope, hyper_rustls, hyper_util, yup_oauth2, Gmail};
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use sqlx::{Connection, Pool, Row, Sqlite, SqliteConnection, SqliteExecutor, Transaction};
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use tokio::time::{Interval, MissedTickBehavior};

type GmailHub =
    Gmail<hyper_rustls::HttpsConnector<hyper_util::client::legacy::connect::HttpConnector>>;

/// How many messages_get calls may be in flight at once.
const FETCH_CONCURRENCY: usize = 8;
/// Minimum spacing between Gmail API calls. Gmail's per-user quota is
/// ~250 quota units/sec and messages.get costs 5 units, so ~50 requests/sec
/// is the ceiling; 25ms spacing (~40 req/sec) stays comfortably under it.
const RATE_LIMIT_INTERVAL: Duration = Duration::from_millis(25);
/// How many times to retry a single message fetch on a rate-limit response.
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
    // TODO: add DB schema and migrations
    let options = SqliteConnectOptions::from_str("sqlite://./stats.db")?
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
        .max_connections(FETCH_CONCURRENCY as u32)
        .connect_with(options.clone())
        .await?;

    // The writer task owns the only connection that ever writes.
    let writer_conn = SqliteConnection::connect_with(&options).await?;
    let (write_tx, write_rx) = mpsc::channel::<SeenMail>(100);
    let writer_handle = task::spawn(db_writer(writer_conn, write_rx));

    // Simple client-side rate limiter: each API call waits for the next tick.
    let mut interval = tokio::time::interval(RATE_LIMIT_INTERVAL);
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

    // Some kind of exponential backpressure on a worker would be nicer
    let mut retries = 0;
    loop {
        // TODO: lol handle these better, i keep getting deadlocks but wanna just churn some emails
        // retries += 1;
        if retries > 3 {
            panic!("Too many retries");
        }

        let res = work(&pool, &hub, &write_tx, &rate_limiter).await;
        if res.is_ok() {
            break;
        }

        println!("Error encountered, retrying: {:?}", res);
    }

    // Close the channel so the writer task drains its queue and exits, then
    // surface any error it hit.
    drop(write_tx);
    writer_handle.await??;

    Ok(())
}

/// The single DB writer: receives fetched results over the channel and commits
/// them one transaction at a time on its own dedicated connection.
async fn db_writer(
    mut conn: SqliteConnection,
    mut rx: mpsc::Receiver<SeenMail>,
) -> anyhow::Result<()> {
    while let Some(mail) = rx.recv().await {
        let mut tx = conn.begin().await?;
        mark_seen(&mail.message_id, &mut *tx).await?;
        increment_sender_mails(&mail.sender, &mut tx).await?;
        tx.commit().await?;
    }
    Ok(())
}

async fn work(
    pool: &Pool<Sqlite>,
    hub: &GmailHub,
    write_tx: &mpsc::Sender<SeenMail>,
    rate_limiter: &Arc<Mutex<Interval>>,
) -> anyhow::Result<()> {
    // Fetch 500 messages at a time...
    let result = hub
        .users()
        .messages_list("me")
        .max_results(500)
        .include_spam_trash(false)
        .doit()
        .await?;

    let mut next_page_token = result.1.next_page_token;

    parse_messages(
        pool,
        result.1.messages.unwrap_or_default(),
        hub,
        write_tx,
        rate_limiter,
    )
    .await?;

    while let Some(token) = next_page_token {
        let result = hub
            .users()
            .messages_list("me")
            .max_results(500)
            .include_spam_trash(false)
            .page_token(&token)
            .doit()
            .await?;

        next_page_token = result.1.next_page_token;
        parse_messages(
            pool,
            result.1.messages.unwrap_or_default(),
            hub,
            write_tx,
            rate_limiter,
        )
        .await?;
    }

    Ok(())
}

async fn parse_messages(
    pool: &Pool<Sqlite>,
    messages: Vec<Message>,
    hub: &GmailHub,
    write_tx: &mpsc::Sender<SeenMail>,
    rate_limiter: &Arc<Mutex<Interval>>,
) -> anyhow::Result<()> {
    // Fetch each individual message concurrently (bounded), then hand the
    // result to the writer task to increment the counter for the sender.
    stream::iter(messages.into_iter().map(Ok::<_, anyhow::Error>))
        .try_for_each_concurrent(FETCH_CONCURRENCY, |message_meta| {
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
            Err(google_gmail1::Error::Failure(ref response))
                if attempts < MAX_FETCH_RETRIES
                    && (response.status().as_u16() == 429
                        || response.status().as_u16() == 403) =>
            {
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

async fn mark_seen(message_id: &str, executor: impl SqliteExecutor<'_>) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO seen_mails (mail_id) VALUES (?)")
        .bind(message_id)
        .execute(executor)
        .await?;
    Ok(())
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
