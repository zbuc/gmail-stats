use std::str::FromStr;

use futures::TryStreamExt;
use google_gmail1::api::Message;
use google_gmail1::{api::Scope, hyper, hyper_rustls, oauth2, Gmail};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Pool, Row, Sqlite};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // TODO: use tokio::spawn and sqlite transactions to make fetching concurrent
    // Also add DB schema and migrations
    let options = SqliteConnectOptions::from_str("sqlite://./stats.db")?
        // WAL mode should be much faster for concurrent reads and writes
        .journal_mode(SqliteJournalMode::Wal)
        // Synchronous mode is OK because a transaction may roll back during a crash, however
        // all mail listings are re-fetched during each run.
        .synchronous(SqliteSynchronous::Normal)
        .shared_cache(true);

    let pool = Pool::<Sqlite>::connect_with(options).await?;

    // Read application OAuth secret from a file.
    let secret = oauth2::read_application_secret("credentials.json")
        .await
        .expect("credentials.json");

    // Create an authenticator that uses an InstalledFlow to authenticate. The
    // authentication tokens are persisted to a file named tokencache.json. The
    // authenticator takes care of caching tokens to disk and refreshing tokens once
    // they've expired.
    let auth = oauth2::InstalledFlowAuthenticator::builder(
        secret,
        oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk("tokencache.json")
    .build()
    .await
    .unwrap();

    let mut hub = Gmail::new(
        hyper::Client::builder().build(
            // hyper_rustls::HttpsConnector::with_native_roots()
            hyper_rustls::HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1()
                .enable_http2()
                .build(),
        ),
        auth,
    );

    let mut retries = 0;
    loop {
        retries += 1;
        if retries > 3 {
            panic!("Too many retries");
        }

        let res = work(&pool, &mut hub).await;
        if res.is_ok() {
            break;
        }

        println!("Error encountered, retrying: {:?}", res);
    }

    Ok(())
}

async fn work(pool: &Pool<Sqlite>, hub: &mut Gmail) -> anyhow::Result<()> {
    // Fetch 500 messages at a time...
    let result = hub
        .users()
        .messages_list("me")
        .max_results(500)
        .include_spam_trash(false)
        .doit()
        .await?;

    let mut next_page_token = result.1.next_page_token;

    parse_messages(pool, result.1.messages.unwrap_or_default(), hub).await?;

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
        parse_messages(pool, result.1.messages.unwrap_or_default(), hub).await?;
    }

    Ok(())
}

async fn parse_messages(
    pool: &Pool<Sqlite>,
    messages: Vec<Message>,
    hub: &mut Gmail,
) -> anyhow::Result<()> {
    // Then fetch each individual message and increment the counter for the sender.
    for message_meta in messages {
        if !seen_mail(message_meta.id.as_ref().expect("message missing id"), pool).await? {
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

            mark_seen(&message, pool).await?;
            increment_sender_mails(&message, pool).await?;
        }
    }

    Ok(())
}

async fn seen_mail(message_id: &str, pool: &Pool<Sqlite>) -> anyhow::Result<bool> {
    let mut res = sqlx::query("SELECT count(1) AS ct FROM seen_mails WHERE mail_id = ?")
        .bind(message_id)
        .fetch(pool);
    while let Some(row) = res.try_next().await? {
        let count: u32 = row.try_get("ct")?;
        if count > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn mark_seen(message: &Message, pool: &Pool<Sqlite>) -> anyhow::Result<()> {
    sqlx::query("INSERT INTO seen_mails (mail_id) VALUES (?)")
        .bind(message.id.as_ref().expect("message missing id"))
        .execute(pool)
        .await?;
    Ok(())
}

async fn increment_sender_mails(message: &Message, pool: &Pool<Sqlite>) -> anyhow::Result<()> {
    let sender = get_sender(message)?;
    let mut res = sqlx::query("SELECT mails_sent FROM senders WHERE sender = ?")
        .bind(&sender)
        .fetch(pool);
    if let Some(row) = res.try_next().await? {
        let mut mails_sent = 0;
        let count: u32 = row.try_get("mails_sent")?;
        if count > 0 {
            mails_sent = count;
        }

        mails_sent += 1;
        sqlx::query("UPDATE senders SET mails_sent = ? WHERE sender = ?")
            .bind(mails_sent)
            .bind(&sender)
            .execute(pool)
            .await?;

        return Ok(());
    }

    sqlx::query("INSERT INTO senders (sender, mails_sent) VALUES (?, 1)")
        .bind(&sender)
        .execute(pool)
        .await?;

    Ok(())
}

fn get_sender(message: &Message) -> anyhow::Result<String> {
    let from_headers = message
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
        let from_headers = message
            .clone()
            .payload
            .unwrap_or_default()
            .headers
            .unwrap_or_default()
            .iter()
            .filter(|header| header.name == Some("FROM".to_string()))
            .cloned()
            .collect::<Vec<_>>();

        if from_headers.is_empty() {
            println!("weird email without from header: {:?}", message);
            return Ok("".to_string());
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
