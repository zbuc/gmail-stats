//! Local web viewer for gmail-stats (Phase 1 of issue #11).
//!
//! Serves one embedded HTML page and one JSON endpoint over `./stats.db`,
//! strictly read-only, bound to 127.0.0.1 only.
//!
//! Usage: `cargo run --bin web [port]` (default port 7878), then open the
//! printed URL. Run from the repo root so `./stats.db` resolves.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;

// CSS/JS are separate same-origin assets, not inline blocks: under the
// `default-src 'self'` CSP below, `'self'` only allowlists same-origin URLs —
// inline <style>/<script> would be refused by the browser.
const INDEX_HTML: &str = include_str!("../../web/index.html");
const APP_CSS: &str = include_str!("../../web/app.css");
const APP_JS: &str = include_str!("../../web/app.js");
const DEFAULT_PORT: u16 = 7878;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port: u16 = match std::env::args().nth(1) {
        Some(arg) => arg
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid port: {arg}"))?,
        None => DEFAULT_PORT,
    };

    // Read-only at the connection level, not by convention: the viewer must
    // not be able to touch a mid-scan DB even via a bug.
    let options = SqliteConnectOptions::new()
        .filename("./stats.db")
        .read_only(true)
        .pragma("query_only", "ON")
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(5));

    // Lazy connect so the server still starts (and can serve the friendly
    // setup page) when stats.db doesn't exist yet.
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .connect_lazy_with(options);

    let app = Router::new()
        .route("/", get(index))
        .route("/app.css", get(app_css))
        .route("/app.js", get(app_js))
        .route("/api/summary", get(summary))
        .fallback(not_found)
        .layer(middleware::from_fn(host_guard_and_security_headers))
        .with_state(pool);

    // Loopback only, structurally. The port is the only knob.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    println!("gmail-stats web viewer listening on http://127.0.0.1:{port}/");
    axum::serve(listener, app).await?;
    Ok(())
}

/// DNS-rebinding guard plus defense-in-depth headers on every response.
async fn host_guard_and_security_headers(req: Request, next: Next) -> Response {
    let host_allowed = req
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(|host| {
            let name = host.rsplit_once(':').map_or(host, |(name, _port)| name);
            name == "127.0.0.1" || name == "localhost"
        })
        .unwrap_or(false);
    if !host_allowed {
        return (StatusCode::FORBIDDEN, "forbidden").into_response();
    }

    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'self'"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn app_css() -> ([(header::HeaderName, &'static str); 1], &'static str) {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

async fn app_js() -> ([(header::HeaderName, &'static str); 1], &'static str) {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        APP_JS,
    )
}

async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

async fn summary(State(pool): State<SqlitePool>) -> Response {
    match build_summary(&pool).await {
        Ok(body) => Json(body).into_response(),
        Err(err) => error_response(&err),
    }
}

async fn build_summary(pool: &SqlitePool) -> Result<serde_json::Value, sqlx::Error> {
    // seen_mails has no PK/unique constraint, so count distinct (matches the
    // README's own query).
    let total_messages: i64 = sqlx::query_scalar("SELECT COUNT(DISTINCT mail_id) FROM seen_mails")
        .fetch_one(pool)
        .await?;

    // The app assumes one row per sender but nothing enforces it, so merge
    // here. CAST guards the affinity-less mails_sent column against stray
    // TEXT values.
    let rows: Vec<(Option<String>, i64)> = sqlx::query_as(
        "SELECT sender, COALESCE(SUM(CAST(mails_sent AS INTEGER)), 0) AS mails_sent \
         FROM senders GROUP BY sender ORDER BY mails_sent DESC",
    )
    .fetch_all(pool)
    .await?;

    let senders: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(sender, mails_sent)| {
            json!({ "sender": sender.unwrap_or_default(), "mails_sent": mails_sent })
        })
        .collect();

    let generated_at_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    Ok(json!({
        "total_messages": total_messages,
        "senders": senders,
        "generated_at_unix": generated_at_unix,
    }))
}

fn error_response(err: &sqlx::Error) -> Response {
    let detail = err.to_string();
    let lower = detail.to_lowercase();
    let (status, kind, message) = if lower.contains("unable to open database file")
        || lower.contains("no such table")
    {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "missing_db",
            "stats.db (or its tables) not found — set up the database and run the scanner first",
        )
    } else if lower.contains("database is locked") || lower.contains("database is busy") {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "database busy — scanner is writing, retry shortly",
        )
    } else {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal error",
        )
    };
    (
        status,
        Json(json!({ "error": kind, "message": message, "detail": detail })),
    )
        .into_response()
}
