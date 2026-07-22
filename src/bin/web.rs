//! Local web viewer for gmail-stats (issue #11 Phase 1, issue #28 Phase B).
//!
//! Serves the embedded UI plus read-only JSON endpoints over `./stats.db`,
//! bound to 127.0.0.1 only. Phase B adds observe-only ingestion state:
//! `GET /api/status` (db readiness, active/last run, flock probe, viewer-side
//! rate/ETA, CSRF token slot) and `GET /api/runs` (run history). The viewer
//! performs no database writes of any kind; the only file it touches is the
//! ingest lockfile, probed without truncation via open + try-lock + release.
//!
//! Usage: `cargo run --bin web [port]` (default port 7878), then open the
//! printed URL. Run from the repo root so `./stats.db` resolves, or point
//! GMAIL_STATS_DB at the database.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};

use gmail_stats::ingest;

// CSS/JS are separate same-origin assets, not inline blocks: under the
// `default-src 'self'` CSP below, `'self'` only allowlists same-origin URLs —
// inline <style>/<script> would be refused by the browser.
const INDEX_HTML: &str = include_str!("../../web/index.html");
const APP_CSS: &str = include_str!("../../web/app.css");
const APP_JS: &str = include_str!("../../web/app.js");
const DEFAULT_PORT: u16 = 7878;

/// How far back the in-memory rate window looks. Old observations age out so
/// the displayed rate tracks the recent pace, not the whole run's average.
const RATE_WINDOW: Duration = Duration::from_secs(30);
/// Minimum span between oldest and newest observation before a rate is
/// reported at all; two samples a few ms apart would just be noise.
const RATE_MIN_SPAN: Duration = Duration::from_millis(500);

/// Shared state for the router. Everything is read-only against the database;
/// the rate window lives purely in viewer memory and is never persisted.
struct AppState {
    pool: SqlitePool,
    db_path: PathBuf,
    /// Per-process CSRF token (32 bytes of CSPRNG output, hex-encoded),
    /// minted at startup and served via /api/status. No endpoint consumes it
    /// yet — the first state-changing routes (Phase C) will require it in a
    /// custom header, and shipping the slot now means clients can already
    /// learn it the intended way (same-origin GET only; a cross-origin page
    /// cannot read this response).
    csrf_token: String,
    rate: Mutex<RateWindow>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port: u16 = match std::env::args().nth(1) {
        Some(arg) => arg
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid port: {arg}"))?,
        None => DEFAULT_PORT,
    };
    let db_path =
        PathBuf::from(std::env::var("GMAIL_STATS_DB").unwrap_or_else(|_| "./stats.db".to_string()));

    let app = build_router(Arc::new(AppState::new(&db_path)));

    // Loopback only, structurally. The port is the only knob.
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
    // Report the bound address, not the requested port: with port 0 the OS
    // picks an ephemeral port, and the printed URL must be the real one.
    let local_addr = listener.local_addr()?;
    println!("gmail-stats web viewer listening on http://{local_addr}/");
    axum::serve(listener, app).await?;
    Ok(())
}

impl AppState {
    fn new(db_path: &Path) -> Self {
        // Read-only at the connection level, not by convention: the viewer
        // must not be able to touch a mid-scan DB even via a bug.
        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .read_only(true)
            .pragma("query_only", "ON")
            .create_if_missing(false)
            .busy_timeout(Duration::from_secs(5));

        // Lazy connect so the server still starts (and can serve the friendly
        // onboarding page) when stats.db doesn't exist yet.
        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_lazy_with(options);

        let mut token_bytes = [0u8; 32];
        getrandom::fill(&mut token_bytes).expect("operating system CSPRNG unavailable");
        let csrf_token: String = token_bytes.iter().map(|b| format!("{b:02x}")).collect();

        AppState {
            pool,
            db_path: db_path.to_path_buf(),
            csrf_token,
            rate: Mutex::new(RateWindow::default()),
        }
    }
}

fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.css", get(app_css))
        .route("/app.js", get(app_js))
        .route("/api/summary", get(summary))
        .route("/api/status", get(status))
        .route("/api/runs", get(runs))
        .fallback(not_found)
        .layer(middleware::from_fn(host_guard_and_security_headers))
        .with_state(state)
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

async fn summary(State(state): State<Arc<AppState>>) -> Response {
    match build_summary(&state.pool).await {
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
    // here. COALESCE folds NULL senders into '' *before* grouping so they
    // merge with literal-'' rows instead of forming a separate group. Both
    // columns need CAST against the schema's loose types: `sender string`
    // matches none of SQLite's affinity keywords, so the column has NUMERIC
    // affinity and a numeric-looking From header (e.g. an SMS shortcode like
    // `40404`) is stored as INTEGER/REAL, which sqlx's String decoder
    // rejects. CAST ... AS TEXT normalizes the storage class (and merges an
    // INTEGER 12345 row with a TEXT '12345' row) so decoding — and thus the
    // whole viewer — survives such rows; the mails_sent CAST likewise guards
    // against stray TEXT values.
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT CAST(COALESCE(sender, '') AS TEXT) AS sender, \
                COALESCE(SUM(CAST(mails_sent AS INTEGER)), 0) AS mails_sent \
         FROM senders GROUP BY CAST(COALESCE(sender, '') AS TEXT) \
         ORDER BY mails_sent DESC",
    )
    .fetch_all(pool)
    .await?;

    let senders: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(sender, mails_sent)| json!({ "sender": sender, "mails_sent": mails_sent }))
        .collect();

    Ok(json!({
        "total_messages": total_messages,
        "senders": senders,
        "generated_at_unix": now_unix(),
    }))
}

// ---------------------------------------------------------------------------
// GET /api/status — the single cheap poll target for "watch from the browser"
// ---------------------------------------------------------------------------

/// How a database-level error should be interpreted.
enum DbErrorKind {
    /// The file (or a table the query needs) doesn't exist.
    MissingFile,
    MissingTable,
    Busy,
    Other,
}

fn classify_db_error(err: &sqlx::Error) -> DbErrorKind {
    let lower = err.to_string().to_lowercase();
    if lower.contains("unable to open database file") {
        DbErrorKind::MissingFile
    } else if lower.contains("no such table") {
        DbErrorKind::MissingTable
    } else if lower.contains("database is locked") || lower.contains("database is busy") {
        DbErrorKind::Busy
    } else {
        DbErrorKind::Other
    }
}

async fn status(State(state): State<Arc<AppState>>) -> Response {
    match build_status(&state).await {
        Ok(body) => Json(body).into_response(),
        Err(err) => error_response(&err),
    }
}

/// Assemble the status document. Missing database, missing tables, and empty
/// tables are all *data* here (they drive onboarding), never errors; only
/// busy/unexpected failures propagate to the caller as HTTP errors.
async fn build_status(state: &AppState) -> Result<serde_json::Value, sqlx::Error> {
    let lock_held = probe_ingest_lock(&state.db_path);

    // Database readiness. `unable to open` ⇒ the file is missing (the pool is
    // read-only and never creates it); `no such table` ⇒ the file exists but
    // was never initialized — both are onboarding states, not errors.
    let db: &str =
        match sqlx::query_scalar::<_, i64>("SELECT COUNT(DISTINCT mail_id) FROM seen_mails")
            .fetch_one(&state.pool)
            .await
        {
            Ok(0) => "empty",
            Ok(_) => "ready",
            Err(err) => match classify_db_error(&err) {
                DbErrorKind::MissingFile => {
                    return Ok(status_document(
                        state, "missing", None, None, false, lock_held,
                    ));
                }
                DbErrorKind::MissingTable => "empty",
                DbErrorKind::Busy | DbErrorKind::Other => return Err(err),
            },
        };

    // Run rows. An old database without ingest_runs simply has no runs; the
    // ingester creates the table on its next start.
    let (active_run, last_run, mixed_sources) = match fetch_runs_view(&state.pool).await {
        Ok(view) => view,
        Err(err) => match classify_db_error(&err) {
            DbErrorKind::MissingFile | DbErrorKind::MissingTable => (None, None, false),
            DbErrorKind::Busy | DbErrorKind::Other => return Err(err),
        },
    };

    Ok(status_document(
        state,
        db,
        active_run,
        last_run,
        mixed_sources,
        lock_held,
    ))
}

async fn fetch_runs_view(
    pool: &SqlitePool,
) -> Result<(Option<SqliteRow>, Option<SqliteRow>, bool), sqlx::Error> {
    let active = sqlx::query(
        "SELECT * FROM ingest_runs \
         WHERE state NOT IN ('done', 'failed', 'cancelled', 'abandoned') \
         ORDER BY run_id DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    let last = sqlx::query(
        "SELECT * FROM ingest_runs \
         WHERE state IN ('done', 'failed', 'cancelled', 'abandoned') \
         ORDER BY run_id DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    // Interim mixed-sources caution (removed by Phase D's exact dedupe): true
    // once more than one source has actually contributed rows — completed, or
    // got far enough to add new messages before stopping.
    let mixed: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT source) FROM ingest_runs \
         WHERE state = 'done' OR messages_new > 0",
    )
    .fetch_one(pool)
    .await?;
    Ok((active, last, mixed >= 2))
}

fn status_document(
    state: &AppState,
    db: &str,
    active_run: Option<SqliteRow>,
    last_run: Option<SqliteRow>,
    mixed_sources: bool,
    lock_held: Option<bool>,
) -> serde_json::Value {
    // Viewer-derived rate/ETA from successive observations of the active
    // run's counters. Pure viewer memory: nothing here is written anywhere.
    let mut rate_per_sec = None;
    let mut eta_seconds = None;
    {
        let mut window = state.rate.lock().unwrap_or_else(|e| e.into_inner());
        match &active_run {
            Some(row) => {
                let run_id: i64 = row.try_get("run_id").unwrap_or(0);
                let messages_seen: i64 = row.try_get("messages_seen").unwrap_or(0);
                let bytes_done: Option<i64> = row.try_get("bytes_done").unwrap_or(None);
                let (msg_rate, byte_rate) =
                    window.observe(Instant::now(), run_id, messages_seen, bytes_done);
                rate_per_sec = msg_rate.map(|r| (r * 10.0).round() / 10.0);
                let bytes_total: Option<i64> = row.try_get("bytes_total").unwrap_or(None);
                let total_estimate: Option<i64> = row.try_get("total_estimate").unwrap_or(None);
                eta_seconds = compute_eta(
                    messages_seen,
                    bytes_done,
                    bytes_total,
                    total_estimate,
                    msg_rate,
                    byte_rate,
                );
            }
            None => window.reset(),
        }
    }

    json!({
        "db": db,
        "now_unix": now_unix(),
        "ingest_lock_held": lock_held,
        "active_run": active_run.as_ref().map(run_to_json),
        "last_run": last_run.as_ref().map(run_to_json),
        // Phase B never spawns anything, so no run is ever owned by the
        // viewer; the field exists so clients already branch on it.
        "owns_active_run": false,
        "mixed_sources": mixed_sources,
        "rate_per_sec": rate_per_sec,
        "eta_seconds": eta_seconds,
        "csrf_token": state.csrf_token,
    })
}

/// Serialize one ingest_runs row for the API. resume_token and
/// mbox_fingerprint are internal resume bookkeeping and stay out of the
/// payload; everything else is display data (rendered via textContent only on
/// the client — error text and paths are not trusted).
fn run_to_json(row: &SqliteRow) -> serde_json::Value {
    fn i(row: &SqliteRow, col: &str) -> Option<i64> {
        row.try_get::<Option<i64>, _>(col).unwrap_or(None)
    }
    fn s(row: &SqliteRow, col: &str) -> Option<String> {
        row.try_get::<Option<String>, _>(col).unwrap_or(None)
    }
    json!({
        "run_id": i(row, "run_id"),
        "source": s(row, "source"),
        "state": s(row, "state"),
        "pid": i(row, "pid"),
        "started_at_unix": i(row, "started_at_unix"),
        "updated_at_unix": i(row, "updated_at_unix"),
        "finished_at_unix": i(row, "finished_at_unix"),
        "messages_seen": i(row, "messages_seen").unwrap_or(0),
        "messages_new": i(row, "messages_new").unwrap_or(0),
        "total_estimate": i(row, "total_estimate"),
        "bytes_total": i(row, "bytes_total"),
        "bytes_done": i(row, "bytes_done"),
        "mbox_path": s(row, "mbox_path"),
        "error_kind": s(row, "error_kind"),
        "error": s(row, "error"),
    })
}

/// Probe the ingest lock without disturbing it: open the lockfile exactly the
/// way the ingester does (create-if-missing, never truncate — see
/// `ingest::acquire_ingest_lock`), try a non-blocking exclusive flock, and
/// release immediately. Held ⇒ an ingester is alive right now, whoever
/// started it. Returns None if the probe itself failed (e.g. permissions),
/// which the client treats as "unknown".
fn probe_ingest_lock(db_path: &Path) -> Option<bool> {
    let lock_path = ingest::ingest_lock_path(db_path);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .ok()?;
    match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => {
            // Explicit unlock before the fd drops, so the probe's hold time
            // is as close to zero as possible.
            let _ = rustix::fs::flock(&file, rustix::fs::FlockOperation::Unlock);
            Some(false)
        }
        Err(rustix::io::Errno::WOULDBLOCK) => Some(true),
        Err(_) => None,
    }
}

/// Sliding window of (time, counters) observations for the active run, kept
/// only in viewer memory. Rates come from the span between the oldest and
/// newest sample still inside RATE_WINDOW.
#[derive(Default)]
struct RateWindow {
    run_id: Option<i64>,
    samples: VecDeque<(Instant, i64, Option<i64>)>,
}

impl RateWindow {
    fn reset(&mut self) {
        self.run_id = None;
        self.samples.clear();
    }

    /// Record an observation and return (messages/sec, bytes/sec) over the
    /// current window, if enough history exists to be meaningful.
    fn observe(
        &mut self,
        now: Instant,
        run_id: i64,
        messages_seen: i64,
        bytes_done: Option<i64>,
    ) -> (Option<f64>, Option<f64>) {
        if self.run_id != Some(run_id) {
            self.reset();
            self.run_id = Some(run_id);
        }
        self.samples.push_back((now, messages_seen, bytes_done));
        while let Some(&(t, _, _)) = self.samples.front() {
            if now.duration_since(t) > RATE_WINDOW && self.samples.len() > 2 {
                self.samples.pop_front();
            } else {
                break;
            }
        }

        let (first_t, first_msgs, first_bytes) = *self.samples.front().unwrap();
        let span = now.duration_since(first_t);
        if span < RATE_MIN_SPAN {
            return (None, None);
        }
        let secs = span.as_secs_f64();
        let msg_rate = ((messages_seen - first_msgs).max(0) as f64) / secs;
        let byte_rate = match (bytes_done, first_bytes) {
            (Some(b), Some(fb)) => Some(((b - fb).max(0) as f64) / secs),
            _ => None,
        };
        (Some(msg_rate), byte_rate)
    }
}

/// Estimated seconds to completion. Prefers byte progress (exact for imports)
/// and falls back to the message-count estimate (scans). None when there is
/// no total or no forward progress in the window.
fn compute_eta(
    messages_seen: i64,
    bytes_done: Option<i64>,
    bytes_total: Option<i64>,
    total_estimate: Option<i64>,
    msg_rate: Option<f64>,
    byte_rate: Option<f64>,
) -> Option<i64> {
    if let (Some(done), Some(total), Some(rate)) = (bytes_done, bytes_total, byte_rate) {
        if rate > 0.0 {
            return Some((((total - done).max(0)) as f64 / rate).ceil() as i64);
        }
    }
    if let (Some(total), Some(rate)) = (total_estimate, msg_rate) {
        if rate > 0.0 {
            return Some((((total - messages_seen).max(0)) as f64 / rate).ceil() as i64);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// GET /api/runs — run history
// ---------------------------------------------------------------------------

async fn runs(
    State(state): State<Arc<AppState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let limit: i64 = params
        .get("limit")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(20)
        .clamp(1, 100);
    match build_runs(&state.pool, limit).await {
        Ok(body) => Json(body).into_response(),
        Err(err) => error_response(&err),
    }
}

async fn build_runs(pool: &SqlitePool, limit: i64) -> Result<serde_json::Value, sqlx::Error> {
    let rows = match sqlx::query("SELECT * FROM ingest_runs ORDER BY run_id DESC LIMIT ?")
        .bind(limit)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => rows,
        // A database that predates ingest_runs (or doesn't exist yet) simply
        // has no run history; this endpoint powers a strip, not a diagnosis.
        Err(err) => match classify_db_error(&err) {
            DbErrorKind::MissingFile | DbErrorKind::MissingTable => Vec::new(),
            DbErrorKind::Busy | DbErrorKind::Other => return Err(err),
        },
    };
    let runs: Vec<serde_json::Value> = rows.iter().map(run_to_json).collect();
    Ok(json!({ "runs": runs, "now_unix": now_unix() }))
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn error_response(err: &sqlx::Error) -> Response {
    let detail = err.to_string();
    let lower = detail.to_lowercase();
    let (status, kind, message) =
        if lower.contains("unable to open database file") || lower.contains("no such table") {
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use sqlx::sqlite::SqliteJournalMode;
    use sqlx::{Connection, SqliteConnection};
    use tower::ServiceExt;

    async fn memory_pool() -> SqlitePool {
        // One connection max: each sqlite::memory: connection is a separate
        // empty database, so the pool must never hand out a second one.
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory db");
        // Mirror the README's real schema, quirky type names included.
        sqlx::query("CREATE TABLE seen_mails (mail_id string)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE senders (sender string, mails_sent int)")
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    /// `sender string` matches none of SQLite's affinity keywords, giving the
    /// column NUMERIC affinity: numeric-looking From headers (SMS shortcodes,
    /// `1e5`, `-1.5`) are stored as INTEGER/REAL, which sqlx's String decoder
    /// rejects unless the query CASTs the column back to TEXT. Regression
    /// test: one such row must not 500 the whole summary forever.
    #[tokio::test]
    async fn summary_survives_numeric_sender_rows() {
        let pool = memory_pool().await;
        sqlx::query("INSERT INTO senders VALUES ('40404', 7), ('-1.5', 2), ('a@b.com', 3)")
            .execute(&pool)
            .await
            .unwrap();

        // Sanity-check the poisoned storage classes this test exists for.
        let types: Vec<(String,)> =
            sqlx::query_as("SELECT typeof(sender) FROM senders ORDER BY mails_sent DESC")
                .fetch_all(&pool)
                .await
                .unwrap();
        let types: Vec<&str> = types.iter().map(|(t,)| t.as_str()).collect();
        assert_eq!(types, ["integer", "text", "real"]);

        let summary = build_summary(&pool)
            .await
            .expect("summary must survive numeric-looking sender rows");
        let senders = summary["senders"].as_array().unwrap();
        assert_eq!(senders.len(), 3);
        assert_eq!(senders[0]["sender"], "40404");
        assert_eq!(senders[0]["mails_sent"], 7);
        assert_eq!(senders[1]["sender"], "a@b.com");
        assert_eq!(senders[2]["sender"], "-1.5");
    }

    /// NULL senders must keep merging with literal-'' rows (pre-existing
    /// behavior the CAST fix must not regress).
    #[tokio::test]
    async fn summary_merges_null_and_empty_senders() {
        let pool = memory_pool().await;
        sqlx::query("INSERT INTO senders VALUES (NULL, 4), ('', 1)")
            .execute(&pool)
            .await
            .unwrap();

        let summary = build_summary(&pool).await.unwrap();
        let senders = summary["senders"].as_array().unwrap();
        assert_eq!(senders.len(), 1);
        assert_eq!(senders[0]["sender"], "");
        assert_eq!(senders[0]["mails_sent"], 5);
    }

    // -- Phase B endpoint tests ---------------------------------------------

    /// Build a real on-disk database with Phase A's actual DDL (via
    /// `ingest::migrate`), so the tests exercise the same schema the ingester
    /// produces.
    async fn migrated_db(dir: &tempfile::TempDir) -> PathBuf {
        let db_path = dir.path().join("stats.db");
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        ingest::migrate(&mut conn).await.unwrap();
        conn.close().await.ok();
        db_path
    }

    async fn exec(db_path: &Path, sql: &str) {
        let options = SqliteConnectOptions::new()
            .filename(db_path)
            .journal_mode(SqliteJournalMode::Wal);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_string()))
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.ok();
    }

    async fn get_json(app: Router, uri: &str) -> (StatusCode, serde_json::Value) {
        let response = app
            .oneshot(
                HttpRequest::builder()
                    .uri(uri)
                    .header(header::HOST, "127.0.0.1:7878")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, value)
    }

    fn app_for(db_path: &Path) -> Router {
        build_router(Arc::new(AppState::new(db_path)))
    }

    #[tokio::test]
    async fn status_with_missing_db_is_200_and_onboardable() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("does-not-exist.db");
        let (status, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "status must never 5xx on missing db"
        );
        assert_eq!(body["db"], "missing");
        assert!(body["active_run"].is_null());
        assert!(body["last_run"].is_null());
        assert_eq!(body["owns_active_run"], false);
        assert_eq!(body["mixed_sources"], false);
        // CSRF token slot: 32 CSPRNG bytes, hex-encoded.
        let token = body["csrf_token"].as_str().unwrap();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
        // The read-only pool must not have created the file as a side effect.
        assert!(!db_path.exists(), "viewer must not create the database");
    }

    #[tokio::test]
    async fn status_with_empty_db_reports_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        let (status, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["db"], "empty");
        assert!(body["active_run"].is_null());
        assert_eq!(body["ingest_lock_held"], false);
    }

    #[tokio::test]
    async fn status_without_ingest_runs_table_treats_runs_as_none() {
        // A pre-Phase-A database: baseline tables with data, no ingest_runs.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("old.db");
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("CREATE TABLE seen_mails (mail_id string)")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE senders (sender string, mails_sent int)")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO seen_mails VALUES ('m1')")
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.ok();

        let (status, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["db"], "ready");
        assert!(body["active_run"].is_null());
        assert!(body["last_run"].is_null());

        let (status, body) = get_json(app_for(&db_path), "/api/runs").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["runs"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn status_with_active_and_finished_runs() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        exec(&db_path, "INSERT INTO seen_mails VALUES ('m1')").await;
        exec(
            &db_path,
            "INSERT INTO ingest_runs \
             (run_id, source, state, started_at_unix, updated_at_unix, finished_at_unix, \
              messages_seen, messages_new, error_kind, error) \
             VALUES (1, 'gmail_api', 'failed', 100, 150, 150, 10, 5, \
                     'policy_enforced', 'Error 400: policy_enforced')",
        )
        .await;
        exec(
            &db_path,
            "INSERT INTO ingest_runs \
             (run_id, source, state, started_at_unix, updated_at_unix, \
              messages_seen, messages_new, bytes_total, bytes_done, mbox_path) \
             VALUES (2, 'mbox', 'running', 200, 205, 1000, 900, 50000, 25000, '/tmp/a.mbox')",
        )
        .await;

        let (status, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["db"], "ready");
        let active = &body["active_run"];
        assert_eq!(active["run_id"], 2);
        assert_eq!(active["source"], "mbox");
        assert_eq!(active["state"], "running");
        assert_eq!(active["messages_seen"], 1000);
        assert_eq!(active["bytes_total"], 50000);
        assert_eq!(active["bytes_done"], 25000);
        let last = &body["last_run"];
        assert_eq!(last["run_id"], 1);
        assert_eq!(last["state"], "failed");
        assert_eq!(last["error_kind"], "policy_enforced");
        assert_eq!(body["owns_active_run"], false);
        // First observation of this run: no rate window yet.
        assert!(body["rate_per_sec"].is_null());
        assert!(body["now_unix"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn status_mixed_sources_flags_dual_source_databases() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        exec(&db_path, "INSERT INTO seen_mails VALUES ('m1')").await;
        exec(
            &db_path,
            "INSERT INTO ingest_runs (run_id, source, state, started_at_unix, updated_at_unix, \
             messages_new) VALUES (1, 'gmail_api', 'done', 100, 150, 10)",
        )
        .await;
        let (_, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(body["mixed_sources"], false);

        exec(
            &db_path,
            "INSERT INTO ingest_runs (run_id, source, state, started_at_unix, updated_at_unix, \
             messages_new) VALUES (2, 'mbox', 'done', 200, 250, 3)",
        )
        .await;
        let (_, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(body["mixed_sources"], true);
    }

    #[tokio::test]
    async fn status_lock_probe_detects_a_running_ingester_without_truncating() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        let lock_path = ingest::ingest_lock_path(&db_path);
        // Pre-existing lockfile content must survive the probe (no truncate).
        std::fs::write(&lock_path, b"sentinel").unwrap();

        let (_, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(body["ingest_lock_held"], false);

        // A live ingester holds the flock; the probe must see it and must not
        // steal or break it.
        let held = ingest::acquire_ingest_lock(&db_path).unwrap();
        let (_, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(body["ingest_lock_held"], true);
        drop(held);

        let (_, body) = get_json(app_for(&db_path), "/api/status").await;
        assert_eq!(body["ingest_lock_held"], false);
        assert_eq!(
            std::fs::read(&lock_path).unwrap(),
            b"sentinel",
            "probe must never truncate the lockfile"
        );
    }

    #[tokio::test]
    async fn runs_endpoint_orders_and_limits() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        for id in 1..=5 {
            exec(
                &db_path,
                &format!(
                    "INSERT INTO ingest_runs (run_id, source, state, started_at_unix, \
                     updated_at_unix) VALUES ({id}, 'mbox', 'done', {id}00, {id}50)"
                ),
            )
            .await;
        }
        let (status, body) = get_json(app_for(&db_path), "/api/runs?limit=3").await;
        assert_eq!(status, StatusCode::OK);
        let runs = body["runs"].as_array().unwrap();
        assert_eq!(runs.len(), 3);
        // Newest first.
        assert_eq!(runs[0]["run_id"], 5);
        assert_eq!(runs[1]["run_id"], 4);
        assert_eq!(runs[2]["run_id"], 3);

        // Default limit, garbage limit, and missing db all behave.
        let (_, body) = get_json(app_for(&db_path), "/api/runs").await;
        assert_eq!(body["runs"].as_array().unwrap().len(), 5);
        let (_, body) = get_json(app_for(&db_path), "/api/runs?limit=bogus").await;
        assert_eq!(body["runs"].as_array().unwrap().len(), 5);
        let missing = dir.path().join("nope.db");
        let (status, body) = get_json(app_for(&missing), "/api/runs").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["runs"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn new_routes_keep_security_headers_and_emit_no_cors() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        for uri in ["/api/status", "/api/runs"] {
            let response = app_for(&db_path)
                .oneshot(
                    HttpRequest::builder()
                        .uri(uri)
                        .header(header::HOST, "127.0.0.1:7878")
                        .header(header::ORIGIN, "https://evil.example")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let headers = response.headers();
            assert_eq!(
                headers.get(header::CONTENT_SECURITY_POLICY).unwrap(),
                "default-src 'self'",
                "{uri} lost the CSP"
            );
            assert_eq!(
                headers.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
                "nosniff"
            );
            assert_eq!(headers.get(header::REFERRER_POLICY).unwrap(), "no-referrer");
            assert!(
                headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).is_none(),
                "{uri} must never emit CORS headers"
            );
            assert!(headers.get(header::ACCESS_CONTROL_ALLOW_METHODS).is_none());
        }
    }

    #[tokio::test]
    async fn new_routes_respect_the_host_guard() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        for uri in ["/api/status", "/api/runs"] {
            let response = app_for(&db_path)
                .oneshot(
                    HttpRequest::builder()
                        .uri(uri)
                        .header(header::HOST, "attacker.example")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{uri}");
        }
    }

    #[tokio::test]
    async fn csrf_token_is_stable_per_process_state() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = migrated_db(&dir).await;
        let state = Arc::new(AppState::new(&db_path));
        let (_, a) = get_json(build_router(state.clone()), "/api/status").await;
        let (_, b) = get_json(build_router(state.clone()), "/api/status").await;
        assert_eq!(a["csrf_token"], b["csrf_token"]);
        // And two distinct viewer processes mint distinct tokens.
        let other = Arc::new(AppState::new(&db_path));
        assert_ne!(state.csrf_token, other.csrf_token);
    }

    #[test]
    fn rate_window_computes_rates_and_resets_across_runs() {
        let mut window = RateWindow::default();
        let t0 = Instant::now();
        // First sample: no span yet.
        assert_eq!(window.observe(t0, 1, 100, Some(1000)), (None, None));
        // Two seconds later, 80 more messages and 8000 more bytes.
        let (msg, bytes) = window.observe(t0 + Duration::from_secs(2), 1, 180, Some(9000));
        assert_eq!(msg, Some(40.0));
        assert_eq!(bytes, Some(4000.0));

        // ETA from bytes: (50000-9000)/4000 = 10.25 → 11s.
        let eta = compute_eta(180, Some(9000), Some(50_000), None, msg, bytes);
        assert_eq!(eta, Some(11));
        // ETA from message estimate when no byte totals exist.
        let eta = compute_eta(180, None, None, Some(580), msg, None);
        assert_eq!(eta, Some(10));
        // No forward progress ⇒ no ETA rather than a division blowup.
        let (msg, _) = window.observe(t0 + Duration::from_secs(3), 1, 180, Some(9000));
        assert!(eta_is_none_when_rate_zero(msg));

        // A new run id resets the window.
        assert_eq!(
            window.observe(t0 + Duration::from_secs(4), 2, 5, None),
            (None, None)
        );
    }

    fn eta_is_none_when_rate_zero(msg_rate: Option<f64>) -> bool {
        compute_eta(180, None, None, Some(580), msg_rate.map(|_| 0.0), None).is_none()
    }
}
