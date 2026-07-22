//! Local web viewer for gmail-stats (issue #11 Phase 1, issue #28 Phase B,
//! issue #30 Phase C).
//!
//! Serves the embedded UI plus JSON endpoints over `./stats.db`, bound to
//! 127.0.0.1 only. Phase B added observe-only ingestion state: `GET
//! /api/status` (db readiness, active/last run, flock probe, viewer-side
//! rate/ETA, CSRF token) and `GET /api/runs` (run history). Phase C adds the
//! state-changing surface: `POST /api/runs` (spawn the ingester as a
//! supervised child), `POST /api/runs/{id}/cancel` (SIGTERM the owned child,
//! SIGKILL escalation), and `GET /api/runs/{id}/log` (in-memory stderr ring
//! buffer of owned children).
//!
//! The viewer still performs no database writes of any kind: every durable
//! state change happens inside the spawned `gmail_stats` ingester process.
//! The only files the viewer touches are the ingest lockfile (probed without
//! truncation via open + try-lock + release) — its new powers are exactly
//! spawn, signal-own-child, and hold a stderr ring buffer in memory.
//!
//! Every mutating route sits behind the full CSRF stack from issue #26:
//! strict `Content-Type: application/json` (415 otherwise), the per-process
//! `X-Gmail-Stats-Csrf` token served by `/api/status`, and Origin /
//! Sec-Fetch-Site validation when those headers are present. No CORS headers
//! are ever emitted.
//!
//! This module is the whole application; `src/bin/web.rs` is a thin `main`.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path as UrlPath, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteRow};
use sqlx::{Row, SqlitePool};
use tokio::io::AsyncBufReadExt;
use tokio::sync::mpsc;

use crate::ingest;

// CSS/JS are separate same-origin assets, not inline blocks: under the
// `default-src 'self'` CSP below, `'self'` only allowlists same-origin URLs —
// inline <style>/<script> would be refused by the browser.
const INDEX_HTML: &str = include_str!("../web/index.html");
const APP_CSS: &str = include_str!("../web/app.css");
const APP_JS: &str = include_str!("../web/app.js");
const DEFAULT_PORT: u16 = 7878;

/// How far back the in-memory rate window looks. Old observations age out so
/// the displayed rate tracks the recent pace, not the whole run's average.
const RATE_WINDOW: Duration = Duration::from_secs(30);
/// Minimum span between oldest and newest observation before a rate is
/// reported at all; two samples a few ms apart would just be noise.
const RATE_MIN_SPAN: Duration = Duration::from_millis(500);

/// The custom header carrying the per-process CSRF token. Custom headers
/// force a CORS preflight from cross-origin JS, which fails because this
/// server never emits CORS headers; the random value holds even under
/// degraded fetch-metadata support.
const CSRF_HEADER: &str = "x-gmail-stats-csrf";
/// Cap on the in-memory stderr ring buffer of a spawned child.
const LOG_RING_LINES: usize = 200;
/// Cap on a single retained stderr line (bytes), so a pathological child
/// cannot balloon viewer memory through 200 giant lines.
const LOG_LINE_MAX: usize = 4096;
/// How long POST /api/runs waits for the spawned child to write its run row
/// before giving up with spawn_failed.
const ROW_WAIT: Duration = Duration::from_secs(10);
/// Grace period between SIGTERM and SIGKILL on cancel.
const TERM_GRACE: Duration = Duration::from_secs(10);

/// Shared state for the router. Everything is read-only against the database;
/// the rate window, CSRF token, child handles, and stderr ring buffers live
/// purely in viewer memory and are never persisted.
pub struct AppState {
    pool: SqlitePool,
    db_path: PathBuf,
    /// Per-process CSRF token (32 bytes of CSPRNG output, hex-encoded),
    /// minted at startup and obtainable only via the same-origin
    /// `GET /api/status`. Required in the CSRF_HEADER of every mutating
    /// request.
    csrf_token: String,
    rate: Mutex<RateWindow>,
    /// Where the ingester binary lives: GMAIL_STATS_INGEST_BIN, or a sibling
    /// of current_exe() named `gmail_stats`.
    ingester_bin: PathBuf,
    /// SIGTERM → SIGKILL escalation window (only tests shrink it).
    term_grace: Duration,
    /// Runs this viewer process spawned, by run_id. Entries persist after the
    /// child exits so the log stays readable; they are memory-only and gone on
    /// viewer restart (the child itself survives — kill_on_drop is false).
    children: Mutex<HashMap<i64, Arc<OwnedRun>>>,
    /// Serializes spawn attempts from this viewer so two racing POSTs don't
    /// both pass the friendly pre-check. The child's own flock acquisition
    /// remains the authoritative cross-process gate.
    spawn_lock: tokio::sync::Mutex<()>,
}

/// The in-memory handle to a child this viewer spawned. The pid is captured
/// from our own child handle at spawn time — never read back from the
/// database (PID reuse) — and signals only ever travel through the supervisor
/// task that owns the `tokio::process::Child`.
struct OwnedRun {
    pid: u32,
    ring: Arc<Mutex<VecDeque<String>>>,
    cancel_tx: mpsc::Sender<()>,
    exited: Arc<AtomicBool>,
}

/// Entry point used by the `web` binary: parse the port argument, bind to
/// loopback, serve.
pub async fn serve() -> anyhow::Result<()> {
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

/// Resolve the ingester binary: env override first, then a sibling of the
/// current executable (both binaries come out of the same `cargo build`).
fn default_ingester_bin() -> PathBuf {
    if let Ok(path) = std::env::var("GMAIL_STATS_INGEST_BIN") {
        return PathBuf::from(path);
    }
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(|dir| dir.join("gmail_stats")))
        .unwrap_or_else(|| PathBuf::from("gmail_stats"))
}

impl AppState {
    pub fn new(db_path: &Path) -> Self {
        Self::configured(db_path, default_ingester_bin(), TERM_GRACE)
    }

    /// Full-control constructor for tests: a custom ingester binary (fake
    /// ingester) and a shortened SIGTERM→SIGKILL grace.
    pub fn configured(db_path: &Path, ingester_bin: PathBuf, term_grace: Duration) -> Self {
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
            ingester_bin,
            term_grace,
            children: Mutex::new(HashMap::new()),
            spawn_lock: tokio::sync::Mutex::new(()),
        }
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.css", get(app_css))
        .route("/app.js", get(app_js))
        .route("/api/summary", get(summary))
        .route("/api/status", get(status))
        .route("/api/runs", get(runs).post(start_run))
        .route("/api/runs/{id}/cancel", post(cancel_run))
        .route("/api/runs/{id}/log", get(run_log))
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

    // A run is "owned" when this viewer process spawned it and still holds
    // the in-memory child handle: only then are Cancel and Log available.
    let owns_active_run = active_run
        .as_ref()
        .and_then(|row| row.try_get::<i64, _>("run_id").ok())
        .map(|run_id| {
            state
                .children
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&run_id)
        })
        .unwrap_or(false);

    json!({
        "db": db,
        "now_unix": now_unix(),
        "ingest_lock_held": lock_held,
        "active_run": active_run.as_ref().map(run_to_json),
        "last_run": last_run.as_ref().map(run_to_json),
        "owns_active_run": owns_active_run,
        "mixed_sources": mixed_sources,
        "rate_per_sec": rate_per_sec,
        "eta_seconds": eta_seconds,
        "csrf_token": state.csrf_token,
    })
}

/// Serialize one ingest_runs row for the API. resume_token and
/// mbox_fingerprint are internal resume bookkeeping and stay out of the
/// payload; everything else is display data (rendered via textContent only on
/// the client — error text, paths, and auth_url are not trusted; the client
/// refuses to hyperlink auth_url unless it parses as
/// https://accounts.google.com/...).
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
        "auth_url": s(row, "auth_url"),
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

// ---------------------------------------------------------------------------
// Phase C: mutating routes (POST /api/runs, POST /api/runs/{id}/cancel) and
// the owned-child log (GET /api/runs/{id}/log)
// ---------------------------------------------------------------------------

fn json_error(status: StatusCode, kind: &str, message: &str) -> Response {
    (status, Json(json!({ "error": kind, "message": message }))).into_response()
}

/// Constant-time byte comparison for the CSRF token, so the check leaks no
/// prefix-length timing signal.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Is this Origin header value an allowed same-machine origin? Only
/// `http://127.0.0.1[:port]` and `http://localhost[:port]` qualify — the port
/// is not pinned (mirroring the Host guard: any loopback-origin page served
/// to this browser is the same local user, and the token check is the
/// unforgeable layer).
fn origin_allowed(origin: &str) -> bool {
    let Some(rest) = origin.strip_prefix("http://") else {
        return false;
    };
    let (host, port) = match rest.split_once(':') {
        Some((host, port)) => (host, Some(port)),
        None => (rest, None),
    };
    if host != "127.0.0.1" && host != "localhost" {
        return false;
    }
    match port {
        None => true,
        Some(port) => !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()),
    }
}

/// The three-layer CSRF gate from issue #26, applied to every mutating route
/// (all three layers required):
///
/// 1. Strict `Content-Type: application/json`, else 415. HTML forms cannot
///    produce it, and cross-origin JS that sets it triggers a CORS preflight
///    that fails because this server never emits CORS headers.
/// 2. `X-Gmail-Stats-Csrf` must equal the per-process CSPRNG token, which is
///    obtainable only via the same-origin `GET /api/status`.
/// 3. If `Origin` is present it must be a loopback http origin; if
///    `Sec-Fetch-Site` is present it must be `same-origin` or `none`. Absent
///    headers pass: curl is the same local user, who can already run the CLI.
// The Err variant is a full HTTP Response by design (it is sent as-is);
// boxing it would just add noise on a cold path.
#[allow(clippy::result_large_err)]
fn check_csrf(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let essence = content_type.split(';').next().unwrap_or("").trim();
    if !essence.eq_ignore_ascii_case("application/json") {
        return Err(json_error(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "content_type",
            "Content-Type must be application/json",
        ));
    }

    let presented = headers
        .get(CSRF_HEADER)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !constant_time_eq(presented.as_bytes(), state.csrf_token.as_bytes()) {
        return Err(json_error(
            StatusCode::FORBIDDEN,
            "csrf",
            "missing or invalid X-Gmail-Stats-Csrf token (fetch it from GET /api/status)",
        ));
    }

    if let Some(origin) = headers.get(header::ORIGIN) {
        let allowed = origin.to_str().is_ok_and(origin_allowed);
        if !allowed {
            return Err(json_error(
                StatusCode::FORBIDDEN,
                "csrf",
                "cross-origin requests are not allowed",
            ));
        }
    }
    if let Some(site) = headers.get("sec-fetch-site") {
        let allowed = matches!(site.to_str(), Ok("same-origin") | Ok("none"));
        if !allowed {
            return Err(json_error(
                StatusCode::FORBIDDEN,
                "csrf",
                "cross-site requests are not allowed",
            ));
        }
    }
    Ok(())
}

/// What POST /api/runs decided to launch. Argv is assembled from these fields
/// as a fixed array — never a shell — and the only user-influenced element is
/// a single already-validated path argument.
enum SpawnPlan {
    Scan { resume: Option<i64> },
    Import { path: PathBuf, resume: Option<i64> },
}

/// Stat-level mbox validation (UX, not a security boundary): absolute,
/// regular, readable, and starting with the mbox `From ` magic so a confused
/// click can't slurp an arbitrary file into sender stats.
fn validate_mbox_path(raw: &str) -> Result<PathBuf, String> {
    use std::io::Read;
    let path = PathBuf::from(raw);
    if !path.is_absolute() {
        return Err(
            "path must be absolute (browsers do not reveal picked-file paths; \
                    paste the full path)"
                .to_string(),
        );
    }
    let meta = match std::fs::metadata(&path) {
        Ok(meta) => meta,
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };
    if !meta.is_file() {
        return Err(format!("{} is not a regular file", path.display()));
    }
    let mut file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(e) => return Err(format!("cannot open {}: {e}", path.display())),
    };
    let mut magic = [0u8; 5];
    if file.read_exact(&mut magic).is_err() || &magic != b"From " {
        return Err(format!(
            "{} does not look like an mbox file (expected it to start with a `From ` separator)",
            path.display()
        ));
    }
    Ok(path)
}

async fn start_run(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if let Err(response) = check_csrf(&state, &headers) {
        return response;
    }
    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return json_error(StatusCode::BAD_REQUEST, "bad_request", "body must be JSON"),
    };
    let resume_run_id = parsed["resume_run_id"].as_i64();
    let plan = match parsed["source"].as_str() {
        Some("gmail_api") => {
            if let Some(run_id) = resume_run_id {
                // The scanner enforces this too; checking here turns a typo
                // into a friendly 422 instead of a failed spawn.
                match run_source(&state.pool, run_id).await {
                    Some(source) if source == "gmail_api" => {}
                    Some(source) => {
                        return json_error(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            "bad_resume",
                            &format!("run {run_id} is a {source} run, not a Gmail API scan"),
                        )
                    }
                    None => {
                        return json_error(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            "bad_resume",
                            &format!("no ingest run {run_id} to resume"),
                        )
                    }
                }
            }
            SpawnPlan::Scan {
                resume: resume_run_id,
            }
        }
        Some("mbox") => {
            let raw_path = match (parsed["path"].as_str(), resume_run_id) {
                (Some(path), _) => path.to_string(),
                (None, Some(run_id)) => match run_mbox_path(&state.pool, run_id).await {
                    Some(path) => path,
                    None => {
                        return json_error(
                            StatusCode::UNPROCESSABLE_ENTITY,
                            "bad_resume",
                            &format!("run {run_id} is not a resumable mbox import"),
                        )
                    }
                },
                (None, None) => {
                    return json_error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "bad_path",
                        "mbox imports need a \"path\" (or a \"resume_run_id\")",
                    )
                }
            };
            match validate_mbox_path(&raw_path) {
                Ok(path) => SpawnPlan::Import {
                    path,
                    resume: resume_run_id,
                },
                Err(message) => {
                    return json_error(StatusCode::UNPROCESSABLE_ENTITY, "bad_path", &message)
                }
            }
        }
        _ => {
            return json_error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "bad_source",
                "source must be \"gmail_api\" or \"mbox\"",
            )
        }
    };

    // Serialize spawns from this viewer; then the friendly pre-check. The
    // child's own flock acquisition is the authoritative gate, so the
    // remaining TOCTOU window is harmless (it surfaces as run_active below).
    let _guard = state.spawn_lock.lock().await;
    if probe_ingest_lock(&state.db_path) == Some(true) {
        return json_error(
            StatusCode::CONFLICT,
            "run_active",
            "an ingester is already running against this database",
        );
    }
    if !state.ingester_bin.is_file() {
        return json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "spawn_failed",
            &format!(
                "ingester binary not found at {} — build it with `cargo build`, \
                 or point GMAIL_STATS_INGEST_BIN at it",
                state.ingester_bin.display()
            ),
        );
    }

    match spawn_and_register(&state, &plan).await {
        Ok(run_id) => (StatusCode::ACCEPTED, Json(json!({ "run_id": run_id }))).into_response(),
        Err(response) => response,
    }
}

async fn run_source(pool: &SqlitePool, run_id: i64) -> Option<String> {
    sqlx::query_scalar::<_, String>("SELECT source FROM ingest_runs WHERE run_id = ?")
        .bind(run_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
}

async fn run_mbox_path(pool: &SqlitePool, run_id: i64) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT mbox_path FROM ingest_runs WHERE run_id = ? AND source = 'mbox'",
    )
    .bind(run_id)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .flatten()
}

async fn max_run_id(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(run_id) FROM ingest_runs")
        .fetch_one(pool)
        .await
        .ok()
        .flatten()
        .unwrap_or(0)
}

async fn find_new_run(pool: &SqlitePool, after: i64, pid: u32) -> Option<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT run_id FROM ingest_runs WHERE run_id > ? AND pid = ? \
         ORDER BY run_id DESC LIMIT 1",
    )
    .bind(after)
    .bind(pid as i64)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

fn push_log_line(ring: &Mutex<VecDeque<String>>, mut line: String) {
    if line.len() > LOG_LINE_MAX {
        let mut cut = LOG_LINE_MAX;
        while !line.is_char_boundary(cut) {
            cut -= 1;
        }
        line.truncate(cut);
    }
    let mut ring = ring.lock().unwrap_or_else(|e| e.into_inner());
    if ring.len() == LOG_RING_LINES {
        ring.pop_front();
    }
    ring.push_back(line);
}

/// Spawn the ingester with a fixed argv (never a shell), wire up stderr
/// capture and the supervisor, wait for the child to write its run row, and
/// register ownership. Returns the run_id for the 202 body.
#[allow(clippy::result_large_err)] // the Err is the HTTP response, sent as-is
async fn spawn_and_register(state: &AppState, plan: &SpawnPlan) -> Result<i64, Response> {
    let before = max_run_id(&state.pool).await;
    // Absolute db path: the child inherits our cwd today, but the argv should
    // not depend on that staying true.
    let db_abs = std::path::absolute(&state.db_path).unwrap_or_else(|_| state.db_path.clone());

    let mut cmd = tokio::process::Command::new(&state.ingester_bin);
    match plan {
        SpawnPlan::Scan { resume } => {
            cmd.arg("scan");
            if let Some(run_id) = resume {
                cmd.arg("--resume").arg(run_id.to_string());
            }
        }
        SpawnPlan::Import { path, resume } => {
            cmd.arg("import").arg(path);
            if let Some(run_id) = resume {
                cmd.arg("--resume").arg(run_id.to_string());
            }
        }
    }
    cmd.arg("--quiet")
        .arg("--db")
        .arg(&db_abs)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        // The run must survive a viewer restart: the child is reparented and
        // keeps writing; only Cancel and Log are lost.
        .kill_on_drop(false);

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => {
            return Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "spawn_failed",
                &format!(
                    "could not launch {}: {e} — build it with `cargo build`",
                    state.ingester_bin.display()
                ),
            ))
        }
    };
    let Some(pid) = child.id() else {
        return Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "spawn_failed",
            "child exited before it could be observed",
        ));
    };

    let ring = Arc::new(Mutex::new(VecDeque::new()));
    let exited = Arc::new(AtomicBool::new(false));
    let (cancel_tx, cancel_rx) = mpsc::channel::<()>(1);

    // stderr → bounded in-memory ring buffer; never persisted anywhere.
    if let Some(stderr) = child.stderr.take() {
        let ring = ring.clone();
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                push_log_line(&ring, line);
            }
        });
    }
    tokio::spawn(supervise_child(
        child,
        cancel_rx,
        exited.clone(),
        state.term_grace,
    ));

    // The 202 contract includes the run_id, which the *child* mints by
    // inserting its run row; wait for it to appear (matched by our child's
    // pid, newer than anything that existed before the spawn).
    let deadline = Instant::now() + ROW_WAIT;
    loop {
        if let Some(run_id) = find_new_run(&state.pool, before, pid).await {
            let owned = Arc::new(OwnedRun {
                pid,
                ring,
                cancel_tx,
                exited,
            });
            state
                .children
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(run_id, owned);
            return Ok(run_id);
        }
        if exited.load(Ordering::Relaxed) {
            // One more look: a tiny import can legitimately finish before the
            // first poll tick.
            if let Some(run_id) = find_new_run(&state.pool, before, pid).await {
                let owned = Arc::new(OwnedRun {
                    pid,
                    ring,
                    cancel_tx,
                    exited,
                });
                state
                    .children
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(run_id, owned);
                return Ok(run_id);
            }
            let tail: String = {
                let ring = ring.lock().unwrap_or_else(|e| e.into_inner());
                ring.iter()
                    .rev()
                    .take(5)
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(" | ")
            };
            // The authoritative flock gate lost a race we pre-checked: the
            // child refused to run because another ingester holds the lock.
            if tail.contains("another ingester is already running") {
                return Err(json_error(
                    StatusCode::CONFLICT,
                    "run_active",
                    "an ingester is already running against this database",
                ));
            }
            return Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "spawn_failed",
                &format!("the ingester exited before reporting a run: {tail}"),
            ));
        }
        if Instant::now() >= deadline {
            return Err(json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "spawn_failed",
                "the ingester started but never reported a run row",
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Owns the `tokio::process::Child`. Normally just reaps it; on a cancel
/// request it SIGTERMs the child (clean drain + `cancelled` row, exactly like
/// terminal Ctrl-C), escalating to SIGKILL after the grace period. A
/// SIGKILLed child leaves its row open; the next ingester's janitor marks it
/// abandoned — the viewer itself never writes to the database.
async fn supervise_child(
    mut child: tokio::process::Child,
    mut cancel_rx: mpsc::Receiver<()>,
    exited: Arc<AtomicBool>,
    grace: Duration,
) {
    let pid = child.id();
    tokio::select! {
        _ = child.wait() => {}
        Some(()) = cancel_rx.recv() => {
            // The pid comes from our own child handle at spawn, never from
            // the database.
            if let Some(pid) = pid.and_then(|p| rustix::process::Pid::from_raw(p as i32)) {
                let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
            }
            if tokio::time::timeout(grace, child.wait()).await.is_err() {
                // SIGTERM ignored: SIGKILL and reap.
                let _ = child.kill().await;
            }
        }
    }
    exited.store(true, Ordering::Relaxed);
}

async fn cancel_run(
    State(state): State<Arc<AppState>>,
    UrlPath(run_id): UrlPath<i64>,
    headers: HeaderMap,
) -> Response {
    if let Err(response) = check_csrf(&state, &headers) {
        return response;
    }
    let owned = state
        .children
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&run_id)
        .cloned();
    match owned {
        None => json_error(
            StatusCode::CONFLICT,
            "not_owned",
            "this viewer did not start that run — cancel it from its own terminal (Ctrl-C)",
        ),
        Some(run) => {
            // Idempotent: repeated cancels (or cancelling an already-exited
            // child) are no-ops.
            let _ = run.cancel_tx.try_send(());
            (
                StatusCode::ACCEPTED,
                Json(json!({ "run_id": run_id, "cancelling": true })),
            )
                .into_response()
        }
    }
}

async fn run_log(State(state): State<Arc<AppState>>, UrlPath(run_id): UrlPath<i64>) -> Response {
    let owned = state
        .children
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&run_id)
        .cloned();
    match owned {
        None => json_error(
            StatusCode::NOT_FOUND,
            "not_owned",
            "no in-memory log for that run (it was started elsewhere, or the viewer restarted)",
        ),
        Some(run) => {
            let lines: Vec<String> = {
                let ring = run.ring.lock().unwrap_or_else(|e| e.into_inner());
                ring.iter().cloned().collect()
            };
            Json(json!({
                "run_id": run_id,
                "pid": run.pid,
                "exited": run.exited.load(Ordering::Relaxed),
                "lines": lines,
            }))
            .into_response()
        }
    }
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
    fn origin_allowlist_is_loopback_http_only() {
        for good in [
            "http://127.0.0.1:7878",
            "http://127.0.0.1",
            "http://localhost:7878",
            "http://localhost",
            "http://localhost:1",
        ] {
            assert!(origin_allowed(good), "{good} should be allowed");
        }
        for bad in [
            "https://127.0.0.1:7878",
            "https://accounts.google.com",
            "http://evil.example",
            "http://127.0.0.1.evil.example",
            "http://localhost:",
            "http://localhost:7878/path",
            "http://localhost:78x8",
            "null",
            "",
        ] {
            assert!(!origin_allowed(bad), "{bad} must be rejected");
        }
    }

    #[test]
    fn constant_time_eq_compares_correctly() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn mbox_path_validation_is_stat_plus_magic_only() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("mail.mbox");
        std::fs::write(&good, b"From a@example.com\nFrom: a@example.com\n\nhi\n").unwrap();
        assert_eq!(validate_mbox_path(good.to_str().unwrap()).unwrap(), good);

        let not_mbox = dir.path().join("notes.txt");
        std::fs::write(&not_mbox, b"not mail").unwrap();
        for (raw, expected) in [
            ("relative/mail.mbox", "absolute"),
            (
                dir.path().join("missing.mbox").to_str().unwrap(),
                "cannot read",
            ),
            (dir.path().to_str().unwrap(), "not a regular file"),
            (not_mbox.to_str().unwrap(), "does not look like an mbox"),
        ] {
            let err = validate_mbox_path(raw).unwrap_err();
            assert!(err.contains(expected), "{raw}: {err}");
        }
    }

    #[test]
    fn log_ring_caps_lines_and_line_length() {
        let ring = Mutex::new(VecDeque::new());
        for i in 0..(LOG_RING_LINES + 30) {
            push_log_line(&ring, format!("line {i}"));
        }
        push_log_line(&ring, "x".repeat(LOG_LINE_MAX + 100));
        let ring = ring.lock().unwrap();
        assert_eq!(ring.len(), LOG_RING_LINES);
        assert_eq!(ring.front().unwrap(), "line 31");
        assert_eq!(ring.back().unwrap().len(), LOG_LINE_MAX);
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
