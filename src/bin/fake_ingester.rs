//! Test-only stand-in for the real ingester, used by the web viewer's
//! supervision tests (tests/web_mutating.rs) to drive child-process paths that
//! are impractical with the real binary: crashing mid-run, ignoring SIGTERM,
//! emitting controlled stderr. It is never spawned in production — the viewer
//! only launches it when GMAIL_STATS_INGEST_BIN points at it explicitly.
//!
//! It speaks just enough of the real CLI (`scan`/`import <path>`, `--db`,
//! `--quiet`, `--resume`) to be spawned by the viewer, takes the real ingest
//! flock, and writes real `ingest_runs` rows, so the viewer's observations are
//! exercised against the genuine coordination primitives.
//!
//! Behavior is selected with FAKE_INGESTER_MODE:
//! - `complete` (default): run row -> brief work -> done, exit 0
//! - `crash`: run row, then exit(1) leaving the row open (flock freed by the
//!   kernel; the next real ingester's janitor marks it abandoned)
//! - `hang_trap`: run row, then wait; on SIGTERM mark the row cancelled and
//!   exit 0 (the real ingester's clean-shutdown contract)
//! - `hang_ignore`: run row, then wait forever, ignoring SIGTERM (forces the
//!   viewer's SIGKILL escalation)
//! - `stderr`: emit FAKE_INGESTER_STDERR_LINES lines (default 250) to stderr,
//!   then behave like `hang_trap`
//! - `norow`: print a failure to stderr and exit(1) before any row exists

use std::path::PathBuf;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, SqliteConnection};

use gmail_stats::ingest;

fn parse_args() -> (String, PathBuf) {
    let mut source = "gmail_api".to_string();
    let mut db = PathBuf::from("stats.db");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "scan" => source = "gmail_api".to_string(),
            "import" => {
                source = "mbox".to_string();
                let _path = args.next();
            }
            "--db" => {
                if let Some(path) = args.next() {
                    db = PathBuf::from(path);
                }
            }
            "--resume" => {
                let _ = args.next();
            }
            "--quiet" | "--verbose" => {}
            other => {
                eprintln!("fake ingester: ignoring argument {other:?}");
            }
        }
    }
    (source, db)
}

/// Tests run in parallel inside one process, so a global env var cannot pick
/// the mode per spawn; a `fake_mode` file next to the target database wins,
/// falling back to FAKE_INGESTER_MODE, then `complete`.
fn resolve_mode(db: &std::path::Path) -> String {
    if let Some(dir) = db.parent() {
        if let Ok(mode) = std::fs::read_to_string(dir.join("fake_mode")) {
            let mode = mode.trim().to_string();
            if !mode.is_empty() {
                return mode;
            }
        }
    }
    std::env::var("FAKE_INGESTER_MODE").unwrap_or_else(|_| "complete".to_string())
}

/// Hard lifetime cap so a test that forgets to cancel can never leak an
/// eternally sleeping process.
const MAX_LIFE: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (source, db) = parse_args();
    let mode = resolve_mode(&db);

    if mode == "norow" {
        eprintln!("fake ingester: refusing to start (norow mode)");
        std::process::exit(1);
    }
    if mode == "flockfail" {
        // Simulate losing the flock race *after* the viewer's friendly
        // pre-check passed (the TOCTOU window): same message and exit as the
        // real acquire_ingest_lock failure.
        eprintln!(
            "Error: another ingester is already running against {} \
             (could not acquire exclusive lock); wait for it to finish or stop it first",
            db.display()
        );
        std::process::exit(1);
    }

    // The real flock, so the viewer's pre-check/probe sees the truth.
    let _lock = ingest::acquire_ingest_lock(&db)?;

    let options = SqliteConnectOptions::new()
        .filename(&db)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5));
    let mut conn = SqliteConnection::connect_with(&options).await?;
    ingest::migrate(&mut conn).await?;
    ingest::abandon_stale_runs(&mut conn).await?;
    let run_id = ingest::create_run(&mut conn, &source, None).await?;
    conn.close().await.ok();

    if mode == "stderr" {
        let lines: usize = std::env::var("FAKE_INGESTER_STDERR_LINES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(250);
        for i in 1..=lines {
            eprintln!("fake stderr line {i}");
        }
    }

    match mode.as_str() {
        "complete" => {
            tokio::time::sleep(Duration::from_millis(200)).await;
            ingest::finish_run(&options, run_id, "done", None, None).await?;
        }
        "crash" => {
            eprintln!("fake ingester: crashing mid-run");
            std::process::exit(1);
        }
        "hang_ignore" => {
            // Swallow SIGTERM (up to the lifetime cap); only SIGKILL ends
            // this mode early.
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            let deadline = tokio::time::sleep(MAX_LIFE);
            tokio::pin!(deadline);
            loop {
                tokio::select! {
                    _ = sigterm.recv() => eprintln!("fake ingester: ignoring SIGTERM"),
                    _ = &mut deadline => break,
                }
            }
        }
        // hang_trap and stderr: wait for SIGTERM, then the real ingester's
        // clean-shutdown contract (cancelled row, exit 0).
        _ => {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! {
                _ = sigterm.recv() => {
                    ingest::finish_run(&options, run_id, "cancelled", None, None).await?;
                }
                _ = tokio::time::sleep(MAX_LIFE) => {}
            }
        }
    }
    Ok(())
}
