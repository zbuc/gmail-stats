use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use futures::{stream, TryStreamExt};
use google_gmail1::api::Message;
use google_gmail1::{api::Scope, hyper_rustls, hyper_util, yup_oauth2, Gmail};
use lazy_static::lazy_static;
use regex::Regex;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Connection, Pool, Row, Sqlite, SqliteConnection};
use tokio::sync::{mpsc, Mutex};
use tokio::task;
use tokio::time::{Interval, MissedTickBehavior};

use gmail_stats::ingest::{self, seen_mail, SeenMail, WriteMsg};
use gmail_stats::mbox;

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

const USAGE: &str = "\
gmail_stats - collect per-sender mail counts into a local SQLite database

USAGE:
    gmail_stats [scan] [OPTIONS]           scan Gmail over the API (OAuth; the default)
    gmail_stats import <PATH> [OPTIONS]    import a Google Takeout mbox export

OPTIONS:
    --db <PATH>            SQLite database path
                           [env: GMAIL_STATS_DB] [default: stats.db]
    --credentials <PATH>   OAuth client secret file (scan only)
                           [env: GMAIL_STATS_CREDENTIALS] [default: credentials.json]
    --tokens <PATH>        OAuth token cache file (scan only)
                           [env: GMAIL_STATS_TOKENS] [default: tokencache.json]
    --resume <RUN_ID>      import only: resume a cancelled/failed/abandoned
                           import from its recorded byte offset; falls back to
                           a full re-parse if the file changed (dedupe keeps
                           the counts correct either way)
    --quiet                only errors and the final summary
    --verbose              per-message detail (prints every sender)
    -h, --help             show this help
";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Verbosity {
    Quiet = 0,
    Normal = 1,
    Verbose = 2,
}

/// Process-wide output level, set once at startup. Normal prints periodic
/// progress; Verbose restores the historical per-message `sender: ...` lines
/// (off by default — they spray inbox metadata into the terminal).
static VERBOSITY: AtomicU8 = AtomicU8::new(Verbosity::Normal as u8);

fn set_verbosity(v: Verbosity) {
    VERBOSITY.store(v as u8, Ordering::Relaxed);
}

fn verbose() -> bool {
    VERBOSITY.load(Ordering::Relaxed) >= Verbosity::Verbose as u8
}

/// True unless --quiet: periodic progress and operational notes are printed.
fn chatty() -> bool {
    VERBOSITY.load(Ordering::Relaxed) >= Verbosity::Normal as u8
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Scan,
    Import { path: PathBuf, resume: Option<i64> },
}

#[derive(Debug)]
struct Config {
    db: PathBuf,
    credentials: PathBuf,
    tokens: PathBuf,
    verbosity: Verbosity,
    mode: Mode,
}

#[derive(Debug)]
enum ParsedArgs {
    Run(Config),
    Help,
}

fn parse_args(
    args: impl IntoIterator<Item = String>,
    env: impl Fn(&str) -> Option<String>,
) -> Result<ParsedArgs, String> {
    let mut db = None;
    let mut credentials = None;
    let mut tokens = None;
    let mut quiet = false;
    let mut verbose = false;
    let mut resume = None;
    let mut positionals: Vec<String> = Vec::new();

    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        let (flag, inline) = match arg.split_once('=') {
            Some((f, v)) if f.starts_with("--") => (f.to_string(), Some(v.to_string())),
            _ => (arg.clone(), None),
        };
        let mut take_value = |name: &str| -> Result<String, String> {
            if let Some(v) = inline.clone() {
                return Ok(v);
            }
            it.next().ok_or_else(|| format!("{name} requires a value"))
        };
        match flag.as_str() {
            "--db" => db = Some(take_value("--db")?),
            "--credentials" => credentials = Some(take_value("--credentials")?),
            "--tokens" => tokens = Some(take_value("--tokens")?),
            "--resume" => {
                let v = take_value("--resume")?;
                resume = Some(
                    v.parse::<i64>()
                        .map_err(|_| format!("--resume expects a run id, got {v:?}"))?,
                );
            }
            "--quiet" => quiet = true,
            "--verbose" => verbose = true,
            "-h" | "--help" => return Ok(ParsedArgs::Help),
            _ if flag.starts_with('-') => return Err(format!("unknown option {flag:?}")),
            _ => positionals.push(arg),
        }
    }

    if quiet && verbose {
        return Err("--quiet and --verbose are mutually exclusive".to_string());
    }

    let mode = match positionals.first().map(String::as_str) {
        None | Some("scan") => {
            if positionals.len() > 1 {
                return Err(format!("unexpected argument {:?}", positionals[1]));
            }
            if resume.is_some() {
                return Err("--resume is only valid with the import subcommand".to_string());
            }
            Mode::Scan
        }
        Some("import") => {
            let path = positionals
                .get(1)
                .ok_or_else(|| "import requires the path to an mbox file".to_string())?;
            if positionals.len() > 2 {
                return Err(format!("unexpected argument {:?}", positionals[2]));
            }
            Mode::Import {
                path: PathBuf::from(path),
                resume,
            }
        }
        Some(other) => return Err(format!("unknown subcommand {other:?}")),
    };

    let pick = |flag: Option<String>, env_name: &str, default: &str| -> PathBuf {
        flag.map(PathBuf::from)
            .or_else(|| env(env_name).map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from(default))
    };

    Ok(ParsedArgs::Run(Config {
        db: pick(db, "GMAIL_STATS_DB", "stats.db"),
        credentials: pick(credentials, "GMAIL_STATS_CREDENTIALS", "credentials.json"),
        tokens: pick(tokens, "GMAIL_STATS_TOKENS", "tokencache.json"),
        verbosity: if quiet {
            Verbosity::Quiet
        } else if verbose {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        },
        mode,
    }))
}

lazy_static! {
    static ref EMAIL_RE_1: Regex =
        Regex::new(r"^[^<]*<?([\w\-\.]+@([\w-]+\.)+[\w-]{2,4}).*$").unwrap();
    static ref EMAIL_RE_2: Regex = Regex::new(r"^([\w\-\.]+@([\w-]+\.)+[\w-]{2,4})$").unwrap();
}

fn db_connect_options(db: &Path) -> SqliteConnectOptions {
    SqliteConnectOptions::new()
        .filename(db)
        // Create the database file on first run; migrate() creates the
        // tables, so a fresh install needs no manual setup.
        .create_if_missing(true)
        // WAL mode allows the seen-mail reads (and the read-only web viewer)
        // to proceed concurrently with the single writer task's commits.
        .journal_mode(SqliteJournalMode::Wal)
        // Synchronous mode is OK because a transaction may roll back during a
        // crash, however both ingestion modes are re-runnable and idempotent.
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_secs(5))
}

/// Flip a shared flag on SIGTERM/SIGINT so both ingestion modes can stop
/// fetching/parsing, drain the writer channel, persist resume state, and mark
/// their run row cancelled. A second signal aborts immediately.
fn spawn_signal_handler() -> Arc<AtomicBool> {
    let cancel = Arc::new(AtomicBool::new(false));
    let flag = cancel.clone();
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).expect("installing SIGTERM handler");
        let mut sigint = signal(SignalKind::interrupt()).expect("installing SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        eprintln!(
            "shutting down: draining queued writes and saving resume state \
             (signal again to abort immediately)"
        );
        flag.store(true, Ordering::Relaxed);
        tokio::select! {
            _ = sigterm.recv() => {}
            _ = sigint.recv() => {}
        }
        eprintln!("aborting");
        std::process::exit(130);
    });
    cancel
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = match parse_args(std::env::args().skip(1), |name| std::env::var(name).ok()) {
        Ok(ParsedArgs::Run(cfg)) => cfg,
        Ok(ParsedArgs::Help) => {
            print!("{USAGE}");
            return Ok(());
        }
        Err(msg) => {
            eprintln!("error: {msg}\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    set_verbosity(cfg.verbosity);

    // Mutual exclusion comes first: never touch the database while another
    // ingester (scan or import, terminal- or web-launched) is running. The
    // kernel releases the flock whenever this process dies, so there are no
    // stale locks to clean up.
    let _ingest_lock = ingest::acquire_ingest_lock(&cfg.db)?;

    let options = db_connect_options(&cfg.db);
    // The writer task owns the only connection that ever writes.
    let mut writer_conn = SqliteConnection::connect_with(&options).await?;
    ingest::migrate(&mut writer_conn).await?;
    // We hold the lock, so any still-open run row belongs to a dead ingester.
    let abandoned = ingest::abandon_stale_runs(&mut writer_conn).await?;
    if abandoned > 0 && chatty() {
        println!("marked {abandoned} interrupted ingest run(s) as abandoned");
    }

    let cancel = spawn_signal_handler();

    match &cfg.mode {
        Mode::Scan => run_scan(&cfg, &options, writer_conn, cancel).await,
        Mode::Import { path, resume } => {
            let summary = run_import(&options, writer_conn, path, *resume, cancel).await?;
            report_import_summary(&summary, path);
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Takeout mbox import
// ---------------------------------------------------------------------------

struct ImportOutcome {
    messages_seen: u64,
    skipped: u64,
    cancelled: bool,
}

#[derive(Debug)]
struct ImportSummary {
    run_id: i64,
    messages_seen: u64,
    messages_new: u64,
    skipped: u64,
    cancelled: bool,
}

fn report_import_summary(summary: &ImportSummary, mbox_path: &Path) {
    if summary.cancelled {
        println!(
            "import cancelled cleanly after {} message(s) ({} new); resume with: \
             gmail_stats import {} --resume {}",
            summary.messages_seen,
            summary.messages_new,
            mbox_path.display(),
            summary.run_id
        );
    } else if summary.skipped > 0 {
        println!(
            "import finished: {} message(s) parsed, {} new, \
             {} unparseable message(s) skipped (malformed or missing Message-ID)",
            summary.messages_seen, summary.messages_new, summary.skipped
        );
    } else {
        println!(
            "import finished: {} message(s) parsed, {} new",
            summary.messages_seen, summary.messages_new
        );
    }
}

async fn run_import(
    options: &SqliteConnectOptions,
    mut writer_conn: SqliteConnection,
    mbox_path: &Path,
    resume: Option<i64>,
    cancel: Arc<AtomicBool>,
) -> anyhow::Result<ImportSummary> {
    let meta = std::fs::metadata(mbox_path)
        .with_context(|| format!("cannot read mbox file {}", mbox_path.display()))?;
    anyhow::ensure!(
        meta.is_file(),
        "{} is not a regular file",
        mbox_path.display()
    );
    {
        // Sanity sniff: refuse files that clearly aren't mbox, so a wrong path
        // can't slurp arbitrary file contents into the sender stats.
        let mut file =
            File::open(mbox_path).with_context(|| format!("opening {}", mbox_path.display()))?;
        let mut magic = [0u8; 5];
        let looks_like_mbox = file.read_exact(&mut magic).is_ok() && &magic == b"From ";
        anyhow::ensure!(
            looks_like_mbox,
            "{} does not look like an mbox file (expected it to start with a `From ` separator)",
            mbox_path.display()
        );
    }

    let fingerprint = ingest::mbox_fingerprint(mbox_path)?;
    let start_offset = match resume {
        None => 0,
        Some(run_id) => resolve_resume_offset(&mut writer_conn, run_id, &fingerprint).await?,
    };
    let run_id = ingest::create_run(
        &mut writer_conn,
        "mbox",
        Some(ingest::MboxRunInfo {
            path: mbox_path,
            fingerprint: &fingerprint,
            bytes_total: meta.len(),
            start_offset,
        }),
    )
    .await?;
    if chatty() {
        if start_offset > 0 {
            println!(
                "importing {} from byte offset {start_offset} (run {run_id})",
                mbox_path.display()
            );
        } else {
            println!(
                "importing {} ({:.1} MiB, run {run_id})",
                mbox_path.display(),
                meta.len() as f64 / (1024.0 * 1024.0)
            );
        }
    }

    let (write_tx, write_rx) = mpsc::channel::<WriteMsg>(1024);
    let writer_handle = task::spawn(ingest::db_writer(writer_conn, write_rx, run_id));
    let path = mbox_path.to_path_buf();
    let bytes_total = meta.len();
    let cancel_flag = cancel.clone();
    let producer = task::spawn_blocking(move || {
        parse_and_send(&path, start_offset, bytes_total, write_tx, &cancel_flag)
    });

    let outcome = producer
        .await
        .map_err(|e| anyhow::anyhow!("import worker panicked: {e:?}"))?;
    let writer_result = writer_handle
        .await
        .map_err(|e| anyhow::anyhow!("DB writer task panicked: {e:?}"))?;

    let outcome = match (outcome, writer_result) {
        (Ok(outcome), Ok(())) => outcome,
        (_, Err(writer_err)) => {
            let msg = format!("{writer_err:#}");
            ingest::finish_run(options, run_id, "failed", Some("db"), Some(&msg))
                .await
                .ok();
            return Err(writer_err.context("DB writer task failed"));
        }
        (Err(e), Ok(())) => {
            let msg = format!("{e:#}");
            ingest::finish_run(options, run_id, "failed", Some("io"), Some(&msg))
                .await
                .ok();
            return Err(e);
        }
    };

    let state = if outcome.cancelled {
        "cancelled"
    } else {
        "done"
    };
    ingest::finish_run(options, run_id, state, None, None).await?;

    let mut conn = SqliteConnection::connect_with(options).await?;
    let messages_new: i64 = sqlx::query("SELECT messages_new FROM ingest_runs WHERE run_id = ?")
        .bind(run_id)
        .fetch_one(&mut conn)
        .await?
        .try_get("messages_new")?;
    conn.close().await.ok();

    Ok(ImportSummary {
        run_id,
        messages_seen: outcome.messages_seen,
        messages_new: messages_new as u64,
        skipped: outcome.skipped,
        cancelled: outcome.cancelled,
    })
}

/// Decide where to start `--resume <run_id>`: at the recorded byte offset if
/// the file still matches the run's fingerprint, else from the beginning
/// (which stays correct — every message id is deduped in seen_mails).
async fn resolve_resume_offset(
    conn: &mut SqliteConnection,
    run_id: i64,
    fingerprint: &str,
) -> anyhow::Result<u64> {
    let row = sqlx::query(
        "SELECT source, mbox_fingerprint, bytes_done FROM ingest_runs WHERE run_id = ?",
    )
    .bind(run_id)
    .fetch_optional(&mut *conn)
    .await?;
    let Some(row) = row else {
        anyhow::bail!("no ingest run {run_id} to resume");
    };
    let source: String = row.try_get("source")?;
    anyhow::ensure!(
        source == "mbox",
        "run {run_id} is a {source} run, not an mbox import"
    );
    let recorded: Option<String> = row.try_get("mbox_fingerprint")?;
    let bytes_done: Option<i64> = row.try_get("bytes_done")?;
    match (recorded, bytes_done) {
        (Some(recorded), Some(offset)) if recorded == fingerprint && offset > 0 => {
            if chatty() {
                println!("resuming run {run_id} from byte offset {offset}");
            }
            Ok(offset as u64)
        }
        _ => {
            if chatty() {
                println!(
                    "mbox file changed since run {run_id} (or it recorded no progress); \
                     re-importing from the start - dedupe keeps the counts correct"
                );
            }
            Ok(0)
        }
    }
}

/// The blocking import producer: streams the mbox with the header-only parser
/// and feeds the writer channel. Memory stays bounded no matter the file size
/// (one BufReader block plus one capped line buffer). Runs on a blocking
/// thread; checks the cancel flag between messages.
fn parse_and_send(
    path: &Path,
    start_offset: u64,
    bytes_total: u64,
    write_tx: mpsc::Sender<WriteMsg>,
    cancel: &AtomicBool,
) -> anyhow::Result<ImportOutcome> {
    let mut file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    if start_offset > 0 {
        file.seek(SeekFrom::Start(start_offset)).with_context(|| {
            format!(
                "seeking to resume offset {start_offset} in {}",
                path.display()
            )
        })?;
    }
    let mut reader = mbox::MboxReader::new(BufReader::with_capacity(64 * 1024, file), start_offset);

    let mut messages_seen = 0u64;
    let mut missing_id = 0u64;
    let mut committed_offset = start_offset;
    let mut cancelled = false;
    let mut last_progress = Instant::now();

    loop {
        if cancel.load(Ordering::Relaxed) {
            cancelled = true;
            break;
        }
        let msg = reader
            .next_message()
            .with_context(|| format!("reading {}", path.display()))?;
        let Some(msg) = msg else { break };
        messages_seen += 1;
        committed_offset = msg.end_offset;
        match msg.message_id {
            None => {
                missing_id += 1;
                if verbose() {
                    println!(
                        "skipping message without Message-ID (ends at byte {})",
                        msg.end_offset
                    );
                }
            }
            Some(mid) => {
                // Same sender priority (From, then Return-Path) and cleanup as
                // the API scan, so counts are consistent across sources.
                let sender =
                    cleanup_sender(pick_sender(msg.from.as_deref(), msg.return_path.as_deref()));
                if verbose() {
                    println!("sender: {:?}", sender);
                }
                // RFC Message-IDs live in the `mid:` namespace of seen_mails so
                // they can never collide with Gmail API ids, and re-imports
                // stay idempotent. This namespacing is the permanent keyspace
                // rule for cross-source dedupe (issue #26).
                write_tx
                    .blocking_send(WriteMsg::Seen(SeenMail {
                        message_id: format!("mid:{mid}"),
                        sender,
                    }))
                    .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;
            }
        }
        if last_progress.elapsed() >= Duration::from_secs(2) {
            last_progress = Instant::now();
            // Sent after the Seen messages it covers (the channel is FIFO), so
            // the recorded bytes_done only ever points past committed work.
            write_tx
                .blocking_send(WriteMsg::Progress {
                    messages_seen,
                    bytes_done: Some(committed_offset),
                    resume_token: None,
                    total_estimate: None,
                })
                .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;
            if chatty() {
                let pct = (committed_offset.min(bytes_total) * 100)
                    .checked_div(bytes_total)
                    .unwrap_or(100);
                println!(
                    "imported {messages_seen} message(s) ({pct}% of {:.1} MiB)",
                    bytes_total as f64 / (1024.0 * 1024.0)
                );
            }
        }
    }

    // Final progress (best-effort: the writer may already be gone on error
    // paths, and the run row is finalized by the caller regardless).
    let _ = write_tx.blocking_send(WriteMsg::Progress {
        messages_seen,
        bytes_done: Some(committed_offset),
        resume_token: None,
        total_estimate: None,
    });

    Ok(ImportOutcome {
        messages_seen,
        skipped: reader.skipped() + missing_id,
        cancelled,
    })
}

// ---------------------------------------------------------------------------
// Gmail API scan
// ---------------------------------------------------------------------------

async fn run_scan(
    cfg: &Config,
    options: &SqliteConnectOptions,
    mut writer_conn: SqliteConnection,
    cancel: Arc<AtomicBool>,
) -> anyhow::Result<()> {
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

    // Small pool used only for the read-side seen-mail checks; all writes go
    // through the single writer task, which eliminates writer-vs-writer
    // deadlocks entirely.
    let pool = SqlitePoolOptions::new()
        .max_connections(fetch_concurrency as u32)
        .connect_with(options.clone())
        .await?;

    let run_id = ingest::create_run(&mut writer_conn, "gmail_api", None).await?;
    let (mut write_tx, write_rx) = mpsc::channel::<WriteMsg>(100);
    let mut writer_handle = task::spawn(ingest::db_writer(writer_conn, write_rx, run_id));

    // Simple client-side rate limiter: each API call waits for the next tick.
    let mut interval = tokio::time::interval(Duration::from_millis(rate_limit_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let rate_limiter = Arc::new(Mutex::new(interval));

    // Read application OAuth secret from a file.
    let secret = match yup_oauth2::read_application_secret(&cfg.credentials).await {
        Ok(secret) => secret,
        Err(e) => {
            drop(write_tx);
            let _ = writer_handle.await;
            let msg = format!(
                "reading OAuth client secret from {}: {e}",
                cfg.credentials.display()
            );
            ingest::finish_run(
                options,
                run_id,
                "failed",
                Some("missing_credentials"),
                Some(&msg),
            )
            .await
            .ok();
            return Err(anyhow::Error::new(e).context(format!(
                "reading OAuth client secret from {} (see the README for OAuth setup, or \
                 use `gmail_stats import` with a Google Takeout mbox export instead)",
                cfg.credentials.display()
            )));
        }
    };

    // Create an authenticator that uses an InstalledFlow to authenticate. The
    // authentication tokens are persisted to the token cache file, so they are
    // cached to disk and refreshed once they've expired.
    let auth = match yup_oauth2::InstalledFlowAuthenticator::builder(
        secret,
        yup_oauth2::InstalledFlowReturnMethod::HTTPRedirect,
    )
    .persist_tokens_to_disk(cfg.tokens.clone())
    .build()
    .await
    {
        Ok(auth) => auth,
        Err(e) => {
            drop(write_tx);
            let _ = writer_handle.await;
            ingest::finish_run(
                options,
                run_id,
                "failed",
                Some("auth_required"),
                Some(&e.to_string()),
            )
            .await
            .ok();
            return Err(anyhow::Error::new(e).context("building the OAuth authenticator"));
        }
    };

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

    // Best-effort total for progress display; ignored on failure (the scan
    // itself will surface any real auth/API problem).
    if let Ok((_, profile)) = hub
        .users()
        .get_profile("me")
        .add_scope(Scope::Readonly)
        .doit()
        .await
    {
        if let Some(total) = profile.messages_total {
            let _ = write_tx
                .send(WriteMsg::Progress {
                    messages_seen: 0,
                    bytes_done: None,
                    resume_token: None,
                    total_estimate: Some(i64::from(total)),
                })
                .await;
        }
    }

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
    let mut progress = ScanProgress {
        resume_token: None,
        pages_done: 0,
        messages_listed: 0,
    };
    loop {
        let pages_before = progress.pages_done;
        let attempt = work(
            &ScanCtx {
                pool: &pool,
                hub: &hub,
                write_tx: &write_tx,
                rate_limiter: &rate_limiter,
                fetch_concurrency,
                cancel: cancel.as_ref(),
            },
            &mut progress,
        )
        .await;
        match attempt {
            Ok(()) => break,
            Err(e) => {
                if cancel.load(Ordering::Relaxed) {
                    // Shutdown was requested; the error is almost certainly
                    // fallout from stopping mid-flight. Save state and leave.
                    if chatty() {
                        println!("stopping scan for shutdown ({e:#})");
                    }
                    break;
                }
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
                if progress.pages_done > pages_before {
                    retries = 0;
                }
                retries += 1;

                if !transient || retries > MAX_RETRIES {
                    let final_err = if transient {
                        e.context(format!("giving up after {MAX_RETRIES} retries"))
                    } else {
                        e.context("giving up: error is not retryable")
                    };
                    let final_err = match writer_err {
                        None => final_err,
                        Some(writer_err) => {
                            final_err.context(format!("DB writer task failed: {writer_err:?}"))
                        }
                    };
                    ingest::finish_run(
                        options,
                        run_id,
                        "failed",
                        Some(classify_scan_error(&final_err)),
                        Some(&format!("{final_err:#}")),
                    )
                    .await
                    .ok();
                    return Err(final_err);
                }

                if let Some(writer_err) = &writer_err {
                    println!("DB writer task failed, restarting it: {writer_err:?}");
                }
                // Spawn a fresh writer so the retry has a chance of working.
                let writer_conn = SqliteConnection::connect_with(options).await?;
                let (tx, rx) = mpsc::channel::<WriteMsg>(100);
                write_tx = tx;
                writer_handle = task::spawn(ingest::db_writer(writer_conn, rx, run_id));

                // Back off before retrying: errors that reach this loop are
                // typically sustained conditions (e.g. quota exhaustion that
                // outlasted the per-call retries), so hammering the API again
                // immediately would just burn through the retry budget in
                // seconds.
                let delay = backoff_delay(retries);
                println!(
                    "Transient error (attempt {retries}/{MAX_RETRIES}), retrying in {delay:?}: {e:?}"
                );
                sleep_cancellable(delay, &cancel).await;
            }
        }
    }

    // Close the channel so the writer task drains its queue and exits, then
    // surface any error it hit.
    drop(write_tx);
    match writer_handle.await {
        Err(join_err) => {
            let msg = format!("DB writer task panicked: {join_err:?}");
            ingest::finish_run(options, run_id, "failed", Some("db"), Some(&msg))
                .await
                .ok();
            anyhow::bail!("{msg}");
        }
        Ok(Err(writer_err)) => {
            ingest::finish_run(
                options,
                run_id,
                "failed",
                Some("db"),
                Some(&format!("{writer_err:#}")),
            )
            .await
            .ok();
            return Err(writer_err.context("DB writer task failed"));
        }
        Ok(Ok(())) => {}
    }

    if cancel.load(Ordering::Relaxed) {
        ingest::finish_run(options, run_id, "cancelled", None, None).await?;
        println!("scan cancelled cleanly; run {run_id} keeps the resume state");
    } else {
        ingest::finish_run(options, run_id, "done", None, None).await?;
        let row =
            sqlx::query("SELECT messages_seen, messages_new FROM ingest_runs WHERE run_id = ?")
                .bind(run_id)
                .fetch_one(&pool)
                .await?;
        println!(
            "scan finished: {} message(s) listed, {} new",
            row.try_get::<i64, _>("messages_seen")?,
            row.try_get::<i64, _>("messages_new")?
        );
    }

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

/// Sleep that wakes early (within ~250ms) once the cancel flag is set.
async fn sleep_cancellable(duration: Duration, cancel: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while !cancel.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        tokio::time::sleep((deadline - now).min(Duration::from_millis(250))).await;
    }
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

struct ScanCtx<'a> {
    pool: &'a Pool<Sqlite>,
    hub: &'a GmailHub,
    write_tx: &'a mpsc::Sender<WriteMsg>,
    rate_limiter: &'a Arc<Mutex<Interval>>,
    fetch_concurrency: usize,
    cancel: &'a AtomicBool,
}

/// Progress that survives across retry attempts. resume_token is the page
/// token to resume from, advanced only once a page has been fully processed:
/// a retry re-lists the page that failed instead of restarting the whole
/// mailbox scan from page one. It is also persisted to the run row after
/// every page, so a future scan could pick it up best-effort.
struct ScanProgress {
    resume_token: Option<String>,
    pages_done: u64,
    messages_listed: u64,
}

async fn work(ctx: &ScanCtx<'_>, progress: &mut ScanProgress) -> anyhow::Result<()> {
    // Fetch 500 messages at a time, starting from the last page we fully
    // processed.
    loop {
        if ctx.cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let result =
            list_messages(ctx.hub, progress.resume_token.as_deref(), ctx.rate_limiter).await?;

        let next_page_token = result.next_page_token;
        let messages = result.messages.unwrap_or_default();
        let listed = messages.len() as u64;
        parse_messages(ctx, messages).await?;

        if ctx.cancel.load(Ordering::Relaxed) {
            // Fetches for this page were (partially) skipped for shutdown;
            // don't advance the resume point past them.
            return Ok(());
        }

        // Completing a page is forward progress: only now advance the resume
        // point and the page counter the caller uses to reset its retry
        // budget, so a long scan isn't aborted by transient errors
        // accumulated across otherwise-successful attempts.
        progress.pages_done += 1;
        progress.messages_listed += listed;
        progress.resume_token = next_page_token.clone();
        ctx.write_tx
            .send(WriteMsg::Progress {
                messages_seen: progress.messages_listed,
                bytes_done: None,
                resume_token: next_page_token,
                total_estimate: None,
            })
            .await
            .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;
        if chatty() {
            println!(
                "page {} done ({} messages listed)",
                progress.pages_done, progress.messages_listed
            );
        }
        if progress.resume_token.is_none() {
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
                if chatty() {
                    println!(
                        "transient error listing messages (attempt {}), backing off for {:?}: {}",
                        attempts, backoff, e
                    );
                }
                tokio::time::sleep(backoff).await;
                backoff *= 2;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn parse_messages(ctx: &ScanCtx<'_>, messages: Vec<Message>) -> anyhow::Result<()> {
    // Fetch each individual message concurrently (bounded), then hand the
    // result to the writer task to increment the counter for the sender.
    stream::iter(messages.into_iter().map(Ok::<_, anyhow::Error>))
        .try_for_each_concurrent(ctx.fetch_concurrency, |message_meta| {
            let hub = ctx.hub.clone();
            let write_tx = ctx.write_tx.clone();
            let rate_limiter = ctx.rate_limiter.clone();
            async move {
                if ctx.cancel.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let message_id = message_meta.id.expect("message missing id");
                if seen_mail(&message_id, ctx.pool).await? {
                    return Ok(());
                }

                let message = fetch_message(&hub, &message_id, &rate_limiter).await?;
                let sender = cleanup_sender(get_sender(&message)?);
                if verbose() {
                    println!("sender: {:?}", sender);
                }

                write_tx
                    .send(WriteMsg::Seen(SeenMail { message_id, sender }))
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
                if chatty() {
                    println!(
                        "transient error fetching {} (attempt {}), backing off for {:?}: {}",
                        message_id, attempts, backoff, e
                    );
                }
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

/// Coarse error classification recorded on failed scan runs so a watcher (or
/// the future web UI) can key remediation off it.
fn classify_scan_error(err: &anyhow::Error) -> &'static str {
    if format!("{err:#}").contains("policy_enforced") {
        return "policy_enforced";
    }
    for cause in err.chain() {
        if let Some(io_err) = cause.downcast_ref::<std::io::Error>() {
            if io_err.kind() == std::io::ErrorKind::NotFound {
                return "missing_credentials";
            }
            return "io";
        }
        if cause.downcast_ref::<sqlx::Error>().is_some() {
            return "db";
        }
        if let Some(gmail_err) = cause.downcast_ref::<google_gmail1::Error>() {
            return if is_transient_gmail(gmail_err) {
                "rate_limited"
            } else {
                "gmail_api"
            };
        }
    }
    "other"
}

// ---------------------------------------------------------------------------
// Sender extraction, shared by both ingestion modes
// ---------------------------------------------------------------------------

/// The shared sender priority: From first, then Return-Path. Used by both the
/// API scan (get_sender) and the mbox importer so counts are consistent.
fn pick_sender(from: Option<&str>, return_path: Option<&str>) -> String {
    from.or(return_path).unwrap_or_default().to_string()
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
    // Header names are case-insensitive (RFC 5322); check candidates in
    // priority order via the shared pick_sender.
    let headers = message
        .payload
        .as_ref()
        .and_then(|p| p.headers.as_deref())
        .unwrap_or(&[]);
    let find = |name: &str| {
        headers.iter().find_map(|header| {
            header
                .name
                .as_deref()
                .filter(|n| n.eq_ignore_ascii_case(name))
                .and(header.value.as_deref())
        })
    };

    let sender = pick_sender(find("from"), find("return-path"));
    if sender.is_empty() && verbose() {
        println!("weird email without from header: {:?}", message.id);
    }
    Ok(sender)
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

    // --- pick_sender / get_sender ---

    #[test]
    fn pick_sender_prefers_from_over_return_path() {
        assert_eq!(
            pick_sender(Some("a@example.com"), Some("b@example.com")),
            "a@example.com"
        );
        assert_eq!(pick_sender(None, Some("b@example.com")), "b@example.com");
        assert_eq!(pick_sender(None, None), "");
    }

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

    // --- argument parsing ---

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn run_cfg(args: &[&str], env: impl Fn(&str) -> Option<String>) -> Config {
        match parse_args(args.iter().map(|s| s.to_string()), env) {
            Ok(ParsedArgs::Run(cfg)) => cfg,
            other => panic!("expected a runnable config, got {:?}", other.err()),
        }
    }

    #[test]
    fn bare_invocation_is_the_scan_with_default_paths() {
        let cfg = run_cfg(&[], no_env);
        assert_eq!(cfg.mode, Mode::Scan);
        assert_eq!(cfg.db, PathBuf::from("stats.db"));
        assert_eq!(cfg.credentials, PathBuf::from("credentials.json"));
        assert_eq!(cfg.tokens, PathBuf::from("tokencache.json"));
        assert_eq!(cfg.verbosity, Verbosity::Normal);
    }

    #[test]
    fn explicit_scan_subcommand_is_equivalent() {
        assert_eq!(run_cfg(&["scan"], no_env).mode, Mode::Scan);
    }

    #[test]
    fn import_subcommand_takes_a_path_and_resume() {
        let cfg = run_cfg(&["import", "/tmp/All mail.mbox", "--resume", "7"], no_env);
        assert_eq!(
            cfg.mode,
            Mode::Import {
                path: PathBuf::from("/tmp/All mail.mbox"),
                resume: Some(7)
            }
        );
    }

    #[test]
    fn path_flags_override_env_which_overrides_defaults() {
        let env = |name: &str| match name {
            "GMAIL_STATS_DB" => Some("/env/stats.db".to_string()),
            "GMAIL_STATS_CREDENTIALS" => Some("/env/creds.json".to_string()),
            _ => None,
        };
        let cfg = run_cfg(&["--db", "/flag/stats.db"], env);
        assert_eq!(cfg.db, PathBuf::from("/flag/stats.db"));
        assert_eq!(cfg.credentials, PathBuf::from("/env/creds.json"));
        assert_eq!(cfg.tokens, PathBuf::from("tokencache.json"));
    }

    #[test]
    fn inline_flag_values_are_accepted() {
        let cfg = run_cfg(&["--db=inline.db", "--quiet"], no_env);
        assert_eq!(cfg.db, PathBuf::from("inline.db"));
        assert_eq!(cfg.verbosity, Verbosity::Quiet);
    }

    #[test]
    fn verbose_flag_selects_verbose_output() {
        assert_eq!(
            run_cfg(&["--verbose"], no_env).verbosity,
            Verbosity::Verbose
        );
    }

    #[test]
    fn invalid_invocations_are_rejected() {
        for args in [
            &["--quiet", "--verbose"][..],
            &["import"][..],
            &["--resume", "3"][..],
            &["--resume", "x", "import", "f.mbox"][..],
            &["frobnicate"][..],
            &["--frobnicate"][..],
            &["scan", "extra"][..],
            &["import", "a.mbox", "extra"][..],
            &["--db"][..],
        ] {
            let result = parse_args(args.iter().map(|s| s.to_string()), no_env);
            assert!(result.is_err(), "expected {args:?} to be rejected");
        }
    }

    #[test]
    fn help_flag_wins() {
        assert!(matches!(
            parse_args(["--help".to_string()], no_env),
            Ok(ParsedArgs::Help)
        ));
        assert!(matches!(
            parse_args(["import".to_string(), "-h".to_string()], no_env),
            Ok(ParsedArgs::Help)
        ));
    }

    // --- end-to-end import ---

    /// Three messages: normal, Return-Path-only with an mboxrd-escaped body
    /// line, and one without a Message-ID (skipped).
    const IMPORT_FIXTURE: &str = "From a@example.com Thu Jan  1 00:00:00 2020\n\
        From: Alice <a@example.com>\n\
        Message-ID: <m1@example.com>\n\
        Subject: one\n\
        \n\
        body one\n\
        >From an escaped body line\n\
        From b@example.com Thu Jan  1 00:00:00 2020\n\
        Return-Path: <b@example.com>\n\
        Message-ID: <m2@example.com>\n\
        \n\
        body two\n\
        From c@example.com Thu Jan  1 00:00:00 2020\n\
        From: Alice <a@example.com>\n\
        Subject: no message id\n\
        \n\
        body three\n";

    async fn migrated_conn(options: &SqliteConnectOptions) -> SqliteConnection {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        ingest::migrate(&mut conn).await.unwrap();
        conn
    }

    async fn sender_count(options: &SqliteConnectOptions, sender: &str) -> Option<i64> {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        sqlx::query("SELECT mails_sent FROM senders WHERE sender = ?")
            .bind(sender)
            .fetch_optional(&mut conn)
            .await
            .unwrap()
            .map(|row| row.try_get("mails_sent").unwrap())
    }

    fn unset_cancel() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    #[tokio::test]
    async fn import_populates_the_db_and_reimport_adds_zero_counts() {
        let dir = tempfile::tempdir().unwrap();
        let mbox = dir.path().join("mail.mbox");
        std::fs::write(&mbox, IMPORT_FIXTURE).unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));

        let conn = migrated_conn(&options).await;
        let s1 = run_import(&options, conn, &mbox, None, unset_cancel())
            .await
            .unwrap();
        assert_eq!(s1.messages_seen, 3);
        assert_eq!(s1.messages_new, 2);
        assert_eq!(s1.skipped, 1);
        assert!(!s1.cancelled);
        assert_eq!(sender_count(&options, "a@example.com").await, Some(1));
        assert_eq!(sender_count(&options, "b@example.com").await, Some(1));

        // Message-IDs are namespaced so they can never collide with Gmail ids.
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let namespaced: i64 =
            sqlx::query("SELECT count(1) AS ct FROM seen_mails WHERE mail_id LIKE 'mid:<%'")
                .fetch_one(&mut conn)
                .await
                .unwrap()
                .try_get("ct")
                .unwrap();
        assert_eq!(namespaced, 2);

        // Re-importing the same mbox is a no-op for the counts.
        let conn = migrated_conn(&options).await;
        let s2 = run_import(&options, conn, &mbox, None, unset_cancel())
            .await
            .unwrap();
        assert_eq!(s2.messages_seen, 3);
        assert_eq!(s2.messages_new, 0, "re-import must add zero new counts");
        assert_eq!(sender_count(&options, "a@example.com").await, Some(1));
        assert_eq!(sender_count(&options, "b@example.com").await, Some(1));

        // Both runs were recorded as done, with progress totals.
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let done: i64 = sqlx::query(
            "SELECT count(1) AS ct FROM ingest_runs \
             WHERE source='mbox' AND state='done' AND bytes_done = bytes_total",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap()
        .try_get("ct")
        .unwrap();
        assert_eq!(done, 2);
    }

    /// Insert a half-finished import run row the way a cancelled run leaves it.
    async fn plant_resumable_run(
        conn: &mut SqliteConnection,
        mbox: &Path,
        fingerprint: &str,
        bytes_done: u64,
    ) -> i64 {
        sqlx::query(
            "INSERT INTO ingest_runs \
             (source, state, pid, started_at_unix, updated_at_unix, \
              bytes_total, bytes_done, mbox_path, mbox_fingerprint) \
             VALUES ('mbox', 'cancelled', 0, 0, 0, ?, ?, ?, ?)",
        )
        .bind(IMPORT_FIXTURE.len() as i64)
        .bind(bytes_done as i64)
        .bind(mbox.display().to_string())
        .bind(fingerprint)
        .execute(conn)
        .await
        .unwrap()
        .last_insert_rowid()
    }

    fn first_message_end_offset() -> u64 {
        let mut reader = mbox::MboxReader::new(std::io::Cursor::new(IMPORT_FIXTURE.as_bytes()), 0);
        reader.next_message().unwrap().unwrap().end_offset
    }

    #[tokio::test]
    async fn resume_with_matching_fingerprint_continues_from_the_offset() {
        let dir = tempfile::tempdir().unwrap();
        let mbox = dir.path().join("mail.mbox");
        std::fs::write(&mbox, IMPORT_FIXTURE).unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));

        let mut conn = migrated_conn(&options).await;
        let fingerprint = ingest::mbox_fingerprint(&mbox).unwrap();
        let offset = first_message_end_offset();
        let run_id = plant_resumable_run(&mut conn, &mbox, &fingerprint, offset).await;

        let summary = run_import(&options, conn, &mbox, Some(run_id), unset_cancel())
            .await
            .unwrap();
        // Only the two messages after the offset are parsed; Alice's message
        // was before the resume point and is never touched.
        assert_eq!(summary.messages_seen, 2);
        assert_eq!(summary.messages_new, 1);
        assert_eq!(summary.skipped, 1);
        assert_eq!(sender_count(&options, "a@example.com").await, None);
        assert_eq!(sender_count(&options, "b@example.com").await, Some(1));
    }

    #[tokio::test]
    async fn resume_with_stale_fingerprint_falls_back_to_a_full_parse() {
        let dir = tempfile::tempdir().unwrap();
        let mbox = dir.path().join("mail.mbox");
        std::fs::write(&mbox, IMPORT_FIXTURE).unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));

        let mut conn = migrated_conn(&options).await;
        let offset = first_message_end_offset();
        let run_id = plant_resumable_run(&mut conn, &mbox, "stale:fingerprint:0000", offset).await;

        let summary = run_import(&options, conn, &mbox, Some(run_id), unset_cancel())
            .await
            .unwrap();
        assert_eq!(summary.messages_seen, 3, "must re-parse from the start");
        assert_eq!(summary.messages_new, 2);
        assert_eq!(sender_count(&options, "a@example.com").await, Some(1));
        assert_eq!(sender_count(&options, "b@example.com").await, Some(1));
    }

    #[tokio::test]
    async fn resuming_a_scan_run_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mbox = dir.path().join("mail.mbox");
        std::fs::write(&mbox, IMPORT_FIXTURE).unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));

        let mut conn = migrated_conn(&options).await;
        let run_id = ingest::create_run(&mut conn, "gmail_api", None)
            .await
            .unwrap();
        let err = run_import(&options, conn, &mbox, Some(run_id), unset_cancel())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not an mbox import"));
    }

    #[tokio::test]
    async fn cancelled_import_marks_the_run_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let mbox = dir.path().join("mail.mbox");
        std::fs::write(&mbox, IMPORT_FIXTURE).unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));

        let conn = migrated_conn(&options).await;
        let cancel = Arc::new(AtomicBool::new(true)); // cancel before the first message
        let summary = run_import(&options, conn, &mbox, None, cancel)
            .await
            .unwrap();
        assert!(summary.cancelled);
        assert_eq!(summary.messages_seen, 0);

        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let state: String = sqlx::query("SELECT state FROM ingest_runs WHERE run_id = ?")
            .bind(summary.run_id)
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .try_get("state")
            .unwrap();
        assert_eq!(state, "cancelled");
    }

    #[tokio::test]
    async fn import_refuses_a_non_mbox_file() {
        let dir = tempfile::tempdir().unwrap();
        let bogus = dir.path().join("notes.txt");
        std::fs::write(&bogus, "definitely not mail\n").unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));

        let conn = migrated_conn(&options).await;
        let err = run_import(&options, conn, &bogus, None, unset_cancel())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("does not look like an mbox file"));
    }
}
