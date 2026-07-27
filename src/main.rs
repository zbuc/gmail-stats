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
    gmail_stats scan --backfill [OPTIONS]  repair pass: fetch the Message-ID and
                                           List-Unsubscribe headers for previously scanned
                                           messages, collapse cross-source duplicate counts,
                                           and capture unsubscribe targets (idempotent;
                                           re-run any time to continue an interrupted pass;
                                           --backfill-message-ids is an accepted alias)

OPTIONS:
    --db <PATH>            SQLite database path
                           [env: GMAIL_STATS_DB] [default: stats.db]
    --credentials <PATH>   OAuth client secret file (scan only)
                           [env: GMAIL_STATS_CREDENTIALS] [default: credentials.json]
    --tokens <PATH>        OAuth token cache file (scan only)
                           [env: GMAIL_STATS_TOKENS] [default: tokencache.json]
    --oauth-port <PORT>    fixed localhost port for the OAuth consent redirect;
                           http://localhost:<PORT> must be an authorized
                           redirect URI on the OAuth client (scan only)
                           [env: GMAIL_STATS_OAUTH_PORT] [default: 8085]
    --resume <RUN_ID>      resume an earlier run of the same kind.
                           import: continue from the recorded byte offset,
                           falling back to a full re-parse if the file changed;
                           scan: start listing from the run's persisted page
                           token, silently restarting from the beginning when
                           the token has expired (seen-mail dedupe keeps the
                           counts correct either way)
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
    Scan {
        resume: Option<i64>,
    },
    Import {
        path: PathBuf,
        resume: Option<i64>,
    },
    /// `scan --backfill` (alias `--backfill-message-ids`): an ingest run that
    /// repairs historical scan rows — fetch Message-ID and List-Unsubscribe
    /// headers, collapse cross-source duplicate counts, and capture
    /// per-sender unsubscribe targets. Naturally resumable: re-running
    /// continues with whatever rows are still unrepaired, so it takes no
    /// --resume run id.
    BackfillMessageIds,
}

#[derive(Debug)]
struct Config {
    db: PathBuf,
    credentials: PathBuf,
    tokens: PathBuf,
    oauth_port: u16,
    verbosity: Verbosity,
    mode: Mode,
}

/// Default localhost port for the OAuth consent redirect. A fixed port keeps
/// the redirect URI stable so it can be preconfigured on the OAuth client;
/// the ephemeral-port flow breaks clients that require exact redirect URIs.
const DEFAULT_OAUTH_PORT: u16 = 8085;

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
    let mut oauth_port = None;
    let mut quiet = false;
    let mut verbose = false;
    let mut resume = None;
    let mut backfill = false;
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
            "--oauth-port" => oauth_port = Some(take_value("--oauth-port")?),
            "--resume" => {
                let v = take_value("--resume")?;
                resume = Some(
                    v.parse::<i64>()
                        .map_err(|_| format!("--resume expects a run id, got {v:?}"))?,
                );
            }
            // --backfill is the current spelling; --backfill-message-ids is
            // the original (message-id-only) flag, kept as an alias.
            "--backfill" | "--backfill-message-ids" => backfill = true,
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
            if backfill {
                if resume.is_some() {
                    return Err("--backfill takes no --resume: re-running it \
                         automatically continues with the still-unrepaired rows"
                        .to_string());
                }
                Mode::BackfillMessageIds
            } else {
                Mode::Scan { resume }
            }
        }
        Some("import") => {
            if backfill {
                return Err("--backfill only applies to scan".to_string());
            }
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

    let oauth_port = match oauth_port.or_else(|| env("GMAIL_STATS_OAUTH_PORT")) {
        None => DEFAULT_OAUTH_PORT,
        Some(v) => v.parse::<u16>().ok().filter(|p| *p != 0).ok_or_else(|| {
            format!("--oauth-port / GMAIL_STATS_OAUTH_PORT expects 1-65535, got {v:?}")
        })?,
    };

    Ok(ParsedArgs::Run(Config {
        db: pick(db, "GMAIL_STATS_DB", "stats.db"),
        credentials: pick(credentials, "GMAIL_STATS_CREDENTIALS", "credentials.json"),
        tokens: pick(tokens, "GMAIL_STATS_TOKENS", "tokencache.json"),
        oauth_port,
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
        Mode::Scan { resume } => run_scan(&cfg, &options, writer_conn, cancel, *resume).await,
        Mode::Import { path, resume } => {
            let summary = run_import(&options, writer_conn, path, *resume, cancel).await?;
            report_import_summary(&summary, path);
            Ok(())
        }
        Mode::BackfillMessageIds => run_backfill(&cfg, &options, writer_conn, cancel).await,
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
        // Same sender priority (From, then Return-Path) and cleanup as the
        // API scan, so counts are consistent across sources.
        let sender = cleanup_sender(pick_sender(msg.from.as_deref(), msg.return_path.as_deref()));
        // List-Unsubscribe capture (issue #41) applies to *every* parsed
        // message — including ones the writer skips as seen-duplicates and
        // ones without a Message-ID — so a re-import of an already-ingested
        // mbox back-populates the table with zero count changes. Latest
        // message wins in the writer's upsert; malformed values yield None
        // and leave any earlier capture alone.
        if let Some(capture) = ingest::parse_unsubscribe_headers(
            msg.list_unsubscribe.as_deref(),
            msg.list_unsubscribe_post.as_deref(),
        ) {
            write_tx
                .blocking_send(WriteMsg::Unsubscribe {
                    sender: sender.clone(),
                    capture,
                    // mbox messages carry no Gmail internalDate; NULL marks
                    // the capture "not comparably dated" so the backfill
                    // repair pass never overwrites it.
                    internal_date: None,
                })
                .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;
        }
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
                if verbose() {
                    println!("sender: {:?}", sender);
                }
                // RFC Message-IDs live in the `mid:` namespace of seen_mails so
                // they can never collide with Gmail API ids, and re-imports
                // stay idempotent. This namespacing is the permanent keyspace
                // rule for cross-source dedupe (issue #26); the normalized id
                // additionally lands in rfc_message_id, where the writer's
                // check-before-increment dedupes against scanned rows.
                write_tx
                    .blocking_send(WriteMsg::Seen(SeenMail {
                        rfc_message_id: ingest::normalize_rfc_message_id(&mid),
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
    resume: Option<i64>,
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

    // Best-effort resume (issue #26 Q4): start listing from the page token the
    // resumed run persisted. If Gmail rejects the token as expired, work()
    // silently restarts from page one — seen-mail dedupe keeps that correct.
    let resumed_token = match resume {
        None => None,
        Some(run_id) => resolve_scan_resume(&mut writer_conn, run_id).await?,
    };

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
    // cached to disk and refreshed once they've expired. The custom delegate
    // surfaces the consent URL on the run row (state='awaiting_auth') so the
    // web viewer can offer an "Authorize in Google" link for spawned runs.
    let auth = match yup_oauth2::InstalledFlowAuthenticator::builder(
        secret,
        yup_oauth2::InstalledFlowReturnMethod::HTTPPortRedirect(cfg.oauth_port),
    )
    .persist_tokens_to_disk(cfg.tokens.clone())
    .flow_delegate(Box::new(RunRowAuthDelegate {
        write_tx: write_tx.downgrade(),
    }))
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

    // Acquire a token up front instead of lazily on the first API call, so an
    // unattended consent is bounded by GMAIL_STATS_AUTH_TIMEOUT (the run turns
    // failed/auth_required instead of hanging forever) and the run row can
    // transition awaiting_auth -> running the moment tokens arrive. When the
    // token cache is already valid this never prompts.
    let auth_timeout = Duration::from_secs(env_or(
        "GMAIL_STATS_AUTH_TIMEOUT",
        DEFAULT_AUTH_TIMEOUT_SECS,
    )?);
    let scopes = ["https://www.googleapis.com/auth/gmail.readonly"];
    match consent_within(auth.token(&scopes), auth_timeout, cancel.as_ref()).await {
        Consent::Granted(_) => {}
        Consent::Cancelled => {
            drop(write_tx);
            let _ = writer_handle.await;
            ingest::finish_run(options, run_id, "cancelled", None, None).await?;
            println!("scan cancelled before OAuth consent completed");
            return Ok(());
        }
        Consent::Failed(e) => {
            drop(write_tx);
            let _ = writer_handle.await;
            let mut msg = format!("{e:#}");
            if msg.contains("in use") {
                msg.push_str(&format!(
                    " (the OAuth redirect listener wants localhost:{} — free the port, or pick \
                     another with --oauth-port and add it to the OAuth client's redirect URIs)",
                    cfg.oauth_port
                ));
            }
            ingest::finish_run(
                options,
                run_id,
                "failed",
                Some(consent_error_kind(&msg)),
                Some(&msg),
            )
            .await
            .ok();
            return Err(e);
        }
    }
    // Tokens are in hand: leave awaiting_auth (a no-op when consent was never
    // needed — the row is already 'running').
    let _ = write_tx
        .send(WriteMsg::AuthState {
            state: "running",
            auth_url: None,
        })
        .await;

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
        resume_unverified: resumed_token.is_some(),
        resume_token: resumed_token,
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

/// Default bound on how long an interactive OAuth consent may stay pending
/// before the run is marked failed/auth_required. Override with the
/// GMAIL_STATS_AUTH_TIMEOUT env var (seconds).
const DEFAULT_AUTH_TIMEOUT_SECS: u64 = 600;

/// Look up the persisted page token for `scan --resume <run_id>`. Missing or
/// wrong-source runs are hard errors (the user named a specific run); a run
/// without a saved token just means a from-scratch scan.
async fn resolve_scan_resume(
    conn: &mut SqliteConnection,
    run_id: i64,
) -> anyhow::Result<Option<String>> {
    let row = sqlx::query("SELECT source, resume_token FROM ingest_runs WHERE run_id = ?")
        .bind(run_id)
        .fetch_optional(&mut *conn)
        .await?;
    let Some(row) = row else {
        anyhow::bail!("no ingest run {run_id} to resume");
    };
    let source: String = row.try_get("source")?;
    anyhow::ensure!(
        source == "gmail_api",
        "run {run_id} is a {source} run, not a Gmail API scan"
    );
    let token: Option<String> = row.try_get("resume_token")?;
    match token {
        Some(token) if !token.is_empty() => {
            if chatty() {
                println!("resuming scan run {run_id} from its saved page token (best effort)");
            }
            Ok(Some(token))
        }
        _ => {
            if chatty() {
                println!("run {run_id} saved no page token; scanning from the beginning");
            }
            Ok(None)
        }
    }
}

/// InstalledFlow delegate that mirrors consent progress onto the run row:
/// state='awaiting_auth' plus the URL while Google waits for the user, so the
/// web viewer can render a validated "Authorize in Google" link. Terminal UX
/// is unchanged: the URL is still printed unless --quiet.
///
/// Holds only a *weak* sender: the authenticator (and thus this delegate)
/// lives as long as the whole scan, and a strong clone here would keep the
/// writer channel open forever, deadlocking the end-of-scan drain.
struct RunRowAuthDelegate {
    write_tx: mpsc::WeakSender<WriteMsg>,
}

impl yup_oauth2::authenticator_delegate::InstalledFlowDelegate for RunRowAuthDelegate {
    fn present_user_url<'a>(
        &'a self,
        url: &'a str,
        need_code: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>>
    {
        Box::pin(present_auth_url(self.write_tx.clone(), url, need_code))
    }
}

async fn present_auth_url(
    write_tx: mpsc::WeakSender<WriteMsg>,
    url: &str,
    need_code: bool,
) -> Result<String, String> {
    // Best-effort: consent must not die just because the writer is gone.
    if let Some(write_tx) = write_tx.upgrade() {
        let _ = write_tx
            .send(WriteMsg::AuthState {
                state: "awaiting_auth",
                auth_url: Some(url.to_string()),
            })
            .await;
    }
    if need_code {
        // Parity with yup-oauth2's default delegate: interactive fallback
        // reads the code from stdin. Spawned runs use the port-redirect
        // flow (need_code=false) and have stdin null, so this path is
        // terminal-only.
        if chatty() {
            println!(
                "Please direct your browser to {url}, follow the instructions and enter \
                 the code displayed here: "
            );
        }
        use tokio::io::AsyncBufReadExt;
        let mut user_input = String::new();
        tokio::io::BufReader::new(tokio::io::stdin())
            .read_line(&mut user_input)
            .await
            .map_err(|e| format!("couldn't read code: {e}"))?;
        user_input.truncate(user_input.trim_end().len());
        Ok(user_input)
    } else {
        if chatty() {
            println!(
                "Please direct your browser to {url} and follow the instructions \
                 displayed there."
            );
        }
        Ok(String::new())
    }
}

/// Outcome of the up-front OAuth token acquisition.
enum Consent<T> {
    Granted(T),
    /// The shutdown signal arrived while waiting at the consent prompt.
    Cancelled,
    Failed(anyhow::Error),
}

/// Bound an OAuth token acquisition so an unattended consent (nobody ever
/// opens the URL) becomes a clean failure instead of an eternal hang, and
/// race it against the shutdown flag: the consent wait is the one place the
/// ingester blocks without polling `cancel`, so without this a Ctrl-C at the
/// prompt would sit out the whole auth timeout before exiting.
async fn consent_within<T, E>(
    fut: impl std::future::Future<Output = Result<T, E>>,
    timeout: Duration,
    cancel: &AtomicBool,
) -> Consent<T>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let cancelled = async {
        while !cancel.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    tokio::select! {
        r = tokio::time::timeout(timeout, fut) => match r {
            Ok(Ok(value)) => Consent::Granted(value),
            Ok(Err(e)) => {
                Consent::Failed(anyhow::Error::new(e).context("acquiring an OAuth token"))
            }
            Err(_) => Consent::Failed(anyhow::anyhow!(
                "OAuth consent was not completed within {}s \
                 (set GMAIL_STATS_AUTH_TIMEOUT to adjust)",
                timeout.as_secs()
            )),
        },
        _ = cancelled => Consent::Cancelled,
    }
}

/// error_kind for a failed consent: Advanced Protection rejections keep their
/// dedicated kind so the UI can steer to Takeout; everything else (timeout,
/// declined consent, broken client secret) is auth_required.
fn consent_error_kind(message: &str) -> &'static str {
    if message.contains("policy_enforced") {
        "policy_enforced"
    } else {
        "auth_required"
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
/// every page, so `scan --resume <run_id>` can pick it up best-effort.
struct ScanProgress {
    resume_token: Option<String>,
    /// True while resume_token came from a previous run's row and has not yet
    /// produced a successful list: Gmail rejecting it (expired/invalid) means
    /// a silent from-scratch restart, not a failure.
    resume_unverified: bool,
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
        let result = match list_messages(
            ctx.hub,
            progress.resume_token.as_deref(),
            ctx.rate_limiter,
        )
        .await
        {
            Ok(result) => result,
            Err(e) if progress.resume_unverified => {
                // The persisted token from a previous run was rejected
                // (typically expired). Fall back to page one silently —
                // seen-mail dedupe makes the re-listing idempotent.
                progress.resume_unverified = false;
                progress.resume_token = None;
                if verbose() {
                    println!(
                        "persisted resume token was rejected ({e:#}); \
                             restarting the scan from the beginning"
                    );
                }
                continue;
            }
            Err(e) => return Err(e),
        };
        progress.resume_unverified = false;

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

/// The headers both the scan's per-message fetch and the backfill repair
/// pass request with format=metadata: the sender headers (same priority as
/// the import: From, then Return-Path), the Message-ID for cross-source
/// dedupe, and the two List-Unsubscribe headers (issue #41) — one shared set
/// so a message yields identical data no matter which path fetched it.
const METADATA_HEADERS: &[&str] = &[
    "From",
    "Return-Path",
    "Message-ID",
    "List-Unsubscribe",
    "List-Unsubscribe-Post",
];

async fn parse_messages(ctx: &ScanCtx<'_>, messages: Vec<Message>) -> anyhow::Result<()> {
    let fetch = |message_id: String| {
        let hub = ctx.hub.clone();
        let rate_limiter = ctx.rate_limiter.clone();
        async move { fetch_message_metadata_headers(&hub, &message_id, &rate_limiter).await }
    };
    fetch_listed_messages(
        ctx.pool,
        ctx.write_tx,
        ctx.fetch_concurrency,
        fetch,
        ctx.cancel,
        messages,
    )
    .await
}

/// The scan's per-page core: fetch each listed message's headers concurrently
/// (bounded) and hand the results to the writer task — the sender count, the
/// Message-ID for cross-source dedupe, and any List-Unsubscribe capture
/// (issue #41; latest ingested message wins, exactly like the importer).
/// Generic over the fetch function so it is testable with fabricated
/// messages; the live path plugs in the rate-limited metadata fetch.
async fn fetch_listed_messages<F, Fut>(
    pool: &Pool<Sqlite>,
    write_tx: &mpsc::Sender<WriteMsg>,
    fetch_concurrency: usize,
    fetch: F,
    cancel: &AtomicBool,
    messages: Vec<Message>,
) -> anyhow::Result<()>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Message>>,
{
    let fetch_ref = &fetch;
    stream::iter(messages.into_iter().map(Ok::<_, anyhow::Error>))
        .try_for_each_concurrent(fetch_concurrency, move |message_meta| {
            let write_tx = write_tx.clone();
            async move {
                if cancel.load(Ordering::Relaxed) {
                    return Ok(());
                }
                let message_id = message_meta.id.expect("message missing id");
                if seen_mail(&message_id, pool).await? {
                    return Ok(());
                }

                let message = fetch_ref(message_id.clone()).await?;
                let sender = cleanup_sender(get_sender(&message)?);
                if verbose() {
                    println!("sender: {:?}", sender);
                }

                // Unsubscribe capture rides ahead of the Seen row on the same
                // FIFO channel; malformed or absent headers yield None and
                // leave any earlier capture alone. The Seen insert marks the
                // row unsubscribe_checked, so the backfill never refetches
                // messages the live scan already examined.
                if let Some(capture) = get_unsubscribe(&message) {
                    write_tx
                        .send(WriteMsg::Unsubscribe {
                            sender: sender.clone(),
                            capture,
                            internal_date: message.internal_date,
                        })
                        .await
                        .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;
                }

                write_tx
                    .send(WriteMsg::Seen(SeenMail {
                        rfc_message_id: get_rfc_message_id(&message),
                        message_id,
                        sender,
                    }))
                    .await
                    .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;

                Ok(())
            }
        })
        .await
}

/// Fetch a single message's [`METADATA_HEADERS`] with format=metadata — much
/// cheaper than a full fetch when only the headers are needed — waiting for a
/// rate-limit tick before each attempt and backing off exponentially on
/// transient errors (rate limits, 5xx responses, network failures). Shared by
/// the scan's per-message fetch and the backfill repair pass.
async fn fetch_message_metadata_headers(
    hub: &GmailHub,
    message_id: &str,
    rate_limiter: &Mutex<Interval>,
) -> anyhow::Result<Message> {
    fetch_message_with(hub, message_id, rate_limiter, Some(METADATA_HEADERS)).await
}

async fn fetch_message_with(
    hub: &GmailHub,
    message_id: &str,
    rate_limiter: &Mutex<Interval>,
    metadata_headers: Option<&[&str]>,
) -> anyhow::Result<Message> {
    let mut attempts = 0;
    let mut backoff = Duration::from_secs(1);
    loop {
        rate_limiter.lock().await.tick().await;

        let mut call = hub
            .users()
            .messages_get("me", message_id)
            .add_scope(Scope::Readonly);
        if let Some(headers) = metadata_headers {
            call = call.format("metadata");
            for header in headers {
                call = call.add_metadata_headers(header);
            }
        }
        let res = call.doit().await;

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
// scan --backfill: repair pass for historical scan rows
// ---------------------------------------------------------------------------

// Rows the backfill still has to repair are matched by the predicate
// `mail_id NOT LIKE 'mid:%' AND (rfc_message_id IS NULL OR
// unsubscribe_checked = 0)`: API-scan rows (bare Gmail ids, never
// `mid:`-prefixed) whose Message-ID is not known yet OR whose headers were
// never examined for List-Unsubscribe data (issue #41 Phase U2 — this covers
// rows an older, message-id-only pass already populated). Repaired rows have
// a non-NULL rfc_message_id (a real id, or '' for "fetched, has none") AND
// unsubscribe_checked = 1, so the same predicate makes re-runs and resumption
// automatic and "checked, nothing to capture" is never refetched. It appears
// inline in the two queries below (sqlx requires literal SQL).

/// The repair pass behind `scan --backfill` (issue #26 Phase D, extended by
/// issue #41 Phase U2): a first-class ingest run — flock and stale-run
/// cleanup happen in main() like every run, it writes an ingest_runs row with
/// heartbeats, stops cleanly on SIGTERM/SIGINT, and is idempotent/resumable
/// by construction.
///
/// It fetches format=metadata for every unrepaired scan row through the same
/// rate-limited, retrying fetch machinery as the scan, records the normalized
/// Message-ID, collapses historical cross-source double counts, and captures
/// per-sender List-Unsubscribe targets (see ingest::backfill_writer for the
/// per-row transaction).
async fn run_backfill(
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

    // Everything still to do; also the run's total for progress display. A
    // fully repaired database finishes here without touching the network.
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(1) FROM seen_mails \
         WHERE mail_id NOT LIKE 'mid:%' \
         AND (rfc_message_id IS NULL OR unsubscribe_checked = 0)",
    )
    .fetch_one(&mut writer_conn)
    .await?;
    let run_id = ingest::create_run(&mut writer_conn, "backfill", None).await?;
    // The writer task isn't spawned yet, so this connection is still the
    // single writer; set the total directly.
    sqlx::query("UPDATE ingest_runs SET total_estimate = ? WHERE run_id = ?")
        .bind(pending)
        .bind(run_id)
        .execute(&mut writer_conn)
        .await?;
    if pending == 0 {
        drop(writer_conn);
        ingest::finish_run(options, run_id, "done", None, None).await?;
        if chatty() {
            println!(
                "nothing to backfill: every scanned message already has its \
                 Message-ID recorded and its unsubscribe headers checked"
            );
        }
        return Ok(());
    }
    if chatty() {
        println!(
            "backfilling Message-ID and List-Unsubscribe headers for {pending} \
             scanned message(s) (run {run_id})"
        );
    }

    // OAuth, exactly like the scan (same credential/token files). Consent is
    // terminal-only here: the backfill is started from a shell, and a repaired
    // token cache from the original scan normally makes this silent.
    let secret = match yup_oauth2::read_application_secret(&cfg.credentials).await {
        Ok(secret) => secret,
        Err(e) => {
            drop(writer_conn);
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
                "reading OAuth client secret from {} (the backfill uses the same \
                 OAuth setup as the scan; see the README)",
                cfg.credentials.display()
            )));
        }
    };
    let auth = match yup_oauth2::InstalledFlowAuthenticator::builder(
        secret,
        yup_oauth2::InstalledFlowReturnMethod::HTTPPortRedirect(cfg.oauth_port),
    )
    .persist_tokens_to_disk(cfg.tokens.clone())
    .build()
    .await
    {
        Ok(auth) => auth,
        Err(e) => {
            drop(writer_conn);
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
    let auth_timeout = Duration::from_secs(env_or(
        "GMAIL_STATS_AUTH_TIMEOUT",
        DEFAULT_AUTH_TIMEOUT_SECS,
    )?);
    let scopes = ["https://www.googleapis.com/auth/gmail.readonly"];
    match consent_within(auth.token(&scopes), auth_timeout, cancel.as_ref()).await {
        Consent::Granted(_) => {}
        Consent::Cancelled => {
            drop(writer_conn);
            ingest::finish_run(options, run_id, "cancelled", None, None).await?;
            println!("backfill cancelled before OAuth consent completed");
            return Ok(());
        }
        Consent::Failed(e) => {
            drop(writer_conn);
            let msg = format!("{e:#}");
            ingest::finish_run(
                options,
                run_id,
                "failed",
                Some(consent_error_kind(&msg)),
                Some(&msg),
            )
            .await
            .ok();
            return Err(e);
        }
    }
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

    let mut read_conn = SqliteConnection::connect_with(options).await?;
    let (write_tx, write_rx) = mpsc::channel::<ingest::BackfillMsg>(100);
    let writer_handle = task::spawn(ingest::backfill_writer(writer_conn, write_rx, run_id));

    let mut interval = tokio::time::interval(Duration::from_millis(rate_limit_ms));
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let rate_limiter = Arc::new(Mutex::new(interval));
    let hub_ref = &hub;
    let rate_limiter_ref = &rate_limiter;
    let fetch = move |mail_id: String| async move {
        fetch_message_metadata_headers(hub_ref, &mail_id, rate_limiter_ref).await
    };

    let loop_result = backfill_scan_rows(
        &mut read_conn,
        &write_tx,
        fetch_concurrency,
        fetch,
        cancel.as_ref(),
    )
    .await;

    drop(write_tx);
    let writer_result = match writer_handle.await {
        Ok(result) => result,
        Err(join_err) => {
            let msg = format!("DB writer task panicked: {join_err:?}");
            ingest::finish_run(options, run_id, "failed", Some("db"), Some(&msg))
                .await
                .ok();
            anyhow::bail!("{msg}");
        }
    };

    let outcome = match (loop_result, writer_result) {
        (Ok(()), Ok(outcome)) => outcome,
        (_, Err(writer_err)) => {
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
        (Err(e), Ok(outcome)) => {
            if cancel.load(Ordering::Relaxed) {
                // Shutdown fallout, not a real failure: everything committed
                // so far is durable, and a re-run continues from it.
                outcome
            } else {
                ingest::finish_run(
                    options,
                    run_id,
                    "failed",
                    Some(classify_scan_error(&e)),
                    Some(&format!("{e:#}")),
                )
                .await
                .ok();
                return Err(e.context(
                    "backfill stopped; re-run `gmail_stats scan --backfill` \
                     to continue where it left off",
                ));
            }
        }
    };

    if cancel.load(Ordering::Relaxed) {
        ingest::finish_run(options, run_id, "cancelled", None, None).await?;
        println!(
            "backfill cancelled cleanly; re-run `gmail_stats scan --backfill` \
             to continue where it left off"
        );
    } else {
        ingest::finish_run(options, run_id, "done", None, None).await?;
        println!(
            "backfill finished: {} message(s) checked, {} Message-ID(s) recorded, \
             {} without a Message-ID, {} cross-source duplicate count(s) collapsed, \
             {} unsubscribe record(s) captured",
            outcome.examined,
            outcome.populated,
            outcome.missing_id,
            outcome.decremented,
            outcome.unsubscribe_recorded
        );
    }
    Ok(())
}

/// Iterate the unrepaired scan rows in keyset-paginated batches, fetch each
/// message's metadata (bounded concurrency, shared rate limiter), and send
/// the extracted Message-ID + sender + List-Unsubscribe capture to the
/// backfill writer.
///
/// The cursor advances over mail_id, so rows dispatched but not yet committed
/// by the writer are never selected twice within a run; across runs, repaired
/// rows are non-NULL and drop out of the query entirely. Generic over the
/// fetch function so the loop is testable with fabricated messages — the live
/// path plugs in the scan's rate-limited/retrying metadata fetch.
async fn backfill_scan_rows<F, Fut>(
    read_conn: &mut SqliteConnection,
    write_tx: &mpsc::Sender<ingest::BackfillMsg>,
    fetch_concurrency: usize,
    fetch: F,
    cancel: &AtomicBool,
) -> anyhow::Result<()>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<Message>>,
{
    let mut dispatched = 0u64;
    let mut cursor = String::new();
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let batch: Vec<String> = sqlx::query_scalar(
            "SELECT mail_id FROM seen_mails \
             WHERE mail_id NOT LIKE 'mid:%' \
             AND (rfc_message_id IS NULL OR unsubscribe_checked = 0) \
             AND mail_id > ? ORDER BY mail_id LIMIT 500",
        )
        .bind(&cursor)
        .fetch_all(&mut *read_conn)
        .await?;
        let Some(last) = batch.last() else {
            return Ok(());
        };
        cursor = last.clone();
        dispatched += batch.len() as u64;

        let fetch_ref = &fetch;
        stream::iter(batch.into_iter().map(Ok::<_, anyhow::Error>))
            .try_for_each_concurrent(fetch_concurrency, move |mail_id| {
                let write_tx = write_tx.clone();
                async move {
                    if cancel.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    let message = fetch_ref(mail_id.clone())
                        .await
                        .with_context(|| format!("fetching metadata for message {mail_id}"))?;
                    let sender = cleanup_sender(get_sender(&message)?);
                    if verbose() {
                        println!("backfill {mail_id}: sender {sender:?}");
                    }
                    write_tx
                        .send(ingest::BackfillMsg {
                            rfc_message_id: get_rfc_message_id(&message),
                            unsubscribe: get_unsubscribe(&message),
                            internal_date: message.internal_date,
                            mail_id,
                            sender,
                        })
                        .await
                        .map_err(|_| anyhow::anyhow!("DB writer task closed unexpectedly"))?;
                    Ok(())
                }
            })
            .await?;
        if chatty() {
            println!("backfill: {dispatched} message(s) fetched");
        }
    }
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

/// Find a header value by case-insensitive name (RFC 5322) in a fetched
/// message's payload.
fn find_header<'a>(message: &'a Message, name: &str) -> Option<&'a str> {
    message
        .payload
        .as_ref()
        .and_then(|p| p.headers.as_deref())
        .unwrap_or(&[])
        .iter()
        .find_map(|header| {
            header
                .name
                .as_deref()
                .filter(|n| n.eq_ignore_ascii_case(name))
                .and(header.value.as_deref())
        })
}

fn get_sender(message: &Message) -> anyhow::Result<String> {
    // Check candidates in priority order via the shared pick_sender.
    let sender = pick_sender(
        find_header(message, "from"),
        find_header(message, "return-path"),
    );
    if sender.is_empty() && verbose() {
        println!("weird email without from header: {:?}", message.id);
    }
    Ok(sender)
}

/// The message's RFC Message-ID, normalized with the shared helper. Used by
/// the scan when marking a message seen and by the backfill repair pass, so
/// the same message always yields the same normalized id.
fn get_rfc_message_id(message: &Message) -> Option<String> {
    find_header(message, "message-id").and_then(ingest::normalize_rfc_message_id)
}

/// The message's validated List-Unsubscribe targets (issue #41), parsed with
/// the same shared `parse_unsubscribe_headers` the mbox importer uses, so
/// capture behaves identically across sources. Used by the scan's per-message
/// fetch and the backfill repair pass.
fn get_unsubscribe(message: &Message) -> Option<ingest::UnsubscribeCapture> {
    ingest::parse_unsubscribe_headers(
        find_header(message, "list-unsubscribe"),
        find_header(message, "list-unsubscribe-post"),
    )
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

    // --- get_rfc_message_id ---

    #[test]
    fn get_rfc_message_id_normalizes_the_header() {
        for (raw, want) in [
            ("<m1@example.com>", Some("m1@example.com")),
            ("m1@example.com", Some("m1@example.com")),
            ("  <m1@example.com>  ", Some("m1@example.com")),
            ("<>", None),
        ] {
            let message = message_with_headers(vec![header("Message-ID", Some(raw))]);
            assert_eq!(
                get_rfc_message_id(&message).as_deref(),
                want,
                "failed for {raw:?}"
            );
        }
        // Case-insensitive header name, like every RFC 5322 header.
        let message = message_with_headers(vec![header("MESSAGE-ID", Some("<m1@example.com>"))]);
        assert_eq!(
            get_rfc_message_id(&message).as_deref(),
            Some("m1@example.com")
        );
        // Absent header (or no payload): None.
        let message = message_with_headers(vec![header("Subject", Some("hi"))]);
        assert_eq!(get_rfc_message_id(&message), None);
        assert_eq!(get_rfc_message_id(&Message::default()), None);
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
        assert_eq!(cfg.mode, Mode::Scan { resume: None });
        assert_eq!(cfg.db, PathBuf::from("stats.db"));
        assert_eq!(cfg.credentials, PathBuf::from("credentials.json"));
        assert_eq!(cfg.tokens, PathBuf::from("tokencache.json"));
        assert_eq!(cfg.verbosity, Verbosity::Normal);
    }

    #[test]
    fn explicit_scan_subcommand_is_equivalent() {
        assert_eq!(run_cfg(&["scan"], no_env).mode, Mode::Scan { resume: None });
    }

    #[test]
    fn scan_accepts_a_resume_run_id() {
        assert_eq!(
            run_cfg(&["scan", "--resume", "5"], no_env).mode,
            Mode::Scan { resume: Some(5) }
        );
        assert_eq!(
            run_cfg(&["--resume", "5"], no_env).mode,
            Mode::Scan { resume: Some(5) }
        );
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
    fn backfill_flag_selects_the_repair_mode() {
        assert_eq!(
            run_cfg(&["scan", "--backfill"], no_env).mode,
            Mode::BackfillMessageIds
        );
        assert_eq!(
            run_cfg(&["--backfill"], no_env).mode,
            Mode::BackfillMessageIds
        );
        // The original flag stays a working alias.
        assert_eq!(
            run_cfg(&["scan", "--backfill-message-ids"], no_env).mode,
            Mode::BackfillMessageIds
        );
        assert_eq!(
            run_cfg(&["--backfill-message-ids"], no_env).mode,
            Mode::BackfillMessageIds
        );
        assert_eq!(
            run_cfg(&["--backfill", "--quiet"], no_env).verbosity,
            Verbosity::Quiet
        );
    }

    #[test]
    fn invalid_invocations_are_rejected() {
        for args in [
            &["--quiet", "--verbose"][..],
            &["import"][..],
            &["--resume", "x", "import", "f.mbox"][..],
            &["frobnicate"][..],
            &["--frobnicate"][..],
            &["scan", "extra"][..],
            &["import", "a.mbox", "extra"][..],
            &["--db"][..],
            &["scan", "--backfill", "--resume", "3"][..],
            &["scan", "--backfill-message-ids", "--resume", "3"][..],
            &["import", "a.mbox", "--backfill"][..],
            &["import", "a.mbox", "--backfill-message-ids"][..],
        ] {
            let result = parse_args(args.iter().map(|s| s.to_string()), no_env);
            assert!(result.is_err(), "expected {args:?} to be rejected");
        }
    }

    #[test]
    fn oauth_port_defaults_flag_env_and_precedence() {
        assert_eq!(run_cfg(&[], no_env).oauth_port, DEFAULT_OAUTH_PORT);
        assert_eq!(run_cfg(&["--oauth-port", "9123"], no_env).oauth_port, 9123);
        let env = |name: &str| (name == "GMAIL_STATS_OAUTH_PORT").then(|| "7001".to_string());
        assert_eq!(run_cfg(&[], env).oauth_port, 7001);
        // The flag wins over the env var.
        assert_eq!(run_cfg(&["--oauth-port=9123"], env).oauth_port, 9123);
    }

    #[test]
    fn oauth_port_rejects_non_ports() {
        for bad in ["0", "65536", "-1", "http", ""] {
            let args = ["--oauth-port".to_string(), bad.to_string()];
            assert!(
                parse_args(args, no_env).is_err(),
                "expected port {bad:?} to be rejected"
            );
        }
        let env = |name: &str| (name == "GMAIL_STATS_OAUTH_PORT").then(|| "nope".to_string());
        assert!(parse_args(std::iter::empty::<String>(), env).is_err());
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

    /// List-Unsubscribe capture fixture (issue #41): Alice sends two messages
    /// whose unsubscribe targets differ (latest must win), Bob is
    /// mailto-only with one-click on the *second* Alice message absent, Carol
    /// has no unsubscribe headers, Dave has capture-worthy headers but no
    /// Message-ID (counted as skipped, still captured), and Eve's header is
    /// hostile (never captured).
    const UNSUB_FIXTURE: &str = "From x Thu Jan  1 00:00:00 2020\n\
        From: Alice <a@example.com>\n\
        Message-ID: <u1@example.com>\n\
        List-Unsubscribe: <mailto:stop@example.com>, <https://news.example/u?tok=old>\n\
        List-Unsubscribe-Post: List-Unsubscribe=One-Click\n\
        \n\
        body\n\
        From x Thu Jan  1 00:00:00 2020\n\
        From: Alice <a@example.com>\n\
        Message-ID: <u2@example.com>\n\
        List-Unsubscribe: <https://news.example/u?tok=new>\n\
        \n\
        body\n\
        From x Thu Jan  1 00:00:00 2020\n\
        From: Bob <b@example.com>\n\
        Message-ID: <u3@example.com>\n\
        List-Unsubscribe: <mailto:leave@example.com?subject=unsubscribe>\n\
        \n\
        body\n\
        From x Thu Jan  1 00:00:00 2020\n\
        From: Carol <c@example.com>\n\
        Message-ID: <u4@example.com>\n\
        \n\
        body\n\
        From x Thu Jan  1 00:00:00 2020\n\
        From: Dave <d@example.com>\n\
        List-Unsubscribe: <https://dave.example/u>\n\
        List-Unsubscribe-Post: List-Unsubscribe=One-Click\n\
        \n\
        body\n\
        From x Thu Jan  1 00:00:00 2020\n\
        From: Eve <e@example.com>\n\
        Message-ID: <u5@example.com>\n\
        List-Unsubscribe: <javascript:alert(1)>, <http://evil.example/u>\n\
        \n\
        body\n";

    async fn unsubscribe_rows(
        options: &SqliteConnectOptions,
    ) -> Vec<(String, Option<String>, Option<String>, i64)> {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        sqlx::query_as(
            "SELECT sender, https_url, mailto, one_click \
             FROM sender_unsubscribe ORDER BY sender",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap()
    }

    fn assert_unsub_fixture_rows(rows: &[(String, Option<String>, Option<String>, i64)]) {
        assert_eq!(
            rows.len(),
            3,
            "only senders with valid captures get rows: {rows:?}"
        );
        // Alice: her *second* message wins wholesale — new token, no mailto,
        // and no one-click (that message had no Post header).
        assert_eq!(rows[0].0, "a@example.com");
        assert_eq!(rows[0].1.as_deref(), Some("https://news.example/u?tok=new"));
        assert_eq!(rows[0].2, None);
        assert_eq!(rows[0].3, 0);
        // Bob: mailto only, never one-click without an https target.
        assert_eq!(rows[1].0, "b@example.com");
        assert_eq!(rows[1].1, None);
        assert_eq!(
            rows[1].2.as_deref(),
            Some("mailto:leave@example.com?subject=unsubscribe")
        );
        assert_eq!(rows[1].3, 0);
        // Dave: captured despite having no Message-ID (his message is skipped
        // for counting but still parsed).
        assert_eq!(rows[2].0, "d@example.com");
        assert_eq!(rows[2].1.as_deref(), Some("https://dave.example/u"));
        assert_eq!(rows[2].3, 1);
    }

    /// The full unsubscribe capture loop plus the back-populate property: a
    /// re-import over a database that already counted every message (but has
    /// no unsubscribe data, like one built by a pre-#41 binary) adds zero
    /// counts yet fills sender_unsubscribe.
    #[tokio::test]
    async fn import_captures_unsubscribe_and_reimport_back_populates() {
        let dir = tempfile::tempdir().unwrap();
        let mbox = dir.path().join("mail.mbox");
        std::fs::write(&mbox, UNSUB_FIXTURE).unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));

        let conn = migrated_conn(&options).await;
        let s1 = run_import(&options, conn, &mbox, None, unset_cancel())
            .await
            .unwrap();
        assert_eq!(s1.messages_seen, 6);
        assert_eq!(s1.messages_new, 5, "Dave has no Message-ID");
        assert_eq!(s1.skipped, 1);
        assert_unsub_fixture_rows(&unsubscribe_rows(&options).await);

        // Simulate a database whose counts predate unsubscribe capture: the
        // messages are all seen, but sender_unsubscribe is empty.
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        sqlx::query("DELETE FROM sender_unsubscribe")
            .execute(&mut conn)
            .await
            .unwrap();
        conn.close().await.ok();

        // Re-import: every message is a seen-duplicate (zero count changes),
        // yet capture happens for all of them — the table is back-populated.
        let conn = migrated_conn(&options).await;
        let s2 = run_import(&options, conn, &mbox, None, unset_cancel())
            .await
            .unwrap();
        assert_eq!(s2.messages_seen, 6);
        assert_eq!(s2.messages_new, 0, "re-import must add zero new counts");
        assert_eq!(sender_count(&options, "a@example.com").await, Some(2));
        assert_eq!(sender_count(&options, "b@example.com").await, Some(1));
        assert_eq!(sender_count(&options, "c@example.com").await, Some(1));
        assert_unsub_fixture_rows(&unsubscribe_rows(&options).await);
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

    // --- scan resume + OAuth consent surfacing ---

    #[tokio::test]
    async fn resolve_scan_resume_returns_the_persisted_token() {
        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;
        let run_id = ingest::create_run(&mut conn, "gmail_api", None)
            .await
            .unwrap();
        sqlx::query("UPDATE ingest_runs SET resume_token = 'tok-123' WHERE run_id = ?")
            .bind(run_id)
            .execute(&mut conn)
            .await
            .unwrap();

        let token = resolve_scan_resume(&mut conn, run_id).await.unwrap();
        assert_eq!(token.as_deref(), Some("tok-123"));
    }

    #[tokio::test]
    async fn resolve_scan_resume_without_a_token_scans_from_scratch() {
        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;
        let run_id = ingest::create_run(&mut conn, "gmail_api", None)
            .await
            .unwrap();
        assert_eq!(resolve_scan_resume(&mut conn, run_id).await.unwrap(), None);
    }

    #[tokio::test]
    async fn resolve_scan_resume_rejects_missing_and_wrong_source_runs() {
        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;

        let err = resolve_scan_resume(&mut conn, 999).await.unwrap_err();
        assert!(err.to_string().contains("no ingest run"));

        let run_id = ingest::create_run(&mut conn, "mbox", None).await.unwrap();
        let err = resolve_scan_resume(&mut conn, run_id).await.unwrap_err();
        assert!(err.to_string().contains("not a Gmail API scan"));
    }

    /// The consent delegate writes state='awaiting_auth' + auth_url onto the
    /// run row via the writer channel, and the post-consent AuthState message
    /// returns it to 'running'. No real OAuth is involved: the delegate is
    /// called directly, exactly as yup-oauth2 would.
    #[tokio::test]
    async fn auth_delegate_surfaces_the_consent_url_on_the_run_row() {
        use google_gmail1::yup_oauth2::authenticator_delegate::InstalledFlowDelegate;

        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;
        let run_id = ingest::create_run(&mut conn, "gmail_api", None)
            .await
            .unwrap();

        let (tx, rx) = mpsc::channel::<WriteMsg>(8);
        let writer = task::spawn(ingest::db_writer(conn, rx, run_id));

        let delegate = RunRowAuthDelegate {
            write_tx: tx.downgrade(),
        };
        let url = "https://accounts.google.com/o/oauth2/auth?client_id=x&redirect_uri=y";
        let code = delegate.present_user_url(url, false).await.unwrap();
        assert_eq!(code, "", "redirect mode returns no code");

        // Let the writer drain the awaiting_auth message, then check the row.
        tx.send(WriteMsg::Progress {
            messages_seen: 0,
            bytes_done: None,
            resume_token: None,
            total_estimate: None,
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut check = SqliteConnection::connect_with(&options).await.unwrap();
        let row = sqlx::query("SELECT state, auth_url FROM ingest_runs WHERE run_id = ?")
            .bind(run_id)
            .fetch_one(&mut check)
            .await
            .unwrap();
        assert_eq!(row.try_get::<String, _>("state").unwrap(), "awaiting_auth");
        assert_eq!(row.try_get::<String, _>("auth_url").unwrap(), url);

        // Tokens arrived: the scanner sends the running transition.
        tx.send(WriteMsg::AuthState {
            state: "running",
            auth_url: None,
        })
        .await
        .unwrap();
        // The delegate holds a Sender clone; both must drop for the writer
        // to drain and exit.
        drop(delegate);
        drop(tx);
        writer.await.unwrap().unwrap();
        let row = sqlx::query("SELECT state, auth_url FROM ingest_runs WHERE run_id = ?")
            .bind(run_id)
            .fetch_one(&mut check)
            .await
            .unwrap();
        assert_eq!(row.try_get::<String, _>("state").unwrap(), "running");
        assert_eq!(row.try_get::<Option<String>, _>("auth_url").unwrap(), None);
    }

    /// An unattended consent (token future never resolves) must convert into
    /// an error after the timeout, and its kind must map to auth_required.
    #[tokio::test]
    async fn consent_timeout_becomes_auth_required() {
        let never = AtomicBool::new(false);
        let outcome = consent_within(
            std::future::pending::<Result<(), std::io::Error>>(),
            Duration::from_millis(50),
            &never,
        )
        .await;
        let Consent::Failed(err) = outcome else {
            panic!("expected a timeout failure");
        };
        let msg = format!("{err:#}");
        assert!(msg.contains("consent was not completed"), "got: {msg}");
        assert_eq!(consent_error_kind(&msg), "auth_required");
        assert_eq!(
            consent_error_kind("Error 400: policy_enforced"),
            "policy_enforced"
        );
    }

    /// Ctrl-C while sitting at the consent prompt must cancel promptly — not
    /// block out the remainder of the (here: 60s) auth timeout.
    #[tokio::test]
    async fn consent_cancelled_by_shutdown_flag_returns_promptly() {
        let flag = Arc::new(AtomicBool::new(false));
        let flip = flag.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flip.store(true, Ordering::Relaxed);
        });
        let start = std::time::Instant::now();
        let outcome = consent_within(
            std::future::pending::<Result<(), std::io::Error>>(),
            Duration::from_secs(60),
            flag.as_ref(),
        )
        .await;
        assert!(matches!(outcome, Consent::Cancelled));
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "cancellation took {:?}",
            start.elapsed()
        );
    }

    // --- backfill fetch loop (fabricated rows and messages; the live path
    //     plugs the scan's rate-limited metadata fetch into the same loop) ---

    /// A canned message for the fabricated fetcher: exactly the shape a
    /// format=metadata fetch of [`METADATA_HEADERS`] yields.
    #[derive(Clone)]
    struct FakeMail {
        id: &'static str,
        rfc: Option<&'static str>,
        from: &'static str,
        list_unsubscribe: Option<&'static str>,
        list_unsubscribe_post: Option<&'static str>,
        internal_date: Option<i64>,
    }

    fn fake_mail(id: &'static str, rfc: Option<&'static str>, from: &'static str) -> FakeMail {
        FakeMail {
            id,
            rfc,
            from,
            list_unsubscribe: None,
            list_unsubscribe_post: None,
            internal_date: None,
        }
    }

    /// A fake fetcher standing in for fetch_message_metadata_headers: returns
    /// the canned message per Gmail id, and errors on any id it wasn't given
    /// (so tests prove exactly which rows get fetched).
    fn fabricated_fetcher(
        mails: Vec<FakeMail>,
    ) -> impl Fn(String) -> std::future::Ready<anyhow::Result<Message>> {
        move |mail_id: String| {
            let found = mails.iter().find(|mail| mail.id == mail_id);
            std::future::ready(match found {
                None => Err(anyhow::anyhow!("unexpected fetch for {mail_id}")),
                Some(mail) => {
                    let mut headers = vec![header("From", Some(mail.from))];
                    if let Some(rfc) = mail.rfc {
                        headers.push(header("Message-ID", Some(rfc)));
                    }
                    if let Some(value) = mail.list_unsubscribe {
                        headers.push(header("List-Unsubscribe", Some(value)));
                    }
                    if let Some(value) = mail.list_unsubscribe_post {
                        headers.push(header("List-Unsubscribe-Post", Some(value)));
                    }
                    let mut message = message_with_headers(headers);
                    message.id = Some(mail.id.to_string());
                    message.internal_date = mail.internal_date;
                    Ok(message)
                }
            })
        }
    }

    async fn unsubscribe_url(options: &SqliteConnectOptions, sender: &str) -> Option<String> {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        sqlx::query_scalar("SELECT https_url FROM sender_unsubscribe WHERE sender = ?")
            .bind(sender)
            .fetch_optional(&mut conn)
            .await
            .unwrap()
            .flatten()
    }

    async fn checked_of(options: &SqliteConnectOptions, mail_id: &str) -> i64 {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        sqlx::query_scalar("SELECT unsubscribe_checked FROM seen_mails WHERE mail_id = ?")
            .bind(mail_id)
            .fetch_one(&mut conn)
            .await
            .unwrap()
    }

    /// End-to-end over fabricated rows: the loop selects exactly the bare
    /// Gmail rows that are still pending — NULL rfc_message_id OR never
    /// unsubscribe-checked, so rows the old message-id-only pass repaired
    /// (g4) are revisited once for capture, while fully checked rows (g5)
    /// and mid: rows are untouched. The writer populates, collapses, captures
    /// unsubscribe targets, and marks everything checked. A second pass finds
    /// nothing to fetch.
    #[tokio::test]
    async fn backfill_loop_repairs_only_pending_scan_rows() {
        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;
        // Historical mixed DB: g1 double-counted with the mid: row, g2 scan
        // only, g3 has no Message-ID, g4 repaired by the old message-id-only
        // pass (never unsubscribe-checked), g5 fully repaired by a U2 binary,
        // plus a mid: row.
        sqlx::query(
            "INSERT INTO seen_mails (mail_id, rfc_message_id, unsubscribe_checked) VALUES \
             ('g1', NULL, 0), ('g2', NULL, 0), ('g3', NULL, 0), \
             ('g4', 'm4@example.com', 0), \
             ('g5', 'm5@example.com', 1), \
             ('mid:<m1@example.com>', 'm1@example.com', 0)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO senders (sender, mails_sent) VALUES \
             ('a@example.com', 3), ('b@example.com', 2)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        let run_id = ingest::create_run(&mut conn, "backfill", None)
            .await
            .unwrap();

        let (tx, rx) = mpsc::channel(16);
        let writer = task::spawn(ingest::backfill_writer(conn, rx, run_id));
        let fetch = fabricated_fetcher(vec![
            fake_mail("g1", Some("<m1@example.com>"), "Alice <a@example.com>"),
            FakeMail {
                list_unsubscribe: Some("<https://b.example/u?tok=g2>"),
                list_unsubscribe_post: Some("List-Unsubscribe=One-Click"),
                internal_date: Some(1_000),
                ..fake_mail("g2", Some("<m2@example.com>"), "Bob <b@example.com>")
            },
            fake_mail("g3", None, "Alice <a@example.com>"),
            FakeMail {
                list_unsubscribe: Some("<https://d.example/u?tok=g4>"),
                internal_date: Some(2_000),
                ..fake_mail("g4", Some("<m4@example.com>"), "Dave <d@example.com>")
            },
        ]);
        let mut read_conn = SqliteConnection::connect_with(&options).await.unwrap();
        backfill_scan_rows(&mut read_conn, &tx, 4, &fetch, &AtomicBool::new(false))
            .await
            .unwrap();
        drop(tx);
        let outcome = writer.await.unwrap().unwrap();
        assert_eq!(outcome.examined, 4, "g5 and the mid: row are never fetched");
        assert_eq!(outcome.populated, 2);
        assert_eq!(outcome.missing_id, 1);
        assert_eq!(outcome.decremented, 1, "only g1 pairs with the mid: row");
        assert_eq!(outcome.unsubscribe_recorded, 2, "g2 and g4 carry targets");

        // Counts collapsed exactly once; the untouched rows kept theirs.
        assert_eq!(sender_count(&options, "a@example.com").await, Some(2));
        assert_eq!(sender_count(&options, "b@example.com").await, Some(2));
        // Captures landed; rows without targets got none but are checked.
        assert_eq!(
            unsubscribe_url(&options, "b@example.com").await.as_deref(),
            Some("https://b.example/u?tok=g2")
        );
        assert_eq!(
            unsubscribe_url(&options, "d@example.com").await.as_deref(),
            Some("https://d.example/u?tok=g4")
        );
        assert_eq!(unsubscribe_url(&options, "a@example.com").await, None);
        for mail_id in ["g1", "g2", "g3", "g4"] {
            assert_eq!(checked_of(&options, mail_id).await, 1, "{mail_id}");
        }

        // Second pass: nothing left to fetch — the fetcher would error on any
        // call, so an empty run proves idempotency.
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let run_id = ingest::create_run(&mut conn, "backfill", None)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(16);
        let writer = task::spawn(ingest::backfill_writer(conn, rx, run_id));
        let failing = fabricated_fetcher(vec![]);
        backfill_scan_rows(&mut read_conn, &tx, 4, &failing, &AtomicBool::new(false))
            .await
            .unwrap();
        drop(tx);
        let outcome = writer.await.unwrap().unwrap();
        assert_eq!(outcome.examined, 0, "a repaired database is a no-op");
    }

    /// Cancellation stops the loop between batches without error; whatever
    /// was committed stays repaired for the next run.
    #[tokio::test]
    async fn backfill_loop_stops_on_cancel() {
        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;
        sqlx::query("INSERT INTO seen_mails (mail_id, rfc_message_id) VALUES ('g1', NULL)")
            .execute(&mut conn)
            .await
            .unwrap();
        let run_id = ingest::create_run(&mut conn, "backfill", None)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(16);
        let writer = task::spawn(ingest::backfill_writer(conn, rx, run_id));
        let fetch = fabricated_fetcher(vec![]);
        let mut read_conn = SqliteConnection::connect_with(&options).await.unwrap();
        backfill_scan_rows(&mut read_conn, &tx, 4, &fetch, &AtomicBool::new(true))
            .await
            .unwrap();
        drop(tx);
        let outcome = writer.await.unwrap().unwrap();
        assert_eq!(outcome.examined, 0);
        // The row is still pending for the next run.
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(1) FROM seen_mails \
             WHERE mail_id NOT LIKE 'mid:%' \
             AND (rfc_message_id IS NULL OR unsubscribe_checked = 0)",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(pending, 1);
    }

    /// A mid-pass interruption at the loop level: the first (partial) run
    /// repairs a prefix, the resumed run fetches exactly the remainder — the
    /// fetcher for the second run does not even know the repaired ids.
    #[tokio::test]
    async fn backfill_loop_resumes_after_partial_run() {
        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;
        sqlx::query(
            "INSERT INTO seen_mails (mail_id, rfc_message_id) VALUES \
             ('g1', NULL), ('g2', NULL)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        let run_id = ingest::create_run(&mut conn, "backfill", None)
            .await
            .unwrap();

        // First run dies after g1: simulate by a fetcher that fails on g2.
        let (tx, rx) = mpsc::channel(16);
        let writer = task::spawn(ingest::backfill_writer(conn, rx, run_id));
        let partial = fabricated_fetcher(vec![FakeMail {
            list_unsubscribe: Some("<https://a.example/u?tok=1>"),
            internal_date: Some(100),
            ..fake_mail("g1", Some("<m1@example.com>"), "Alice <a@example.com>")
        }]);
        let mut read_conn = SqliteConnection::connect_with(&options).await.unwrap();
        let err = backfill_scan_rows(&mut read_conn, &tx, 1, &partial, &AtomicBool::new(false))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("fetching metadata"), "{err:#}");
        drop(tx);
        writer.await.unwrap().unwrap();
        assert_eq!(checked_of(&options, "g1").await, 1);

        // The resumed run fetches only g2.
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let run_id = ingest::create_run(&mut conn, "backfill", None)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(16);
        let writer = task::spawn(ingest::backfill_writer(conn, rx, run_id));
        let rest = fabricated_fetcher(vec![fake_mail(
            "g2",
            Some("<m2@example.com>"),
            "Bob <b@example.com>",
        )]);
        backfill_scan_rows(&mut read_conn, &tx, 1, &rest, &AtomicBool::new(false))
            .await
            .unwrap();
        drop(tx);
        let outcome = writer.await.unwrap().unwrap();
        assert_eq!(outcome.examined, 1);
        assert_eq!(outcome.already_done, 0);
        assert_eq!(
            unsubscribe_url(&options, "a@example.com").await.as_deref(),
            Some("https://a.example/u?tok=1"),
            "the partial run's capture is durable"
        );
        assert_eq!(checked_of(&options, "g2").await, 1);
    }

    // --- live scan capture (issue #41 Phase U2): the scan's per-message core
    //     with a fabricated fetcher, no network involved ---

    /// Newly listed messages get their unsubscribe headers captured and their
    /// seen rows born checked; messages without targets are checked without
    /// creating records; already-seen messages are never fetched at all (the
    /// fetcher would error on them).
    #[tokio::test]
    async fn scan_captures_unsubscribe_for_newly_seen_messages() {
        let dir = tempfile::tempdir().unwrap();
        let options = db_connect_options(&dir.path().join("stats.db"));
        let mut conn = migrated_conn(&options).await;
        // g0 was seen by an earlier scan (pre-U2: unchecked) — the seen check
        // must skip it before any fetch happens.
        sqlx::query("INSERT INTO seen_mails (mail_id) VALUES ('g0')")
            .execute(&mut conn)
            .await
            .unwrap();
        let run_id = ingest::create_run(&mut conn, "gmail_api", None)
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel::<WriteMsg>(16);
        let writer = task::spawn(ingest::db_writer(conn, rx, run_id));

        let pool = SqlitePoolOptions::new()
            .max_connections(2)
            .connect_with(options.clone())
            .await
            .unwrap();
        let fetch = fabricated_fetcher(vec![
            FakeMail {
                list_unsubscribe: Some(
                    "<https://news.example/u?tok=1>, <mailto:stop@news.example>",
                ),
                list_unsubscribe_post: Some("List-Unsubscribe=One-Click"),
                internal_date: Some(1_700_000_000_000),
                ..fake_mail("g1", Some("<m1@example.com>"), "Alice <a@example.com>")
            },
            fake_mail("g2", Some("<m2@example.com>"), "Bob <b@example.com>"),
        ]);
        let listed = ["g0", "g1", "g2"]
            .into_iter()
            .map(|id| Message {
                id: Some(id.to_string()),
                ..Default::default()
            })
            .collect();
        fetch_listed_messages(&pool, &tx, 2, &fetch, &AtomicBool::new(false), listed)
            .await
            .unwrap();
        drop(tx);
        writer.await.unwrap().unwrap();
        pool.close().await;

        // Counts: only the two newly seen messages.
        assert_eq!(sender_count(&options, "a@example.com").await, Some(1));
        assert_eq!(sender_count(&options, "b@example.com").await, Some(1));
        // Alice's capture landed with its internalDate; Bob has no record.
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let row: (Option<String>, Option<String>, i64, Option<i64>) = sqlx::query_as(
            "SELECT https_url, mailto, one_click, source_internal_date \
             FROM sender_unsubscribe WHERE sender = 'a@example.com'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.0.as_deref(), Some("https://news.example/u?tok=1"));
        assert_eq!(row.1.as_deref(), Some("mailto:stop@news.example"));
        assert_eq!(row.2, 1);
        assert_eq!(row.3, Some(1_700_000_000_000));
        assert_eq!(unsubscribe_url(&options, "b@example.com").await, None);
        // Newly scanned rows are born checked; the old row stays pending for
        // the backfill.
        assert_eq!(checked_of(&options, "g1").await, 1);
        assert_eq!(checked_of(&options, "g2").await, 1);
        assert_eq!(checked_of(&options, "g0").await, 0);
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
