//! Ingestion coordination shared by the Gmail API scan and the Takeout mbox
//! importer: schema migrations, the cross-process ingest lock, `ingest_runs`
//! bookkeeping, and the single DB writer task.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use futures::TryStreamExt;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Connection, Row, Sqlite, SqliteConnection, SqliteExecutor, Transaction};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

/// The schema version this binary knows how to produce, tracked in
/// `PRAGMA user_version`. The pre-versioning tables (`seen_mails`, `senders`)
/// are the version-0 baseline; migration 1 adds `ingest_runs`; migration 2
/// adds `seen_mails.rfc_message_id` for cross-source dedupe (issue #26
/// Phase D).
pub const SCHEMA_VERSION: i64 = 2;

pub fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Bring the database up to [`SCHEMA_VERSION`].
///
/// The baseline (pre-versioning) schema is applied with guarded, idempotent
/// DDL exactly as before schema versioning existed, so an old database — or a
/// brand-new file — needs no manual steps. Versioned migrations then run one
/// by one inside transactions, each bumping `PRAGMA user_version`, and are
/// guarded so they never run twice and a database from a *newer* binary is
/// refused instead of mangled.
pub async fn migrate(conn: &mut SqliteConnection) -> anyhow::Result<()> {
    // Baseline: create the original tables if they don't exist yet and enforce
    // uniqueness of seen_mails.mail_id so that replaying a message (e.g. after
    // a mid-run retry) is idempotent instead of double-counting. Pre-existing
    // duplicate rows from earlier versions are collapsed before the unique
    // index is created.
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

    let version: i64 = sqlx::query("PRAGMA user_version")
        .fetch_one(&mut *conn)
        .await?
        .try_get(0)?;
    anyhow::ensure!(
        version <= SCHEMA_VERSION,
        "database schema is version {version}, newer than this binary supports \
         ({SCHEMA_VERSION}); upgrade gmail_stats"
    );

    if version < 1 {
        // Migration 1: the ingest_runs table (one row per scan/import run).
        let mut tx = conn.begin().await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ingest_runs (
               run_id           INTEGER PRIMARY KEY AUTOINCREMENT,
               source           TEXT NOT NULL,
               state            TEXT NOT NULL,
               pid              INTEGER,
               started_at_unix  INTEGER NOT NULL,
               updated_at_unix  INTEGER NOT NULL,
               finished_at_unix INTEGER,
               messages_seen    INTEGER NOT NULL DEFAULT 0,
               messages_new     INTEGER NOT NULL DEFAULT 0,
               total_estimate   INTEGER,
               bytes_total      INTEGER,
               bytes_done       INTEGER,
               resume_token     TEXT,
               mbox_path        TEXT,
               mbox_fingerprint TEXT,
               error_kind       TEXT,
               error            TEXT,
               auth_url         TEXT
             )",
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query("PRAGMA user_version = 1")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }

    if version < 2 {
        // Migration 2: the rfc_message_id column that unifies the two
        // seen_mails keyspaces (bare Gmail ids from API scans, `mid:`-prefixed
        // RFC Message-IDs from mbox imports). Nullable — NULL means "not
        // known yet" for scan rows (repaired by `scan --backfill-message-ids`)
        // and stays NULL forever for messages that genuinely have no
        // Message-ID. Non-unique index: duplicate Message-IDs are data, the
        // dedupe check just needs the lookup to be cheap.
        //
        // `mid:` rows embed their Message-ID in the key itself, so they are
        // populated right here with the same normalization the ingesters use;
        // only bare Gmail rows need the API backfill afterwards.
        let mut tx = conn.begin().await?;
        sqlx::query("ALTER TABLE seen_mails ADD COLUMN rfc_message_id TEXT")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS seen_mails_rfc_message_id \
             ON seen_mails (rfc_message_id)",
        )
        .execute(&mut *tx)
        .await?;
        loop {
            let batch: Vec<String> = sqlx::query_scalar(
                "SELECT mail_id FROM seen_mails \
                 WHERE mail_id LIKE 'mid:%' AND rfc_message_id IS NULL LIMIT 10000",
            )
            .fetch_all(&mut *tx)
            .await?;
            if batch.is_empty() {
                break;
            }
            for mail_id in batch {
                let normalized = normalize_rfc_message_id(&mail_id["mid:".len()..]);
                // A mid: key whose embedded id normalizes to nothing would be
                // unreachable by the dedupe check either way; the empty-string
                // marker below just keeps this loop from revisiting the row.
                sqlx::query("UPDATE seen_mails SET rfc_message_id = ? WHERE mail_id = ?")
                    .bind(normalized.unwrap_or_default())
                    .bind(&mail_id)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        sqlx::query("PRAGMA user_version = 2")
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
    }

    Ok(())
}

/// Normalize an RFC 5322 Message-ID for cross-source comparison: trim
/// whitespace, strip one pair of enclosing angle brackets, trim again.
/// Case is preserved (the left-hand side of a Message-ID is case-sensitive).
/// Returns None when nothing usable remains — such messages are never deduped
/// across sources.
///
/// This is the single shared normalization used by the API scanner, the mbox
/// importer, the migration that populates `mid:` rows, and the backfill.
pub fn normalize_rfc_message_id(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let inner = trimmed
        .strip_prefix('<')
        .and_then(|s| s.strip_suffix('>'))
        .unwrap_or(trimmed)
        .trim();
    if inner.is_empty() {
        None
    } else {
        Some(inner.to_string())
    }
}

/// Where the ingest lockfile for a given database lives: right next to it.
pub fn ingest_lock_path(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push(".ingest.lock");
    PathBuf::from(name)
}

/// Held for the whole life of the process; the kernel releases the flock when
/// the process exits, however it dies, so there are no stale locks.
#[derive(Debug)]
pub struct IngestLock {
    _file: std::fs::File,
}

/// Take an exclusive kernel `flock` on the lockfile next to the database.
/// This is the authoritative "one ingester per database" gate: a second
/// scanner or importer (from a terminal or, later, spawned by the web viewer)
/// fails here promptly, before touching the database.
pub fn acquire_ingest_lock(db_path: &Path) -> anyhow::Result<IngestLock> {
    let lock_path = ingest_lock_path(db_path);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("opening ingest lockfile {}", lock_path.display()))?;
    match rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive) {
        Ok(()) => Ok(IngestLock { _file: file }),
        Err(rustix::io::Errno::WOULDBLOCK) => anyhow::bail!(
            "another ingester is already running against {} \
             (could not acquire exclusive lock on {}); wait for it to finish or stop it first",
            db_path.display(),
            lock_path.display()
        ),
        Err(e) => Err(anyhow::Error::new(e).context(format!("locking {}", lock_path.display()))),
    }
}

/// Mark any run rows left open by a dead ingester as abandoned. Callers hold
/// the ingest lock, so an open row cannot belong to a live process: liveness
/// is judged by the flock, never by heartbeats or pids.
pub async fn abandon_stale_runs(conn: &mut SqliteConnection) -> anyhow::Result<u64> {
    let now = now_unix();
    let result = sqlx::query(
        "UPDATE ingest_runs SET state = 'abandoned', finished_at_unix = ?, updated_at_unix = ? \
         WHERE state NOT IN ('done', 'failed', 'cancelled', 'abandoned')",
    )
    .bind(now)
    .bind(now)
    .execute(conn)
    .await?;
    Ok(result.rows_affected())
}

/// mbox-specific details recorded on an import run's row.
pub struct MboxRunInfo<'a> {
    pub path: &'a Path,
    pub fingerprint: &'a str,
    pub bytes_total: u64,
    pub start_offset: u64,
}

/// Insert the row for a new run and return its run_id.
pub async fn create_run(
    conn: &mut SqliteConnection,
    source: &str,
    mbox: Option<MboxRunInfo<'_>>,
) -> anyhow::Result<i64> {
    let now = now_unix();
    let (path, fingerprint, bytes_total, bytes_done) = match &mbox {
        Some(info) => (
            Some(info.path.display().to_string()),
            Some(info.fingerprint.to_string()),
            Some(info.bytes_total as i64),
            Some(info.start_offset as i64),
        ),
        None => (None, None, None, None),
    };
    let result = sqlx::query(
        "INSERT INTO ingest_runs \
         (source, state, pid, started_at_unix, updated_at_unix, \
          bytes_total, bytes_done, mbox_path, mbox_fingerprint) \
         VALUES (?, 'running', ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(source)
    .bind(std::process::id() as i64)
    .bind(now)
    .bind(now)
    .bind(bytes_total)
    .bind(bytes_done)
    .bind(path)
    .bind(fingerprint)
    .execute(conn)
    .await?;
    Ok(result.last_insert_rowid())
}

/// Write a run's terminal state. Opens its own short-lived connection: this
/// runs strictly after the writer task has drained and exited (we hold the
/// ingest lock, so no other writer exists), including on error paths where
/// the writer's connection is no longer available.
pub async fn finish_run(
    options: &SqliteConnectOptions,
    run_id: i64,
    state: &str,
    error_kind: Option<&str>,
    error: Option<&str>,
) -> anyhow::Result<()> {
    let mut conn = SqliteConnection::connect_with(options).await?;
    let now = now_unix();
    sqlx::query(
        "UPDATE ingest_runs SET state = ?, finished_at_unix = ?, updated_at_unix = ?, \
         error_kind = ?, error = ? WHERE run_id = ?",
    )
    .bind(state)
    .bind(now)
    .bind(now)
    .bind(error_kind)
    .bind(error)
    .bind(run_id)
    .execute(&mut conn)
    .await?;
    conn.close().await.ok();
    Ok(())
}

/// A fetched/parsed message's result, sent to the single DB writer task.
pub struct SeenMail {
    pub message_id: String,
    pub sender: String,
    /// The message's RFC 5322 Message-ID, already normalized via
    /// [`normalize_rfc_message_id`]. None when the message has none — such
    /// messages keep the pre-Phase-D behavior (counted, NULL column).
    pub rfc_message_id: Option<String>,
}

/// Everything that flows over the writer channel; all durable writes ride
/// through here so single-writer discipline holds.
pub enum WriteMsg {
    Seen(SeenMail),
    Progress {
        messages_seen: u64,
        /// Import only: committed parse offset (doubles as the resume point).
        /// Sent after the Seen messages it covers — the channel is FIFO, so by
        /// the time it is applied those messages are committed.
        bytes_done: Option<u64>,
        /// Scan only: last fully-processed page token.
        resume_token: Option<String>,
        /// Scan only: the mailbox's total message count, once known.
        total_estimate: Option<i64>,
    },
    /// Scan only: OAuth consent progress. The InstalledFlow delegate sets
    /// state='awaiting_auth' plus the consent URL when Google asks for the
    /// user; once tokens arrive the scanner sets state back to 'running' and
    /// clears the URL. Rides the writer channel like every other durable
    /// write, so single-writer discipline holds.
    AuthState {
        state: &'static str,
        auth_url: Option<String>,
    },
}

/// The single DB writer: receives results over the channel and commits them
/// one transaction at a time on its own dedicated connection.
///
/// The insert-and-count pair is idempotent per message id: the sender counter
/// (and the run's messages_new) is only incremented when the INSERT OR IGNORE
/// actually inserts the row, so a message that slips through a read-side seen
/// check twice is still counted exactly once. Cross-source dedupe happens in
/// the same transaction: a newly inserted row whose normalized Message-ID
/// already exists on any other seen_mails row (the other keyspace having
/// ingested the same message) is recorded as seen but not counted — neither
/// the sender nor the run's messages_new moves. Between messages the writer
/// heartbeats the run row (~2s) so watchers can distinguish "slow" from
/// "stalled".
pub async fn db_writer(
    mut conn: SqliteConnection,
    mut rx: mpsc::Receiver<WriteMsg>,
    run_id: i64,
) -> anyhow::Result<()> {
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                None => break,
                Some(WriteMsg::Seen(mail)) => {
                    let mut tx = conn.begin().await?;
                    let newly_seen = mark_seen(&mail, &mut *tx).await?;
                    if newly_seen {
                        let cross_source_dup = match mail.rfc_message_id.as_deref() {
                            Some(rfc_id) => {
                                rfc_id_exists_elsewhere(rfc_id, &mail.message_id, &mut *tx).await?
                            }
                            None => false,
                        };
                        if !cross_source_dup {
                            increment_sender_mails(&mail.sender, &mut tx).await?;
                            sqlx::query(
                                "UPDATE ingest_runs SET messages_new = messages_new + 1, \
                                 updated_at_unix = ? WHERE run_id = ?",
                            )
                            .bind(now_unix())
                            .bind(run_id)
                            .execute(&mut *tx)
                            .await?;
                        }
                    }
                    tx.commit().await?;
                }
                Some(WriteMsg::Progress { messages_seen, bytes_done, resume_token, total_estimate }) => {
                    sqlx::query(
                        "UPDATE ingest_runs SET messages_seen = ?, \
                         bytes_done = COALESCE(?, bytes_done), \
                         resume_token = COALESCE(?, resume_token), \
                         total_estimate = COALESCE(?, total_estimate), \
                         updated_at_unix = ? WHERE run_id = ?",
                    )
                    .bind(messages_seen as i64)
                    .bind(bytes_done.map(|b| b as i64))
                    .bind(resume_token)
                    .bind(total_estimate)
                    .bind(now_unix())
                    .bind(run_id)
                    .execute(&mut conn)
                    .await?;
                }
                Some(WriteMsg::AuthState { state, auth_url }) => {
                    sqlx::query(
                        "UPDATE ingest_runs SET state = ?, auth_url = ?, \
                         updated_at_unix = ? WHERE run_id = ?",
                    )
                    .bind(state)
                    .bind(auth_url)
                    .bind(now_unix())
                    .bind(run_id)
                    .execute(&mut conn)
                    .await?;
                }
            },
            _ = heartbeat.tick() => {
                sqlx::query("UPDATE ingest_runs SET updated_at_unix = ? WHERE run_id = ?")
                    .bind(now_unix())
                    .bind(run_id)
                    .execute(&mut conn)
                    .await?;
            }
        }
    }
    Ok(())
}

pub async fn seen_mail(
    message_id: &str,
    executor: impl SqliteExecutor<'_>,
) -> anyhow::Result<bool> {
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

/// Record a message id as seen (with its normalized Message-ID when known).
/// Returns whether the id was newly inserted; false means it was already
/// present and must not be counted again.
async fn mark_seen(mail: &SeenMail, executor: impl SqliteExecutor<'_>) -> anyhow::Result<bool> {
    let result =
        sqlx::query("INSERT OR IGNORE INTO seen_mails (mail_id, rfc_message_id) VALUES (?, ?)")
            .bind(&mail.message_id)
            .bind(&mail.rfc_message_id)
            .execute(executor)
            .await?;
    Ok(result.rows_affected() > 0)
}

/// Whether a normalized Message-ID is already recorded on any seen_mails row
/// other than `own_mail_id`. This is the cross-source duplicate check, run in
/// both directions: a scan finding an mbox-imported message and an import
/// finding a scanned one both land here.
async fn rfc_id_exists_elsewhere(
    rfc_id: &str,
    own_mail_id: &str,
    executor: impl SqliteExecutor<'_>,
) -> anyhow::Result<bool> {
    let exists: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM seen_mails WHERE rfc_message_id = ? AND mail_id <> ?)",
    )
    .bind(rfc_id)
    .bind(own_mail_id)
    .fetch_one(executor)
    .await?;
    Ok(exists != 0)
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

// ---------------------------------------------------------------------------
// `scan --backfill-message-ids`: populate rfc_message_id on historical API
// scan rows and collapse the cross-source double counts that accumulated
// while sources were mixed without it.
// ---------------------------------------------------------------------------

/// One backfilled scan row, produced by the metadata fetch loop and sent to
/// the single backfill writer task.
pub struct BackfillMsg {
    /// The bare Gmail id of the seen_mails row being repaired.
    pub mail_id: String,
    /// The fetched message's Message-ID, already normalized via
    /// [`normalize_rfc_message_id`]; None when the message has none.
    pub rfc_message_id: Option<String>,
    /// The sender extracted from the fetched message, cleaned exactly like
    /// the scan does — this is whose count is decremented when the row turns
    /// out to be a cross-source duplicate.
    pub sender: String,
}

/// What the backfill writer did, for the run summary.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct BackfillOutcome {
    /// Rows received from the fetch loop.
    pub examined: u64,
    /// Rows whose rfc_message_id was populated with a real Message-ID.
    pub populated: u64,
    /// Rows fetched but found to have no Message-ID header; marked with the
    /// empty-string sentinel so a re-run does not fetch them again.
    pub missing_id: u64,
    /// Duplicate pairs collapsed: sender counts decremented once each.
    pub decremented: u64,
    /// Rows that were already repaired when their result arrived (replays);
    /// always 0 in a single clean run.
    pub already_done: u64,
}

/// The single DB writer for a backfill run: applies each fetched row in its
/// own transaction and heartbeats the run row like [`db_writer`]. Returns a
/// summary of what it changed.
///
/// Per-row logic (the collapse fused with the populate, so partial completion
/// plus a re-run is always exact):
/// - a row that is no longer NULL is skipped — populate-and-decrement already
///   happened, replaying the message is a no-op;
/// - otherwise the fetched Message-ID (or the '' sentinel for "fetched, has
///   none") is stored, and if the normalized id already exists on any *other*
///   row — the mbox side of a historical double count, or an earlier repaired
///   scan row with the same Message-ID — the sender's count is decremented
///   once, never below zero.
///
/// Because the decision is committed atomically with the populate, a run
/// killed anywhere leaves every row either fully repaired (populated and, if
/// it was a duplicate, decremented) or untouched (still NULL, picked up by
/// the next run). A second run over a repaired database finds no NULL rows
/// and does nothing.
pub async fn backfill_writer(
    mut conn: SqliteConnection,
    mut rx: mpsc::Receiver<BackfillMsg>,
    run_id: i64,
) -> anyhow::Result<BackfillOutcome> {
    let mut outcome = BackfillOutcome::default();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(2));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                None => break,
                Some(msg) => {
                    let mut tx = conn.begin().await?;
                    apply_backfill(&mut tx, &msg, &mut outcome).await?;
                    sqlx::query(
                        "UPDATE ingest_runs SET messages_seen = ?, messages_new = ?, \
                         updated_at_unix = ? WHERE run_id = ?",
                    )
                    .bind(outcome.examined as i64)
                    .bind((outcome.populated + outcome.missing_id) as i64)
                    .bind(now_unix())
                    .bind(run_id)
                    .execute(&mut *tx)
                    .await?;
                    tx.commit().await?;
                }
            },
            _ = heartbeat.tick() => {
                sqlx::query("UPDATE ingest_runs SET updated_at_unix = ? WHERE run_id = ?")
                    .bind(now_unix())
                    .bind(run_id)
                    .execute(&mut conn)
                    .await?;
            }
        }
    }
    Ok(outcome)
}

async fn apply_backfill(
    tx: &mut Transaction<'_, Sqlite>,
    msg: &BackfillMsg,
    outcome: &mut BackfillOutcome,
) -> anyhow::Result<()> {
    outcome.examined += 1;
    let still_null: i64 = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM seen_mails \
         WHERE mail_id = ? AND rfc_message_id IS NULL)",
    )
    .bind(&msg.mail_id)
    .fetch_one(&mut **tx)
    .await?;
    if still_null == 0 {
        outcome.already_done += 1;
        return Ok(());
    }
    match msg.rfc_message_id.as_deref() {
        None => {
            // '' sentinel: fetched, genuinely has no Message-ID. Never
            // matched by the dedupe check (which only binds non-empty
            // normalized ids), but no longer NULL, so re-runs skip it.
            sqlx::query("UPDATE seen_mails SET rfc_message_id = '' WHERE mail_id = ?")
                .bind(&msg.mail_id)
                .execute(&mut **tx)
                .await?;
            outcome.missing_id += 1;
        }
        Some(rfc_id) => {
            let duplicate = rfc_id_exists_elsewhere(rfc_id, &msg.mail_id, &mut **tx).await?;
            sqlx::query("UPDATE seen_mails SET rfc_message_id = ? WHERE mail_id = ?")
                .bind(rfc_id)
                .bind(&msg.mail_id)
                .execute(&mut **tx)
                .await?;
            outcome.populated += 1;
            if duplicate {
                // This message was counted once by the scan (this row) and
                // once by the other row's ingester: collapse exactly one of
                // the two, clamped at zero in case the counts were already
                // edited out from under us.
                sqlx::query(
                    "UPDATE senders SET mails_sent = mails_sent - 1 \
                     WHERE sender = ? AND mails_sent > 0",
                )
                .bind(&msg.sender)
                .execute(&mut **tx)
                .await?;
                outcome.decremented += 1;
            }
        }
    }
    Ok(())
}

/// Fingerprint an mbox file for resume validation: size, mtime, and an
/// FNV-1a hash of the first 64 KiB. Cheap to compute, and catches both
/// truncation/replacement (size, prefix) and in-place edits (mtime) without
/// hashing a multi-GB file.
pub fn mbox_fingerprint(path: &Path) -> anyhow::Result<String> {
    use std::io::Read;
    let meta =
        std::fs::metadata(path).with_context(|| format!("cannot stat {}", path.display()))?;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut file =
        std::fs::File::open(path).with_context(|| format!("cannot open {}", path.display()))?;
    let mut buf = vec![0u8; 64 * 1024];
    let mut filled = 0;
    loop {
        let n = file.read(&mut buf[filled..])?;
        if n == 0 {
            break;
        }
        filled += n;
        if filled == buf.len() {
            break;
        }
    }
    Ok(format!(
        "{}:{}:{:016x}",
        meta.len(),
        mtime,
        fnv1a(&buf[..filled])
    ))
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::{SqliteJournalMode, SqliteSynchronous};
    use std::io::Write;
    use std::str::FromStr;

    async fn memory_conn() -> SqliteConnection {
        let options = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        SqliteConnection::connect_with(&options).await.unwrap()
    }

    fn file_options(path: &Path) -> SqliteConnectOptions {
        SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5))
    }

    async fn table_exists(conn: &mut SqliteConnection, name: &str) -> bool {
        let count: i64 =
            sqlx::query("SELECT count(1) AS ct FROM sqlite_master WHERE type='table' AND name=?")
                .bind(name)
                .fetch_one(conn)
                .await
                .unwrap()
                .try_get("ct")
                .unwrap();
        count > 0
    }

    async fn user_version(conn: &mut SqliteConnection) -> i64 {
        sqlx::query("PRAGMA user_version")
            .fetch_one(conn)
            .await
            .unwrap()
            .try_get(0)
            .unwrap()
    }

    async fn index_exists(conn: &mut SqliteConnection, name: &str) -> bool {
        let count: i64 =
            sqlx::query("SELECT count(1) AS ct FROM sqlite_master WHERE type='index' AND name=?")
                .bind(name)
                .fetch_one(conn)
                .await
                .unwrap()
                .try_get("ct")
                .unwrap();
        count > 0
    }

    async fn rfc_of(conn: &mut SqliteConnection, mail_id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT rfc_message_id FROM seen_mails WHERE mail_id = ?")
            .bind(mail_id)
            .fetch_one(conn)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn migrate_initializes_a_fresh_database() {
        let mut conn = memory_conn().await;
        migrate(&mut conn).await.unwrap();
        assert!(table_exists(&mut conn, "seen_mails").await);
        assert!(table_exists(&mut conn, "senders").await);
        assert!(table_exists(&mut conn, "ingest_runs").await);
        assert!(index_exists(&mut conn, "seen_mails_rfc_message_id").await);
        assert_eq!(user_version(&mut conn).await, SCHEMA_VERSION);
        // The column is usable on a fresh database.
        sqlx::query("INSERT INTO seen_mails (mail_id, rfc_message_id) VALUES ('g1', 'a@b')")
            .execute(&mut conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn migrate_upgrades_a_version_0_database_with_existing_data() {
        let mut conn = memory_conn().await;
        // Simulate a pre-versioning database: baseline tables, some data,
        // user_version still 0, no ingest_runs.
        sqlx::query("CREATE TABLE seen_mails (mail_id string)")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE senders (sender string, mails_sent int)")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO seen_mails (mail_id) VALUES ('m1'), ('m1'), ('m2')")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("INSERT INTO senders (sender, mails_sent) VALUES ('a@example.com', 7)")
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(user_version(&mut conn).await, 0);

        migrate(&mut conn).await.unwrap();

        assert_eq!(user_version(&mut conn).await, SCHEMA_VERSION);
        assert!(table_exists(&mut conn, "ingest_runs").await);
        assert!(index_exists(&mut conn, "seen_mails_rfc_message_id").await);
        // Existing data survives; pre-index duplicates are collapsed; the
        // new column defaults to NULL ("Message-ID not known yet").
        let seen: i64 = sqlx::query("SELECT count(1) AS ct FROM seen_mails")
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .try_get("ct")
            .unwrap();
        assert_eq!(seen, 2);
        assert_eq!(rfc_of(&mut conn, "m1").await, None);
        let mails: i64 = sqlx::query("SELECT mails_sent FROM senders WHERE sender='a@example.com'")
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .try_get("mails_sent")
            .unwrap();
        assert_eq!(mails, 7);

        // Running again is a no-op.
        migrate(&mut conn).await.unwrap();
        assert_eq!(user_version(&mut conn).await, SCHEMA_VERSION);
    }

    /// A version-1 database (Phases A-C): migration 2 adds the column and
    /// index, and populates `mid:` rows from the Message-ID embedded in their
    /// key — only bare Gmail rows are left NULL for the API backfill.
    #[tokio::test]
    async fn migrate_upgrades_a_version_1_database_and_populates_mid_rows() {
        let mut conn = memory_conn().await;
        migrate(&mut conn).await.unwrap();
        // Rewind to version 1: drop the Phase-D column by rebuilding the
        // table the way a v1 binary left it.
        sqlx::query("DROP TABLE seen_mails")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE seen_mails (mail_id string)")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query("CREATE UNIQUE INDEX seen_mails_mail_id_unique ON seen_mails (mail_id)")
            .execute(&mut conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO seen_mails (mail_id) VALUES \
             ('gmailid1'), ('mid:<m1@example.com>'), ('mid: <m2@example.com> '), ('mid:')",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("PRAGMA user_version = 1")
            .execute(&mut conn)
            .await
            .unwrap();

        migrate(&mut conn).await.unwrap();

        assert_eq!(user_version(&mut conn).await, SCHEMA_VERSION);
        assert!(index_exists(&mut conn, "seen_mails_rfc_message_id").await);
        // Scan rows stay NULL (repaired later by the backfill)...
        assert_eq!(rfc_of(&mut conn, "gmailid1").await, None);
        // ...mid: rows are populated with the shared normalization...
        assert_eq!(
            rfc_of(&mut conn, "mid:<m1@example.com>").await.as_deref(),
            Some("m1@example.com")
        );
        assert_eq!(
            rfc_of(&mut conn, "mid: <m2@example.com> ").await.as_deref(),
            Some("m2@example.com")
        );
        // ...and a degenerate empty embedded id gets the '' marker, not NULL.
        assert_eq!(rfc_of(&mut conn, "mid:").await.as_deref(), Some(""));

        // Running again is a no-op.
        migrate(&mut conn).await.unwrap();
        assert_eq!(user_version(&mut conn).await, SCHEMA_VERSION);
    }

    #[test]
    fn normalize_rfc_message_id_variants() {
        for raw in [
            "<id@host>",
            "id@host",
            "  <id@host>  ",
            "\t<id@host>\r\n",
            "< id@host >",
            "  id@host  ",
        ] {
            assert_eq!(
                normalize_rfc_message_id(raw).as_deref(),
                Some("id@host"),
                "failed for {raw:?}"
            );
        }
        // Case is preserved.
        assert_eq!(
            normalize_rfc_message_id("<ID@Host>").as_deref(),
            Some("ID@Host")
        );
        // Only one enclosing pair is stripped, and unbalanced brackets stay.
        assert_eq!(
            normalize_rfc_message_id("<<id@host>>").as_deref(),
            Some("<id@host>")
        );
        assert_eq!(
            normalize_rfc_message_id("<id@host").as_deref(),
            Some("<id@host")
        );
        // Nothing usable.
        assert_eq!(normalize_rfc_message_id(""), None);
        assert_eq!(normalize_rfc_message_id("   "), None);
        assert_eq!(normalize_rfc_message_id("<>"), None);
        assert_eq!(normalize_rfc_message_id(" < > "), None);
    }

    #[tokio::test]
    async fn migrate_refuses_a_newer_database() {
        let mut conn = memory_conn().await;
        sqlx::query("PRAGMA user_version = 99")
            .execute(&mut conn)
            .await
            .unwrap();
        let err = migrate(&mut conn).await.unwrap_err();
        assert!(err.to_string().contains("newer"), "unexpected error: {err}");
    }

    #[test]
    fn second_ingester_is_refused_by_the_flock() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("stats.db");
        let first = acquire_ingest_lock(&db).expect("first lock");
        let err = acquire_ingest_lock(&db).expect_err("second lock must fail");
        assert!(
            err.to_string()
                .contains("another ingester is already running"),
            "unexpected error: {err}"
        );
        drop(first);
        // Once the first ingester is gone the lock is free again.
        acquire_ingest_lock(&db).expect("lock after release");
    }

    #[tokio::test]
    async fn run_row_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        migrate(&mut conn).await.unwrap();

        let run_id = create_run(&mut conn, "gmail_api", None).await.unwrap();
        let row = sqlx::query("SELECT state, pid, source FROM ingest_runs WHERE run_id = ?")
            .bind(run_id)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.try_get::<String, _>("state").unwrap(), "running");
        assert_eq!(row.try_get::<String, _>("source").unwrap(), "gmail_api");
        assert_eq!(
            row.try_get::<i64, _>("pid").unwrap(),
            std::process::id() as i64
        );

        // A later ingester (holding the freed lock) marks the open row abandoned.
        let abandoned = abandon_stale_runs(&mut conn).await.unwrap();
        assert_eq!(abandoned, 1);
        let state: String = sqlx::query("SELECT state FROM ingest_runs WHERE run_id = ?")
            .bind(run_id)
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .try_get("state")
            .unwrap();
        assert_eq!(state, "abandoned");

        // Closed rows are left alone.
        finish_run(&options, run_id, "failed", Some("io"), Some("boom"))
            .await
            .unwrap();
        assert_eq!(abandon_stale_runs(&mut conn).await.unwrap(), 0);
        let row = sqlx::query(
            "SELECT state, error_kind, error, finished_at_unix FROM ingest_runs WHERE run_id = ?",
        )
        .bind(run_id)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.try_get::<String, _>("state").unwrap(), "failed");
        assert_eq!(row.try_get::<String, _>("error_kind").unwrap(), "io");
        assert_eq!(row.try_get::<String, _>("error").unwrap(), "boom");
        assert!(row.try_get::<i64, _>("finished_at_unix").unwrap() > 0);
    }

    #[tokio::test]
    async fn db_writer_dedupes_and_tracks_progress() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        migrate(&mut conn).await.unwrap();
        let run_id = create_run(&mut conn, "mbox", None).await.unwrap();

        let (tx, rx) = mpsc::channel(16);
        let writer = tokio::spawn(db_writer(conn, rx, run_id));
        for _ in 0..2 {
            tx.send(WriteMsg::Seen(SeenMail {
                message_id: "mid:<m1@example.com>".into(),
                sender: "a@example.com".into(),
                rfc_message_id: Some("m1@example.com".into()),
            }))
            .await
            .unwrap();
        }
        tx.send(WriteMsg::Seen(SeenMail {
            message_id: "mid:<m2@example.com>".into(),
            sender: "a@example.com".into(),
            rfc_message_id: Some("m2@example.com".into()),
        }))
        .await
        .unwrap();
        tx.send(WriteMsg::Progress {
            messages_seen: 3,
            bytes_done: Some(4096),
            resume_token: None,
            total_estimate: None,
        })
        .await
        .unwrap();
        drop(tx);
        writer.await.unwrap().unwrap();

        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let row = sqlx::query(
            "SELECT messages_seen, messages_new, bytes_done FROM ingest_runs WHERE run_id = ?",
        )
        .bind(run_id)
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(row.try_get::<i64, _>("messages_seen").unwrap(), 3);
        assert_eq!(row.try_get::<i64, _>("messages_new").unwrap(), 2);
        assert_eq!(row.try_get::<i64, _>("bytes_done").unwrap(), 4096);
        let mails: i64 = sqlx::query("SELECT mails_sent FROM senders WHERE sender='a@example.com'")
            .fetch_one(&mut conn)
            .await
            .unwrap()
            .try_get("mails_sent")
            .unwrap();
        assert_eq!(mails, 2);
    }

    #[tokio::test]
    async fn db_writer_applies_auth_state_transitions() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        migrate(&mut conn).await.unwrap();
        let run_id = create_run(&mut conn, "gmail_api", None).await.unwrap();

        let (tx, rx) = mpsc::channel(4);
        let writer = tokio::spawn(db_writer(conn, rx, run_id));
        tx.send(WriteMsg::AuthState {
            state: "awaiting_auth",
            auth_url: Some("https://accounts.google.com/o/oauth2/auth?x=1".into()),
        })
        .await
        .unwrap();
        tx.send(WriteMsg::AuthState {
            state: "running",
            auth_url: None,
        })
        .await
        .unwrap();
        drop(tx);
        writer.await.unwrap().unwrap();

        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        let row = sqlx::query("SELECT state, auth_url FROM ingest_runs WHERE run_id = ?")
            .bind(run_id)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.try_get::<String, _>("state").unwrap(), "running");
        assert_eq!(row.try_get::<Option<String>, _>("auth_url").unwrap(), None);
    }

    // --- cross-source dedupe (Phase D) -------------------------------------

    async fn sender_count(conn: &mut SqliteConnection, sender: &str) -> Option<i64> {
        sqlx::query_scalar("SELECT mails_sent FROM senders WHERE sender = ?")
            .bind(sender)
            .fetch_optional(conn)
            .await
            .unwrap()
    }

    async fn seen_count(conn: &mut SqliteConnection) -> i64 {
        sqlx::query_scalar("SELECT count(1) FROM seen_mails")
            .fetch_one(conn)
            .await
            .unwrap()
    }

    fn seen(message_id: &str, sender: &str, rfc: Option<&str>) -> WriteMsg {
        WriteMsg::Seen(SeenMail {
            message_id: message_id.into(),
            sender: sender.into(),
            rfc_message_id: rfc.map(str::to_string),
        })
    }

    /// Run one batch of writer messages as its own ingest run (fresh writer
    /// task, like a separate scan/import invocation) and return messages_new.
    async fn run_writer_batch(
        options: &SqliteConnectOptions,
        source: &str,
        msgs: Vec<WriteMsg>,
    ) -> i64 {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        let run_id = create_run(&mut conn, source, None).await.unwrap();
        let (tx, rx) = mpsc::channel(64);
        let writer = tokio::spawn(db_writer(conn, rx, run_id));
        for msg in msgs {
            tx.send(msg).await.unwrap();
        }
        drop(tx);
        writer.await.unwrap().unwrap();
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        sqlx::query_scalar("SELECT messages_new FROM ingest_runs WHERE run_id = ?")
            .bind(run_id)
            .fetch_one(&mut conn)
            .await
            .unwrap()
    }

    /// Scan first, then import an overlapping mailbox: the shared message is
    /// marked seen by both keyspaces but counted once — sender totals equal
    /// the true union. Messages without a Message-ID keep current behavior.
    #[tokio::test]
    async fn forward_dedupe_scan_then_import() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        migrate(&mut conn).await.unwrap();
        drop(conn);

        // Scan: two messages from Alice (one with no Message-ID), one from Bob.
        let new = run_writer_batch(
            &options,
            "gmail_api",
            vec![
                seen("g1", "a@example.com", Some("m1@example.com")),
                seen("g2", "a@example.com", None),
                seen("g3", "b@example.com", Some("m3@example.com")),
            ],
        )
        .await;
        assert_eq!(new, 3);

        // Import: overlaps on m1 (dup, not counted) and adds m4 (counted).
        // The importer never emits Seen for id-less messages, so no None here.
        let new = run_writer_batch(
            &options,
            "mbox",
            vec![
                seen(
                    "mid:<m1@example.com>",
                    "a@example.com",
                    Some("m1@example.com"),
                ),
                seen(
                    "mid:<m4@example.com>",
                    "c@example.com",
                    Some("m4@example.com"),
                ),
            ],
        )
        .await;
        assert_eq!(new, 1, "the overlapping message must be seen-not-new");

        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        assert_eq!(sender_count(&mut conn, "a@example.com").await, Some(2));
        assert_eq!(sender_count(&mut conn, "b@example.com").await, Some(1));
        assert_eq!(sender_count(&mut conn, "c@example.com").await, Some(1));
        // Idempotency intact: the duplicate's seen row exists in both keyspaces.
        assert_eq!(seen_count(&mut conn).await, 5);

        // Replaying the import adds nothing.
        let new = run_writer_batch(
            &options,
            "mbox",
            vec![seen(
                "mid:<m1@example.com>",
                "a@example.com",
                Some("m1@example.com"),
            )],
        )
        .await;
        assert_eq!(new, 0);
        assert_eq!(sender_count(&mut conn, "a@example.com").await, Some(2));
    }

    /// The other direction: import first, then scan the same mailbox.
    #[tokio::test]
    async fn forward_dedupe_import_then_scan() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        migrate(&mut conn).await.unwrap();
        drop(conn);

        let new = run_writer_batch(
            &options,
            "mbox",
            vec![
                seen(
                    "mid:<m1@example.com>",
                    "a@example.com",
                    Some("m1@example.com"),
                ),
                seen(
                    "mid:<m2@example.com>",
                    "b@example.com",
                    Some("m2@example.com"),
                ),
            ],
        )
        .await;
        assert_eq!(new, 2);

        // Scan sees the same two messages plus one new id-less one.
        let new = run_writer_batch(
            &options,
            "gmail_api",
            vec![
                seen("g1", "a@example.com", Some("m1@example.com")),
                seen("g2", "b@example.com", Some("m2@example.com")),
                seen("g3", "b@example.com", None),
            ],
        )
        .await;
        assert_eq!(new, 1, "only the id-less message is new");

        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        assert_eq!(sender_count(&mut conn, "a@example.com").await, Some(1));
        assert_eq!(sender_count(&mut conn, "b@example.com").await, Some(2));
        assert_eq!(seen_count(&mut conn).await, 5);
    }

    /// Fixture for backfill tests: a database that historically mixed sources
    /// without rfc_message_id. Scan rows g1/g2 (NULL rfc) and mbox rows for
    /// m1/m2, with senders double-counted accordingly.
    async fn double_counted_fixture(options: &SqliteConnectOptions) {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        migrate(&mut conn).await.unwrap();
        sqlx::query(
            "INSERT INTO seen_mails (mail_id, rfc_message_id) VALUES \
             ('g1', NULL), \
             ('g2', NULL), \
             ('g3', NULL), \
             ('mid:<m1@example.com>', 'm1@example.com'), \
             ('mid:<m2@example.com>', 'm2@example.com')",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        // Alice sent m1 (counted by scan AND import = 2) plus the id-less g3
        // (counted once): recorded 3, true count 2. Bob sent m2 (counted
        // twice): recorded 2, true count 1.
        sqlx::query(
            "INSERT INTO senders (sender, mails_sent) VALUES \
             ('a@example.com', 3), ('b@example.com', 2)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }

    fn backfill(mail_id: &str, rfc: Option<&str>, sender: &str) -> BackfillMsg {
        BackfillMsg {
            mail_id: mail_id.into(),
            rfc_message_id: rfc.map(str::to_string),
            sender: sender.into(),
        }
    }

    async fn run_backfill_batch(
        options: &SqliteConnectOptions,
        msgs: Vec<BackfillMsg>,
    ) -> BackfillOutcome {
        let mut conn = SqliteConnection::connect_with(options).await.unwrap();
        let run_id = create_run(&mut conn, "backfill", None).await.unwrap();
        let (tx, rx) = mpsc::channel(64);
        let writer = tokio::spawn(backfill_writer(conn, rx, run_id));
        for msg in msgs {
            tx.send(msg).await.unwrap();
        }
        drop(tx);
        writer.await.unwrap().unwrap()
    }

    /// The repair pass: populates scan rows, collapses each historical
    /// double count exactly once, marks id-less rows with the '' sentinel,
    /// and a second run finds nothing to do.
    #[tokio::test]
    async fn backfill_collapses_historical_double_counts_exactly_once() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        double_counted_fixture(&options).await;

        let outcome = run_backfill_batch(
            &options,
            vec![
                backfill("g1", Some("m1@example.com"), "a@example.com"),
                backfill("g2", Some("m2@example.com"), "b@example.com"),
                backfill("g3", None, "a@example.com"),
            ],
        )
        .await;
        assert_eq!(
            outcome,
            BackfillOutcome {
                examined: 3,
                populated: 2,
                missing_id: 1,
                decremented: 2,
                already_done: 0,
            }
        );

        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        assert_eq!(sender_count(&mut conn, "a@example.com").await, Some(2));
        assert_eq!(sender_count(&mut conn, "b@example.com").await, Some(1));
        assert_eq!(
            rfc_of(&mut conn, "g1").await.as_deref(),
            Some("m1@example.com")
        );
        assert_eq!(
            rfc_of(&mut conn, "g2").await.as_deref(),
            Some("m2@example.com")
        );
        assert_eq!(rfc_of(&mut conn, "g3").await.as_deref(), Some(""));
        // Nothing is left for a second run to fetch.
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(1) FROM seen_mails \
             WHERE rfc_message_id IS NULL AND mail_id NOT LIKE 'mid:%'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert_eq!(pending, 0);

        // Replaying the very same results (a crashed run's retry) is a no-op:
        // every row is already repaired, no count moves again.
        let outcome = run_backfill_batch(
            &options,
            vec![
                backfill("g1", Some("m1@example.com"), "a@example.com"),
                backfill("g2", Some("m2@example.com"), "b@example.com"),
                backfill("g3", None, "a@example.com"),
            ],
        )
        .await;
        assert_eq!(outcome.already_done, 3);
        assert_eq!(outcome.decremented, 0);
        assert_eq!(sender_count(&mut conn, "a@example.com").await, Some(2));
        assert_eq!(sender_count(&mut conn, "b@example.com").await, Some(1));
    }

    /// A run that dies mid-collapse leaves every processed row fully repaired
    /// and every unprocessed row untouched; the resumed run repairs exactly
    /// the remainder.
    #[tokio::test]
    async fn backfill_interrupted_midway_resumes_exactly() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        double_counted_fixture(&options).await;

        // First run only got through g1 before dying.
        let outcome = run_backfill_batch(
            &options,
            vec![backfill("g1", Some("m1@example.com"), "a@example.com")],
        )
        .await;
        assert_eq!(outcome.decremented, 1);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        assert_eq!(sender_count(&mut conn, "a@example.com").await, Some(2));
        assert_eq!(sender_count(&mut conn, "b@example.com").await, Some(2));
        assert_eq!(rfc_of(&mut conn, "g2").await, None);

        // The next run's NULL-row query finds exactly the remainder.
        let pending: Vec<String> = sqlx::query_scalar(
            "SELECT mail_id FROM seen_mails \
             WHERE rfc_message_id IS NULL AND mail_id NOT LIKE 'mid:%' ORDER BY mail_id",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(pending, vec!["g2".to_string(), "g3".to_string()]);

        let outcome = run_backfill_batch(
            &options,
            vec![
                backfill("g2", Some("m2@example.com"), "b@example.com"),
                backfill("g3", None, "a@example.com"),
            ],
        )
        .await;
        assert_eq!(outcome.decremented, 1);
        assert_eq!(sender_count(&mut conn, "a@example.com").await, Some(2));
        assert_eq!(sender_count(&mut conn, "b@example.com").await, Some(1));
    }

    /// The decrement clamps at zero even when counts were edited externally.
    #[tokio::test]
    async fn backfill_never_decrements_below_zero() {
        let dir = tempfile::tempdir().unwrap();
        let options = file_options(&dir.path().join("stats.db"));
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        migrate(&mut conn).await.unwrap();
        sqlx::query(
            "INSERT INTO seen_mails (mail_id, rfc_message_id) VALUES \
             ('g1', NULL), ('mid:<m1@example.com>', 'm1@example.com')",
        )
        .execute(&mut conn)
        .await
        .unwrap();
        sqlx::query("INSERT INTO senders (sender, mails_sent) VALUES ('a@example.com', 0)")
            .execute(&mut conn)
            .await
            .unwrap();
        drop(conn);

        let outcome = run_backfill_batch(
            &options,
            vec![backfill("g1", Some("m1@example.com"), "a@example.com")],
        )
        .await;
        assert_eq!(outcome.decremented, 1);
        let mut conn = SqliteConnection::connect_with(&options).await.unwrap();
        assert_eq!(
            sender_count(&mut conn, "a@example.com").await,
            Some(0),
            "count must never go below zero"
        );
    }

    #[test]
    fn fingerprint_changes_when_the_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mail.mbox");
        std::fs::write(&path, b"From a@example.com\nFrom: a@example.com\n\nbody\n").unwrap();
        let fp1 = mbox_fingerprint(&path).unwrap();
        let fp2 = mbox_fingerprint(&path).unwrap();
        assert_eq!(fp1, fp2, "fingerprint must be stable for an unchanged file");

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(b"From b@example.com\n\nmore\n").unwrap();
        drop(file);
        let fp3 = mbox_fingerprint(&path).unwrap();
        assert_ne!(fp1, fp3, "fingerprint must change when the file grows");
    }
}
