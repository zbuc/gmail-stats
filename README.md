# gmail-stats
Generate stats on your GMail inbox and store in a SQLite database

**[Live demo](https://zbuc.github.io/gmail-stats/)** — a static demo of the web viewer with synthetic data for a fictional account (no real inbox involved).

# What?
My GMail inbox was a mess and there's no easy built-in way to view things like who the most frequent senders of mail are.

I found an add-on, [Mailstrom](https://mailstrom.co/) however it was a commercial product and also required granting OAuth
access to manage my entire inbox, which I didn't feel comfortable with.

So, I spent a few hours and hacked this together. The code's not great, it's slow and very basic, but it works.

# How?

There are two ways to get your mail into the database. Both fill the same
tables and use the same sender normalization, so the resulting stats look the
same either way:

| | **Gmail API scan** (default) | **Takeout mbox import** |
|---|---|---|
| Command | `cargo run` | `cargo run -- import path/to/All-mail.mbox` |
| Needs | A Google Cloud OAuth client + browser consent | A Google Takeout export file |
| Network | Yes — fetches every message's headers (slow for big inboxes, rate-limited) | No — purely local, fast |
| Works with Advanced Protection | **No** — Google blocks Gmail OAuth scopes for APP accounts outright (`Error 400: policy_enforced`) | **Yes** |
| Stays current | Re-run any time to pick up new mail | Snapshot as of the export |

Choose the **API scan** if you can OAuth and want to re-scan incrementally;
choose the **Takeout import** if you're enrolled in Advanced Protection, don't
want to create a Cloud project, or already have an export lying around.

Both modes can share one database: messages are deduplicated across sources
by their RFC `Message-ID` header, so scanning and importing overlapping mail
counts each message once. One caveat for databases with scans that predate
this dedupe (the scanner didn't record Message-IDs back then): run the
[backfill repair pass](#repairing-cross-source-duplicates) once to fetch the
missing Message-IDs and collapse any historical double counts. The web viewer
shows a banner with the exact command whenever that applies.

## Option A: Gmail API scan (OAuth)

First follow the Google Workspace [setup instructions](https://developers.google.com/workspace/guides/get-started) to get OAuth
credentials associated with a Google Cloud Project.

You'll need to make sure your OAuth Consent Screen is configured to have the following scopes:

* `https://www.googleapis.com/auth/gmail.readonly`
* `https://www.googleapis.com/auth/gmail.metadata`

As well as adding `http://localhost:8085` to the OAuth client's authorized
redirect URIs. The consent redirect always uses that fixed port so the URI can
be preconfigured; pick a different one with `--oauth-port <PORT>` (or
`GMAIL_STATS_OAUTH_PORT`) and authorize `http://localhost:<PORT>` instead.

After setting up your OAuth credentials, download the client secret file and save it as `credentials.json`.

The application creates the local database tables (and a unique index on
`seen_mails.mail_id`, which keeps retries from double-counting messages)
automatically on startup, so no manual schema setup is needed.

Finally, run the application:

```console
$ cargo run
```

This will trigger an OAuth flow which you launch in your browser, after which the access credentials are stored on disk (in `tokencache.json` by default).
Please be aware that these are credentials that would allow anyone to read the contents of your email inbox, so you probably want to `rm tokencache.json`
after you're done.

A scan can be interrupted at any time with Ctrl-C (or SIGTERM): it drains
pending writes, records its progress, and exits cleanly. Because every message
is deduplicated, simply re-running the scan continues where it makes sense and
never double-counts.

## Option B: Google Takeout mbox import

No Google Cloud project, no OAuth, no network access — and the only option
that works for accounts enrolled in Google's Advanced Protection Program.

1. Go to [takeout.google.com](https://takeout.google.com), deselect
   everything except **Mail**, and request the export. Gmail exports arrive in
   **mbox** format.

   > **Advanced Protection note:** for exactly the accounts that need this
   > path, the export is slow — with Advanced Protection enabled, Google
   > deliberately waits **about 2 days** before delivering a Takeout export.
   > Plan ahead; once downloaded, the import itself is local and fast.

2. Download and extract the archive; you're looking for the large `.mbox`
   file (often named `All mail Including Spam and Trash.mbox`).

3. Import it:

   ```console
   $ cargo run -- import "path/to/All mail Including Spam and Trash.mbox"
   ```

The importer streams the file (multi-GB archives are fine — memory use stays
flat), parses only the message headers, and skips unparseable regions with a
count in the final summary. Re-running the same import adds zero new counts:
messages are deduplicated by their `Message-ID` header.

An interrupted import (Ctrl-C, SIGTERM, crash) can be resumed from where it
stopped — the cancel message prints the exact command, e.g.:

```console
$ cargo run -- import path/to/mail.mbox --resume 3
```

Resume validates that the file hasn't changed (size, mtime, content
fingerprint); if it has, it safely falls back to re-parsing from the start,
which stays correct thanks to the dedupe.

## Unsubscribing from senders

Both ingesters capture each message's `List-Unsubscribe` /
`List-Unsubscribe-Post` headers (RFC 2369 / RFC 8058) and store the latest
valid targets per sender — unsubscribe URLs embed per-recipient tokens, so the
newest message's link is the one most likely to still work. Live API scans
capture this automatically for every newly seen message; re-running an mbox
import over an existing database back-populates the data without changing any
counts; and for messages scanned *before* capture existed, running the
[repair pass](#repairing-cross-source-duplicates) (`scan --backfill`) once
fetches their headers and fills in the history.

In the web viewer, senders with captured data get an **Unsubscribe** control
on their row: a one-click (RFC 8058) sender gets a button that sends the
standard unsubscribe POST from your browser, others get a plain link to the
sender's unsubscribe page or a `mailto:` link. Only `https:` and `mailto:`
targets are ever actionable — anything else is shown as inert text — and the
viewer process itself never contacts any unsubscribe URL; every action is an
explicit click in your browser behind a confirmation naming the exact target.

**Caveat:** using any unsubscribe mechanism confirms to the sender that your
address is live and read. For legitimate newsletters that's fine; for spam or
senders you don't trust, prefer hiding the sender in the viewer or filtering
in Gmail instead of unsubscribing.

## Repairing cross-source duplicates

The scan and the import key `seen_mails` differently (Gmail's internal id vs.
`mid:`-prefixed RFC `Message-ID`), so cross-source dedupe works through a
shared `rfc_message_id` column: both ingesters record each message's
normalized Message-ID and check it before counting, and a message ingested by
both sources is counted exactly once. Messages without any Message-ID are
simply counted per source, as before.

Scans that ran before this feature existed didn't record Message-IDs, so a
database that mixed such scans with an import may hold double counts. Repair
it once with:

```console
$ cargo run -- scan --backfill
```

(`--backfill-message-ids`, the original name, still works as an alias.)

This is a normal ingest run (it takes the same lock, writes a run row, and
shows progress in the web viewer). For every previously scanned message that
still needs it, it fetches just the message's headers over the Gmail API —
`Message-ID` plus the `List-Unsubscribe` headers, using the same OAuth
credentials, rate limiting, and retries as a scan — then collapses every
message found in both sources by decrementing the affected sender count once
per duplicate, and records per-sender unsubscribe targets for mail scanned
before capture existed (preferring the newest message's link when it can tell
by date). Each row is marked checked once its headers have been examined, so
the pass is idempotent and resumable: interrupt it any time and re-run the
same command to continue; a fully repaired database finishes immediately
without touching the network. The web viewer's duplicate-counts banner
disappears once the repair is complete.

## Command line

```
gmail_stats [scan] [OPTIONS]           scan Gmail over the API (OAuth; the default)
gmail_stats import <PATH> [OPTIONS]    import a Google Takeout mbox export
gmail_stats scan --backfill [OPTIONS]  fetch Message-ID and List-Unsubscribe headers for
                                       earlier scans, collapse cross-source duplicate
                                       counts, and capture unsubscribe targets (idempotent;
                                       --backfill-message-ids is an accepted alias)

--db <PATH>            SQLite database path        [env: GMAIL_STATS_DB] [default: stats.db]
--credentials <PATH>   OAuth client secret (scan)  [env: GMAIL_STATS_CREDENTIALS] [default: credentials.json]
--tokens <PATH>        OAuth token cache (scan)    [env: GMAIL_STATS_TOKENS] [default: tokencache.json]
--resume <RUN_ID>      import only: resume from the recorded byte offset
--quiet                only errors and the final summary
--verbose              per-message detail (prints every sender)
```

With `cargo run`, pass options after `--`: `cargo run -- import mail.mbox --db elsewhere.db`.

By default output is a periodic progress line; `--verbose` restores the old
per-message `sender: ...` lines (be aware they print inbox metadata to the
terminal), and `--quiet` reduces output to errors and the final summary.

Only one ingester (scan **or** import) can run against a database at a time:
a kernel lock on `<db>.ingest.lock` makes a second one exit immediately with a
clear message. Each run is also recorded in an `ingest_runs` table (state,
message counts, progress, timestamps), which you can inspect with `sqlite3`
and which the web viewer will use for live progress in a later phase of
[#26](https://github.com/zbuc/gmail-stats/issues/26).

## Configuration

Two optional environment variables tune the Gmail API request rate for your
Google Cloud project's per-user quota (scan mode only):

* `GMAIL_STATS_FETCH_CONCURRENCY` — how many `messages.get` calls may be in
  flight at once (default: 8, minimum: 1).
* `GMAIL_STATS_RATE_LIMIT_MS` — minimum spacing between Gmail API calls in
  milliseconds (default: 25, i.e. ~40 requests/sec, minimum: 1).

For example, to run gently against a project with a low quota:

```console
$ GMAIL_STATS_FETCH_CONCURRENCY=2 GMAIL_STATS_RATE_LIMIT_MS=100 cargo run
```

## Viewing the results

When the run finishes, you can view the statistics on senders in the DB:

```console
$ sqlite3 stats.db
sqlite> select count(distinct(mail_id)) from seen_mails;
145096
sqlite> select * from senders order by mails_sent asc;
...
```
