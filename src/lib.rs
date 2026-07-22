//! Shared library surface for the gmail_stats binaries.
//!
//! The scanner/importer binary (`src/main.rs`) and the web viewer
//! (`src/bin/web.rs`) both need the ingestion coordination primitives —
//! lockfile naming, the `ingest_runs` schema, migrations — so they live here
//! rather than being duplicated. The viewer only ever uses them read-only
//! (plus a non-destructive flock probe); all writes stay in the ingester.

pub mod ingest;
pub mod mbox;
pub mod webapp;
