//! Thin entry point for the local web viewer. The whole application lives in
//! `gmail_stats::webapp` so integration tests can drive the exact router the
//! binary serves.
//!
//! Usage: `cargo run --bin web [port]` (default port 7878), then open the
//! printed URL. Run from the repo root so `./stats.db` resolves, or point
//! GMAIL_STATS_DB at the database.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    gmail_stats::webapp::serve().await
}
