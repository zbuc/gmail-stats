//! Phase C endpoint + supervision tests for the web viewer (issue #30).
//!
//! Drives the exact router the `web` binary serves (via `gmail_stats::webapp`)
//! with `tower::ServiceExt::oneshot`: the full CSRF matrix on the mutating
//! routes, and child-process supervision paths using the test-only
//! `fake_ingester` binary (normal completion, crash → abandoned, SIGTERM
//! honored, SIGTERM ignored → SIGKILL escalation, viewer restart mid-run).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, HeaderMap, Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use tower::ServiceExt;

use gmail_stats::ingest;
use gmail_stats::webapp::{build_router, AppState};

const FAKE_INGESTER: &str = env!("CARGO_BIN_EXE_fake_ingester");
const REAL_INGESTER: &str = env!("CARGO_BIN_EXE_gmail_stats");

/// A viewer wired to the fake ingester with a short SIGTERM→SIGKILL grace.
fn state_for(db: &Path) -> Arc<AppState> {
    Arc::new(AppState::configured(
        db,
        PathBuf::from(FAKE_INGESTER),
        Duration::from_millis(400),
    ))
}

fn app(state: &Arc<AppState>) -> Router {
    build_router(state.clone())
}

fn set_fake_mode(dir: &Path, mode: &str) {
    std::fs::write(dir.join("fake_mode"), mode).unwrap();
}

fn write_mbox(dir: &Path) -> PathBuf {
    let path = dir.join("mail.mbox");
    std::fs::write(
        &path,
        "From a@example.com Thu Jan  1 00:00:00 2020\n\
         From: Alice <a@example.com>\n\
         Message-ID: <m1@example.com>\n\
         \n\
         body\n",
    )
    .unwrap();
    path
}

async fn read_json(response: axum::response::Response) -> (StatusCode, Value, HeaderMap) {
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value, headers)
}

fn assert_no_cors(headers: &HeaderMap, context: &str) {
    for name in [
        "access-control-allow-origin",
        "access-control-allow-methods",
        "access-control-allow-headers",
        "access-control-allow-credentials",
        "access-control-expose-headers",
    ] {
        assert!(
            headers.get(name).is_none(),
            "{context}: must never emit CORS header {name}"
        );
    }
}

async fn get_json(state: &Arc<AppState>, uri: &str) -> (StatusCode, Value, HeaderMap) {
    let response = app(state)
        .oneshot(
            Request::builder()
                .uri(uri)
                .header(header::HOST, "127.0.0.1:7878")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    read_json(response).await
}

async fn csrf_token(state: &Arc<AppState>) -> String {
    let (status, body, _) = get_json(state, "/api/status").await;
    assert_eq!(status, StatusCode::OK);
    body["csrf_token"].as_str().unwrap().to_string()
}

struct PostSpec<'a> {
    uri: &'a str,
    content_type: Option<&'a str>,
    token: Option<&'a str>,
    origin: Option<&'a str>,
    sec_fetch_site: Option<&'a str>,
    body: &'a str,
}

async fn post(state: &Arc<AppState>, spec: PostSpec<'_>) -> (StatusCode, Value, HeaderMap) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(spec.uri)
        .header(header::HOST, "127.0.0.1:7878");
    if let Some(content_type) = spec.content_type {
        builder = builder.header(header::CONTENT_TYPE, content_type);
    }
    if let Some(token) = spec.token {
        builder = builder.header("x-gmail-stats-csrf", token);
    }
    if let Some(origin) = spec.origin {
        builder = builder.header(header::ORIGIN, origin);
    }
    if let Some(site) = spec.sec_fetch_site {
        builder = builder.header("sec-fetch-site", site);
    }
    let response = app(state)
        .oneshot(builder.body(Body::from(spec.body.to_string())).unwrap())
        .await
        .unwrap();
    read_json(response).await
}

/// A fully well-formed same-origin browser request (the "valid trio":
/// JSON content type + correct token + benign browser-context headers).
async fn post_valid(
    state: &Arc<AppState>,
    uri: &str,
    body: Value,
) -> (StatusCode, Value, HeaderMap) {
    let token = csrf_token(state).await;
    post(
        state,
        PostSpec {
            uri,
            content_type: Some("application/json"),
            token: Some(&token),
            origin: Some("http://127.0.0.1:7878"),
            sec_fetch_site: Some("same-origin"),
            body: &body.to_string(),
        },
    )
    .await
}

/// Poll /api/status until `pred` says stop, or the deadline passes.
async fn wait_status<F>(state: &Arc<AppState>, mut pred: F, what: &str) -> Value
where
    F: FnMut(&Value) -> bool,
{
    for _ in 0..200 {
        let (status, body, _) = get_json(state, "/api/status").await;
        if status == StatusCode::OK && pred(&body) {
            return body;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for: {what}");
}

async fn run_state(state: &Arc<AppState>, run_id: i64) -> Option<String> {
    let (_, body, _) = get_json(state, "/api/runs?limit=50").await;
    body["runs"]
        .as_array()?
        .iter()
        .find(|run| run["run_id"].as_i64() == Some(run_id))
        .and_then(|run| run["state"].as_str().map(str::to_string))
}

// ---------------------------------------------------------------------------
// CSRF matrix on POST /api/runs and POST /api/runs/{id}/cancel
// ---------------------------------------------------------------------------

#[tokio::test]
async fn csrf_matrix_rejects_everything_short_of_the_full_stack() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    let state = state_for(&db);
    let token = csrf_token(&state).await;
    let good_body = json!({ "source": "mbox", "path": mbox }).to_string();

    for uri in ["/api/runs", "/api/runs/1/cancel"] {
        // Form content types (what a cross-site <form> can produce): 415,
        // even with an otherwise-valid token.
        for form_ct in [
            "application/x-www-form-urlencoded",
            "multipart/form-data",
            "text/plain",
        ] {
            let (status, body, headers) = post(
                &state,
                PostSpec {
                    uri,
                    content_type: Some(form_ct),
                    token: Some(&token),
                    origin: None,
                    sec_fetch_site: None,
                    body: &good_body,
                },
            )
            .await;
            assert_eq!(
                status,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{uri} with {form_ct}"
            );
            assert_eq!(body["error"], "content_type");
            assert_no_cors(&headers, uri);
        }

        // Missing content type entirely: also 415.
        let (status, _, headers) = post(
            &state,
            PostSpec {
                uri,
                content_type: None,
                token: Some(&token),
                origin: None,
                sec_fetch_site: None,
                body: &good_body,
            },
        )
        .await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{uri} no CT");
        assert_no_cors(&headers, uri);

        // JSON but no CSRF header: 403.
        let (status, body, headers) = post(
            &state,
            PostSpec {
                uri,
                content_type: Some("application/json"),
                token: None,
                origin: None,
                sec_fetch_site: None,
                body: &good_body,
            },
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri} without token");
        assert_eq!(body["error"], "csrf");
        assert_no_cors(&headers, uri);

        // Wrong token: 403.
        let (status, _, _) = post(
            &state,
            PostSpec {
                uri,
                content_type: Some("application/json"),
                token: Some("deadbeef"),
                origin: None,
                sec_fetch_site: None,
                body: &good_body,
            },
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri} wrong token");

        // Hostile Origins: 403 even with a valid token.
        for origin in [
            "https://evil.example",
            "http://evil.example",
            "https://127.0.0.1:7878",
            "null",
        ] {
            let (status, _, _) = post(
                &state,
                PostSpec {
                    uri,
                    content_type: Some("application/json"),
                    token: Some(&token),
                    origin: Some(origin),
                    sec_fetch_site: None,
                    body: &good_body,
                },
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{uri} origin {origin}");
        }

        // Cross-site fetch metadata: 403 even with a valid token.
        for site in ["cross-site", "same-site"] {
            let (status, _, _) = post(
                &state,
                PostSpec {
                    uri,
                    content_type: Some("application/json"),
                    token: Some(&token),
                    origin: None,
                    sec_fetch_site: Some(site),
                    body: &good_body,
                },
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{uri} sec-fetch-site {site}");
        }
    }

    // Benign loopback origins pass the origin layer (proven end-to-end by
    // the valid-trio 202 below and cancel's 409-not-owned, which are both
    // past-CSRF outcomes).
    let (status, body, _) = post(
        &state,
        PostSpec {
            uri: "/api/runs/1/cancel",
            content_type: Some("application/json"),
            token: Some(&token),
            origin: Some("http://localhost:7878"),
            sec_fetch_site: Some("none"),
            body: "{}",
        },
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "not_owned");
}

#[tokio::test]
async fn valid_trio_starts_a_run_and_completes_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "complete");
    let state = state_for(&db);

    let (status, body, headers) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "valid trio must 202: {body}");
    let run_id = body["run_id"].as_i64().expect("202 body carries run_id");
    assert_no_cors(&headers, "202 response");

    let final_status = wait_status(
        &state,
        |s| {
            s["active_run"].is_null()
                && s["last_run"]["run_id"].as_i64() == Some(run_id)
                && s["last_run"]["state"] == "done"
        },
        "spawned run to complete",
    )
    .await;
    assert_eq!(final_status["ingest_lock_held"], false);
}

#[tokio::test]
async fn bad_paths_and_sources_are_422() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let state = state_for(&db);

    let not_mbox = dir.path().join("notes.txt");
    std::fs::write(&not_mbox, "definitely not mail\n").unwrap();

    let cases: Vec<(Value, &str)> = vec![
        (
            json!({ "source": "mbox", "path": "relative/mail.mbox" }),
            "bad_path",
        ),
        (
            json!({ "source": "mbox", "path": dir.path().join("missing.mbox") }),
            "bad_path",
        ),
        (json!({ "source": "mbox", "path": dir.path() }), "bad_path"),
        (json!({ "source": "mbox", "path": not_mbox }), "bad_path"),
        (json!({ "source": "mbox" }), "bad_path"),
        (
            json!({ "source": "mbox", "resume_run_id": 42 }),
            "bad_resume",
        ),
        (
            json!({ "source": "gmail_api", "resume_run_id": 42 }),
            "bad_resume",
        ),
        (json!({ "source": "carrier_pigeon" }), "bad_source"),
        (json!({}), "bad_source"),
    ];
    for (body, expected_kind) in cases {
        let (status, response, _) = post_valid(&state, "/api/runs", body.clone()).await;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "body {body} => {response}"
        );
        assert_eq!(response["error"], expected_kind, "body {body}");
    }

    // Malformed JSON with the right headers: 400, not a spawn.
    let token = csrf_token(&state).await;
    let (status, _, _) = post(
        &state,
        PostSpec {
            uri: "/api/runs",
            content_type: Some("application/json"),
            token: Some(&token),
            origin: None,
            sec_fetch_site: None,
            body: "not json",
        },
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn second_start_while_active_is_409_and_cancel_is_honored() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "hang_trap");
    let state = state_for(&db);

    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let run_id = body["run_id"].as_i64().unwrap();

    // Owned and visibly active.
    let status_doc = wait_status(
        &state,
        |s| s["active_run"]["run_id"].as_i64() == Some(run_id),
        "run visible as active",
    )
    .await;
    assert_eq!(status_doc["owns_active_run"], true);
    assert_eq!(status_doc["ingest_lock_held"], true);

    // A second launch while the first holds the flock: 409 run_active.
    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "run_active");

    // Cancel: 202, SIGTERM honored, clean `cancelled` row.
    let (status, body, _) =
        post_valid(&state, &format!("/api/runs/{run_id}/cancel"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    wait_status(
        &state,
        |s| s["active_run"].is_null() && s["ingest_lock_held"] == false,
        "cancelled child to exit",
    )
    .await;
    assert_eq!(
        run_state(&state, run_id).await.as_deref(),
        Some("cancelled")
    );
}

#[tokio::test]
async fn cancelling_a_run_this_viewer_did_not_start_is_409_not_owned() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let state = state_for(&db);

    // Plant a foreign active run (as if started from a terminal).
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(&db)
        .create_if_missing(true);
    let mut conn = <sqlx::SqliteConnection as sqlx::Connection>::connect_with(&options)
        .await
        .unwrap();
    ingest::migrate(&mut conn).await.unwrap();
    let run_id = ingest::create_run(&mut conn, "gmail_api", None)
        .await
        .unwrap();

    let (status, body, _) =
        post_valid(&state, &format!("/api/runs/{run_id}/cancel"), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "not_owned");

    // And the log of a non-owned run is equally unavailable.
    let (status, body, _) = get_json(&state, &format!("/api/runs/{run_id}/log")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_owned");
}

// ---------------------------------------------------------------------------
// Supervision: crash → abandoned, SIGKILL escalation, restart, log ring
// ---------------------------------------------------------------------------

/// Run the real ingester (import) once, to completion; its startup janitor
/// marks any stale open rows abandoned.
fn run_real_import(db: &Path, mbox: &Path) {
    let output = std::process::Command::new(REAL_INGESTER)
        .arg("import")
        .arg(mbox)
        .arg("--quiet")
        .arg("--db")
        .arg(db)
        .output()
        .expect("running the real ingester");
    assert!(
        output.status.success(),
        "real ingester failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn crashed_child_leaves_an_open_row_that_the_next_ingester_abandons() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "crash");
    let state = state_for(&db);

    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let run_id = body["run_id"].as_i64().unwrap();

    // The crash frees the flock but leaves the row open ('running').
    wait_status(
        &state,
        |s| s["ingest_lock_held"] == false,
        "crashed child to release the flock",
    )
    .await;
    assert_eq!(run_state(&state, run_id).await.as_deref(), Some("running"));

    // The next (real) ingester's janitor marks it abandoned.
    set_fake_mode(dir.path(), "complete"); // irrelevant to the real binary
    run_real_import(&db, &mbox);
    assert_eq!(
        run_state(&state, run_id).await.as_deref(),
        Some("abandoned")
    );
}

#[tokio::test]
async fn sigterm_ignoring_child_is_sigkilled_and_later_abandoned() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "hang_ignore");
    let state = state_for(&db); // 400ms SIGTERM→SIGKILL grace

    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let run_id = body["run_id"].as_i64().unwrap();

    let (status, _, _) = post_valid(&state, &format!("/api/runs/{run_id}/cancel"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // SIGTERM is ignored; after the grace the viewer SIGKILLs it, which the
    // kernel proves by releasing the flock. The row stays open (the child
    // never got to write a terminal state)...
    wait_status(
        &state,
        |s| s["ingest_lock_held"] == false,
        "SIGKILL escalation to reap the child",
    )
    .await;
    assert_eq!(run_state(&state, run_id).await.as_deref(), Some("running"));

    // ...until the next ingester's janitor marks it abandoned.
    run_real_import(&db, &mbox);
    assert_eq!(
        run_state(&state, run_id).await.as_deref(),
        Some("abandoned")
    );
}

#[tokio::test]
async fn viewer_restart_loses_ownership_but_not_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "hang_trap");
    let old_viewer = state_for(&db);

    let (status, body, _) = post_valid(
        &old_viewer,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let run_id = body["run_id"].as_i64().unwrap();

    // A "restarted" viewer: fresh state over the same database. The child
    // keeps running (kill_on_drop=false, still holds the flock), but the new
    // viewer does not own it: no cancel, no log.
    let new_viewer = state_for(&db);
    let status_doc = wait_status(
        &new_viewer,
        |s| s["active_run"]["run_id"].as_i64() == Some(run_id),
        "restarted viewer to see the run",
    )
    .await;
    assert_eq!(status_doc["owns_active_run"], false);
    assert_eq!(status_doc["ingest_lock_held"], true);

    let (status, body, _) = post_valid(
        &new_viewer,
        &format!("/api/runs/{run_id}/cancel"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "not_owned");
    let (status, _, _) = get_json(&new_viewer, &format!("/api/runs/{run_id}/log")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The original viewer still owns it and can clean up.
    let (status, _, _) = post_valid(
        &old_viewer,
        &format!("/api/runs/{run_id}/cancel"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    wait_status(
        &old_viewer,
        |s| s["active_run"].is_null() && s["ingest_lock_held"] == false,
        "cancelled child to exit",
    )
    .await;
    assert_eq!(
        run_state(&old_viewer, run_id).await.as_deref(),
        Some("cancelled")
    );
}

#[tokio::test]
async fn log_endpoint_serves_the_last_200_stderr_lines_of_owned_runs() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "stderr"); // emits 250 lines, then hangs
    let state = state_for(&db);

    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    let run_id = body["run_id"].as_i64().unwrap();

    // Wait until the ring has filled to its 200-line cap.
    let mut lines = Vec::new();
    for _ in 0..100 {
        let (status, body, _) = get_json(&state, &format!("/api/runs/{run_id}/log")).await;
        assert_eq!(status, StatusCode::OK);
        lines = body["lines"]
            .as_array()
            .unwrap()
            .iter()
            .map(|line| line.as_str().unwrap().to_string())
            .collect();
        if lines
            .last()
            .is_some_and(|last| last == "fake stderr line 250")
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(lines.len(), 200, "ring buffer is capped at 200 lines");
    assert_eq!(lines.first().unwrap(), "fake stderr line 51");
    assert_eq!(lines.last().unwrap(), "fake stderr line 250");

    let (status, _, _) = post_valid(&state, &format!("/api/runs/{run_id}/cancel"), json!({})).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    wait_status(&state, |s| s["active_run"].is_null(), "child to exit").await;
}

#[tokio::test]
async fn missing_ingester_binary_is_spawn_failed_with_a_build_hint() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    let state = Arc::new(AppState::configured(
        &db,
        dir.path().join("no-such-binary"),
        Duration::from_millis(400),
    ));

    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"], "spawn_failed");
    assert!(
        body["message"].as_str().unwrap().contains("cargo build"),
        "spawn_failed must hint at cargo build: {body}"
    );
}

#[tokio::test]
async fn a_child_that_dies_before_reporting_a_run_is_spawn_failed() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "norow");
    let state = state_for(&db);

    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(body["error"], "spawn_failed");
    assert!(
        body["message"].as_str().unwrap().contains("norow"),
        "spawn_failed should carry the child's stderr tail: {body}"
    );
}

#[tokio::test]
async fn a_terminal_launched_ingester_beats_the_viewer_to_the_flock() {
    // A terminal-started ingester holds the flock (no run row needed): the
    // viewer's friendly pre-check turns the launch into 409 run_active.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "complete");
    let state = state_for(&db);

    let _terminal_lock = ingest::acquire_ingest_lock(&db).unwrap();
    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "run_active");
}

#[tokio::test]
async fn losing_the_flock_race_after_the_precheck_is_still_409() {
    // The TOCTOU window: the pre-check passes (lock free), but the child
    // loses the authoritative flock race and exits with the lock message.
    // The viewer maps that to 409 run_active, not spawn_failed.
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stats.db");
    let mbox = write_mbox(dir.path());
    set_fake_mode(dir.path(), "flockfail");
    let state = state_for(&db);

    let (status, body, _) = post_valid(
        &state,
        "/api/runs",
        json!({ "source": "mbox", "path": mbox }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "run_active");
}
