#!/usr/bin/env python3
"""Headless-Chrome harness for the Phase C front-end behaviors (issue #30).

Serves the real web/ assets from a stub server that fakes the viewer API,
drives Chrome headless through five scenarios, and asserts both sides:

- what the page rendered (extracted from --dump-dom via an injected
  tests/frontend/test_driver.js that writes a <pre id="harness-results">), and
- what requests the page actually made (the stub records every POST, so the
  CSRF header / JSON content type / body are asserted on the wire).

Scenarios:
  launch      onboarding launch buttons POST /api/runs with the CSRF header
  demo        /api/status 404s (static hosting): no ingestion UI, no POSTs
  owns_false  active run not owned: Cancel/Log hidden, terminal hint shown
  owns_true   owned run: Log renders ring-buffer lines, Cancel POSTs w/ CSRF
  auth_good   accounts.google.com auth_url renders a safe link
  auth_evil   any other auth_url renders as inert text (no link)
  mixed_unrepaired  mixed_sources: banner names the backfill command + count
  mixed_repaired    repaired database: no banner

Run:  python3 tests/frontend/harness.py
Requires Google Chrome (path override: CHROME env var). Exits non-zero on any
failure. Not wired into cargo test — run it alongside.
"""

import html
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
WEB = ROOT / "web"
DRIVER = Path(__file__).resolve().parent / "test_driver.js"

TOKEN = "f" * 64  # stands in for the viewer's per-process CSRF token

CHROME_CANDIDATES = [
    os.environ.get("CHROME"),
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/google-chrome",
    "/usr/bin/chromium-browser",
    "/usr/bin/chromium",
    shutil.which("google-chrome"),
    shutil.which("chromium"),
]


def find_chrome():
    for candidate in CHROME_CANDIDATES:
        if candidate and Path(candidate).exists():
            return candidate
    sys.exit("harness: no Chrome found (set CHROME=/path/to/chrome)")


def base_status(**overrides):
    doc = {
        "db": "ready",
        "now_unix": int(time.time()),
        "ingest_lock_held": False,
        "active_run": None,
        "last_run": None,
        "owns_active_run": False,
        "mixed_sources": False,
        "unrepaired_count": 0,
        "rate_per_sec": None,
        "eta_seconds": None,
        "csrf_token": TOKEN,
    }
    doc.update(overrides)
    return doc


def active_run(**overrides):
    run = {
        "run_id": 7,
        "source": "gmail_api",
        "state": "running",
        "pid": 4242,
        "started_at_unix": int(time.time()) - 30,
        "updated_at_unix": int(time.time()),
        "finished_at_unix": None,
        "messages_seen": 120,
        "messages_new": 100,
        "total_estimate": 1000,
        "bytes_total": None,
        "bytes_done": None,
        "mbox_path": None,
        "error_kind": None,
        "error": None,
        "auth_url": None,
    }
    run.update(overrides)
    return run


SUMMARY = {
    "total_messages": 5,
    "senders": [{"sender": "a@example.com", "mails_sent": 5}],
    "generated_at_unix": int(time.time()),
}

SCENARIOS = {
    "launch": {
        "status": base_status(db="missing"),
        "summary_missing": True,
    },
    "demo": {
        "status_404": True,
    },
    "owns_false": {
        "status": base_status(active_run=active_run(), ingest_lock_held=True),
    },
    "owns_true": {
        "status": base_status(
            active_run=active_run(), ingest_lock_held=True, owns_active_run=True
        ),
        "log_lines": ["stub stderr line 1", "stub stderr line 2", "stub stderr line 3"],
    },
    "auth_good": {
        "status": base_status(
            active_run=active_run(
                state="awaiting_auth",
                auth_url="https://accounts.google.com/o/oauth2/v2/auth?client_id=x",
            ),
            ingest_lock_held=True,
            owns_active_run=True,
        ),
    },
    "auth_evil": {
        "status": base_status(
            active_run=active_run(
                state="awaiting_auth",
                auth_url="https://accounts.google.com.evil.example/phish",
            ),
            ingest_lock_held=True,
            owns_active_run=True,
        ),
    },
    "mixed_unrepaired": {
        "status": base_status(mixed_sources=True, unrepaired_count=1234),
    },
    "mixed_repaired": {
        "status": base_status(),
    },
}


def make_handler(name, scenario, captured):
    index_html = (WEB / "index.html").read_text()
    anchor = '<script src="/app.js" defer></script>'
    assert index_html.count(anchor) == 1
    index_html = index_html.replace(
        anchor, anchor + f'\n<script src="/test.js?scenario={name}" defer></script>'
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_body(self, status, content_type, body):
            data = body.encode() if isinstance(body, str) else body
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            # Fidelity with the real viewer: same CSP, so the injected driver
            # proves same-origin scripts (and nothing else) run.
            self.send_header("Content-Security-Policy", "default-src 'self'")
            self.end_headers()
            self.wfile.write(data)

        def send_json(self, payload, status=200):
            self.send_body(status, "application/json", json.dumps(payload))

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/":
                self.send_body(200, "text/html; charset=utf-8", index_html)
            elif path == "/app.css":
                self.send_body(200, "text/css", (WEB / "app.css").read_text())
            elif path == "/app.js":
                self.send_body(200, "text/javascript", (WEB / "app.js").read_text())
            elif path == "/test.js":
                self.send_body(200, "text/javascript", DRIVER.read_text())
            elif path == "/api/status":
                if scenario.get("status_404"):
                    self.send_body(404, "text/plain", "not found")
                else:
                    self.send_json(scenario["status"])
            elif path == "/api/summary":
                if scenario.get("summary_missing"):
                    self.send_json({"error": "missing_db", "message": "no db"}, 503)
                else:
                    self.send_json(SUMMARY)
            elif path == "/api/runs":
                self.send_json({"runs": [], "now_unix": int(time.time())})
            elif re.fullmatch(r"/api/runs/\d+/log", path):
                self.send_json(
                    {"run_id": 7, "lines": scenario.get("log_lines", []), "exited": False}
                )
            else:
                self.send_body(404, "text/plain", "not found")

        def do_POST(self):
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length).decode("utf-8", "replace")
            captured.append(
                {
                    "path": self.path,
                    "content_type": self.headers.get("Content-Type"),
                    "csrf": self.headers.get("X-Gmail-Stats-Csrf"),
                    "sec_fetch_site": self.headers.get("Sec-Fetch-Site"),
                    "body": body,
                }
            )
            if re.fullmatch(r"/api/runs/\d+/cancel", self.path):
                self.send_json({"run_id": 7, "cancelling": True}, 202)
            elif self.path == "/api/runs":
                self.send_json({"run_id": 9}, 202)
            else:
                self.send_body(404, "text/plain", "not found")

    return Handler


def run_scenario(chrome, name, scenario):
    captured = []
    server = ThreadingHTTPServer(
        ("127.0.0.1", 0), make_handler(name, scenario, captured)
    )
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with tempfile.TemporaryDirectory() as profile:
            # Some Chrome builds print the DOM but never exit afterwards, so
            # collect stdout with a hard deadline and keep whatever arrived.
            proc = subprocess.Popen(
                [
                    chrome,
                    "--headless=new",
                    "--disable-gpu",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-component-update",
                    "--no-first-run",
                    "--no-default-browser-check",
                    f"--user-data-dir={profile}",
                    "--virtual-time-budget=10000",
                    "--dump-dom",
                    f"http://127.0.0.1:{port}/",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                stdout, stderr = proc.communicate(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, stderr = proc.communicate()
        match = re.search(r'<pre id="harness-results">(.*?)</pre>', stdout, re.S)
        if not match:
            raise AssertionError(
                f"no harness results in DOM (chrome rc={proc.returncode}); "
                f"stderr tail: {stderr[-400:]}"
            )
        results = json.loads(html.unescape(match.group(1)))
        if "error" in results:
            raise AssertionError(f"driver error: {results['error']}")
        return results, captured
    finally:
        server.shutdown()


def check(condition, message, failures):
    status = "ok" if condition else "FAIL"
    print(f"    [{status}] {message}")
    if not condition:
        failures.append(message)


def main():
    chrome = find_chrome()
    failures = []

    for name, scenario in SCENARIOS.items():
        print(f"scenario: {name}")
        results, captured = run_scenario(chrome, name, scenario)

        if name == "launch":
            posts = [c for c in captured if c["path"] == "/api/runs"]
            check(len(posts) == 2, f"two launch POSTs captured (got {len(posts)})", failures)
            for post in posts:
                check(
                    (post["content_type"] or "").startswith("application/json"),
                    f"POST content type is JSON ({post['content_type']})",
                    failures,
                )
                check(post["csrf"] == TOKEN, "POST carries the CSRF token from /api/status", failures)
            bodies = sorted(post["body"] for post in posts)
            check(
                bodies
                == sorted(
                    [
                        json.dumps({"source": "mbox", "path": "/tmp/harness.mbox"}, separators=(",", ":")),
                        json.dumps({"source": "gmail_api"}, separators=(",", ":")),
                    ]
                ),
                f"POST bodies are exactly the two launches ({bodies})",
                failures,
            )
        elif name == "demo":
            check(results["allLaunchHidden"], "launch controls hidden without a status endpoint", failures)
            check(results["pillHidden"], "pill hidden", failures)
            check(results["panelHidden"], "panel hidden", failures)
            check(results["historyHidden"], "history hidden", failures)
            check(len(captured) == 0, f"no mutating requests fired ({len(captured)})", failures)
        elif name == "owns_false":
            check(results["cancelHidden"], "Cancel hidden for non-owned run", failures)
            check(results["logHidden"], "Log hidden for non-owned run", failures)
            check("terminal" in results["origin"], "terminal hint shown", failures)
            check(len(captured) == 0, "no POSTs fired", failures)
        elif name == "owns_true":
            check(not results["cancelHidden"], "Cancel visible for owned run", failures)
            check(not results["logHidden"], "Log visible for owned run", failures)
            check("stub stderr line 1" in results["logText"], "log tail rendered", failures)
            cancels = [c for c in captured if c["path"] == "/api/runs/7/cancel"]
            check(len(cancels) == 1, f"one cancel POST captured (got {len(cancels)})", failures)
            if cancels:
                check(cancels[0]["csrf"] == TOKEN, "cancel POST carries the CSRF token", failures)
                check(
                    (cancels[0]["content_type"] or "").startswith("application/json"),
                    "cancel POST content type is JSON",
                    failures,
                )
        elif name == "auth_good":
            check(results["hasLink"], "accounts.google.com URL renders a link", failures)
            check(
                (results["href"] or "").startswith("https://accounts.google.com/"),
                "href is the Google URL",
                failures,
            )
            check(results["target"] == "_blank", "target=_blank", failures)
            check(results["rel"] == "noopener noreferrer", "rel=noopener noreferrer", failures)
        elif name == "auth_evil":
            check(not results["hasLink"], "hostile auth_url renders no link", failures)
            check(
                "accounts.google.com.evil.example" in results["text"],
                "hostile URL shown as inert text",
                failures,
            )
        elif name == "mixed_unrepaired":
            check(not results["bannerHidden"], "mixed banner visible when unrepaired", failures)
            check(
                results["command"] == "cargo run -- scan --backfill-message-ids",
                f"banner names the exact backfill command ({results['command']!r})",
                failures,
            )
            check(
                "1,234" in results["text"] or "1234" in results["text"],
                "banner mentions the unrepaired count",
                failures,
            )
            check(
                "duplicate" in results["text"].lower(),
                "banner explains cross-source duplicates",
                failures,
            )
        elif name == "mixed_repaired":
            check(results["bannerHidden"], "mixed banner absent when repaired", failures)

    print()
    if failures:
        print(f"FAILED: {len(failures)} assertion(s)")
        for failure in failures:
            print(f"  - {failure}")
        sys.exit(1)
    print("all front-end harness scenarios passed")


if __name__ == "__main__":
    main()
