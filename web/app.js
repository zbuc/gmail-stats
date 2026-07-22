"use strict";

// Sender strings come from arbitrary From: headers and are attacker-controlled;
// run error text and mbox paths from /api/status are equally untrusted. This
// page only ever renders dynamic strings via textContent — never innerHTML.

const PAGE_SIZES = [25, 50, 100, 500];
const DEFAULT_PAGE_SIZE = 50;
const HIDDEN_KEY = "gmail-stats.hiddenSenders";
const PAGE_SIZE_KEY = "gmail-stats.pageSize";

// Ingestion status polling cadence (issue #26): 1s while a run is active,
// decaying to 10s when idle, paused entirely while the tab is hidden.
const STATUS_POLL_ACTIVE_MS = 1000;
const STATUS_POLL_IDLE_MAX_MS = 10000;
// During an active run the summary re-fetches every ~3-5s so the table fills
// in live without hammering the DB.
const SUMMARY_REFRESH_MS = 3500;
// A heartbeat older than this is displayed as "stalled?" (display-only).
const STALL_AFTER_S = 15;

const RUN_STATES = [
  "starting", "awaiting_auth", "running",
  "done", "failed", "cancelled", "abandoned",
];

const views = ["loading", "setup", "error", "stats"];

const state = {
  data: null,
  query: "",
  page: 1,
  pageSize: loadPageSize(),
  showHidden: false,
  hidden: loadHidden(),
  view: "loading",
  // Keeps the setup view visible even when data exists: the "use Takeout
  // instead" deep-link from a policy_enforced failure lands on its card.
  pinnedSetup: false,
};

// Everything the ingestion UI knows, all viewer-side.
const ingest = {
  // False once /api/status turns out not to exist (the static Pages demo, or
  // any non-viewer host): every piece of ingestion UI hides, polling stops,
  // and no mutating request is ever attempted.
  available: true,
  status: null,
  runsNow: 0,
  delay: STATUS_POLL_ACTIVE_MS,
  timer: null,
  lastSummaryAt: 0,
  runsKey: null,
  wasActive: false,
  // CSRF token from the latest /api/status payload; sent on every POST.
  csrf: null,
  launching: false,
  logOpen: false,
};

function loadPageSize() {
  try {
    const stored = Number(localStorage.getItem(PAGE_SIZE_KEY));
    return PAGE_SIZES.includes(stored) ? stored : DEFAULT_PAGE_SIZE;
  } catch (err) {
    return DEFAULT_PAGE_SIZE;
  }
}

function loadHidden() {
  try {
    const raw = localStorage.getItem(HIDDEN_KEY);
    const list = raw ? JSON.parse(raw) : [];
    return new Set(Array.isArray(list) ? list.filter(s => typeof s === "string") : []);
  } catch (err) {
    return new Set();
  }
}

function saveHidden() {
  try {
    localStorage.setItem(HIDDEN_KEY, JSON.stringify([...state.hidden]));
  } catch (err) {
    // Storage unavailable (private mode etc.) — hiding still works, just
    // doesn't survive reload.
  }
}

function showView(name) {
  state.view = name;
  for (const id of views) {
    document.getElementById(id).classList.toggle("hidden", id !== name);
  }
}

function showError(message) {
  document.getElementById("error-message").textContent = message;
  showView("error");
}

function senderRow(row, isHidden) {
  const tr = document.createElement("tr");
  if (isHidden) tr.className = "hidden-row";

  const senderCell = document.createElement("td");
  senderCell.className = "sender";
  senderCell.textContent = row.sender;

  const countCell = document.createElement("td");
  countCell.className = "num";
  countCell.textContent = Number(row.mails_sent || 0).toLocaleString();

  const actionCell = document.createElement("td");
  actionCell.className = "act";
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "icon-btn";
  btn.textContent = isHidden ? "👁" : "🙈";
  btn.title = isHidden ? "Unhide this sender" : "Hide this sender";
  btn.setAttribute("aria-label", btn.title);
  btn.addEventListener("click", () => {
    if (isHidden) {
      state.hidden.delete(row.sender);
    } else {
      state.hidden.add(row.sender);
    }
    saveHidden();
    update();
  });
  actionCell.appendChild(btn);

  tr.append(senderCell, countCell, actionCell);
  return tr;
}

// --- ingestion status helpers ---------------------------------------------

function activeRun() {
  return (ingest.available && ingest.status && ingest.status.active_run) || null;
}

function ingestActive() {
  const s = ingest.available ? ingest.status : null;
  return Boolean(s && (s.active_run || s.ingest_lock_held === true));
}

// Onboarding replaces the stats view when the database is missing/empty and
// nothing is running. Only the live viewer (with /api/status) can know this.
function shouldOnboard() {
  const s = ingest.available ? ingest.status : null;
  return Boolean(s && (s.db === "missing" || s.db === "empty") && !ingestActive());
}

function sourceLabel(source) {
  if (source === "mbox") return "Takeout import";
  if (source === "gmail_api") return "Gmail scan";
  return source ? String(source) : "Ingestion";
}

function formatDuration(totalSeconds) {
  const s = Math.max(0, Math.round(Number(totalSeconds)));
  if (s < 90) return `${s}s`;
  const m = Math.round(s / 60);
  if (m < 90) return `${m} min`;
  return `${Math.floor(m / 60)} h ${m % 60} min`;
}

function relativeTime(nowUnix, thenUnix) {
  const d = Math.max(0, Number(nowUnix) - Number(thenUnix));
  if (d < 60) return "just now";
  if (d < 3600) return `${Math.round(d / 60)} min ago`;
  if (d < 172800) return `${Math.round(d / 3600)} h ago`;
  return `${Math.round(d / 86400)} d ago`;
}

function stateChip(el, runState) {
  const known = RUN_STATES.includes(runState) ? runState : "other";
  el.className = `chip chip-${known}`;
  el.textContent =
    runState === "awaiting_auth" ? "awaiting authorization" : String(runState);
}

/// Fraction complete for a run, or null when there is no usable total.
function runProgress(run) {
  if (!run) return null;
  const bytesTotal = Number(run.bytes_total);
  if (bytesTotal > 0 && run.bytes_done != null) {
    return Math.min(1, Math.max(0, Number(run.bytes_done) / bytesTotal));
  }
  const total = Number(run.total_estimate);
  if (total > 0) {
    return Math.min(1, Math.max(0, Number(run.messages_seen) / total));
  }
  return null;
}

function hideIngestUi() {
  document.getElementById("ingest-pill").classList.add("hidden");
  document.getElementById("ingest-panel").classList.add("hidden");
  document.getElementById("history").classList.add("hidden");
  document.getElementById("mixed-banner").classList.add("hidden");
  // Launch controls exist only against the live viewer: without a status
  // endpoint there is no CSRF token and no spawn surface.
  for (const el of document.querySelectorAll(".live-only")) {
    el.classList.add("hidden");
  }
}

// --- mutating calls (Phase C) ----------------------------------------------

// Every mutating request carries the full client side of the CSRF stack:
// JSON content type plus the per-process token learned from /api/status.
// (The browser adds Origin/Sec-Fetch-Site itself.)
async function apiPost(path, body) {
  if (!ingest.available || !ingest.csrf) {
    throw new Error("ingestion API unavailable");
  }
  return fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Gmail-Stats-Csrf": ingest.csrf,
    },
    body: JSON.stringify(body),
  });
}

// Start a run and surface any error inline (all error text via textContent).
async function startRun(body, errorEl) {
  if (ingest.launching) return;
  ingest.launching = true;
  if (errorEl) errorEl.classList.add("hidden");
  try {
    const response = await apiPost("/api/runs", body);
    if (response.status === 202) {
      state.pinnedSetup = false;
      pollStatus();
      return;
    }
    let payload = null;
    try {
      payload = await response.json();
    } catch (err) {
      // non-JSON error body; fall through to the generic message
    }
    if (errorEl) {
      errorEl.textContent =
        (payload && payload.message) || `Could not start the run (HTTP ${response.status}).`;
      errorEl.classList.remove("hidden");
    }
  } catch (err) {
    if (errorEl) {
      errorEl.textContent = "Could not reach the server.";
      errorEl.classList.remove("hidden");
    }
  } finally {
    ingest.launching = false;
  }
}

async function cancelActiveRun() {
  const run = activeRun();
  if (!run || !ingest.status || ingest.status.owns_active_run !== true) return;
  const errorEl = document.getElementById("cancel-error");
  errorEl.classList.add("hidden");
  try {
    const response = await apiPost(`/api/runs/${Number(run.run_id)}/cancel`, {});
    if (response.status !== 202) {
      let payload = null;
      try {
        payload = await response.json();
      } catch (err) {
        // keep the generic message
      }
      errorEl.textContent =
        (payload && payload.message) || `Cancel failed (HTTP ${response.status}).`;
      errorEl.classList.remove("hidden");
    }
    pollStatus();
  } catch (err) {
    errorEl.textContent = "Could not reach the server.";
    errorEl.classList.remove("hidden");
  }
}

async function refreshLog() {
  const run = activeRun();
  const view = document.getElementById("run-log-view");
  if (!run || !ingest.logOpen) return;
  try {
    const response = await fetch(`/api/runs/${Number(run.run_id)}/log`);
    if (!response.ok) return;
    const body = await response.json();
    const lines = Array.isArray(body.lines) ? body.lines : [];
    view.textContent = lines.length > 0 ? lines.join("\n") : "(no stderr output yet)";
  } catch (err) {
    // Leave whatever is shown; the next poll retries.
  }
}

// Deep-link a policy_enforced failure to the Takeout card: pin the setup view
// open (even when data already exists) and flash the card.
function showTakeoutCard() {
  state.pinnedSetup = true;
  showView("setup");
  const card = document.getElementById("takeout-card");
  card.classList.remove("flash");
  // Restart the highlight animation on repeat clicks.
  void card.offsetWidth;
  card.classList.add("flash");
  card.scrollIntoView({ behavior: "smooth", block: "center" });
}

/// The only URL the UI will ever hyperlink: a Google consent URL. Anything
/// else (however it got into auth_url) renders as inert text.
function isGoogleAuthUrl(url) {
  if (typeof url !== "string" || !url.startsWith("https://accounts.google.com/")) {
    return false;
  }
  try {
    return new URL(url).origin === "https://accounts.google.com";
  } catch (err) {
    return false;
  }
}

function renderAuthSlot(run) {
  const slot = document.getElementById("run-auth");
  if (!run || run.state !== "awaiting_auth") {
    slot.classList.add("hidden");
    slot.replaceChildren();
    return;
  }
  slot.replaceChildren();
  const label = document.createElement("span");
  label.textContent = "Google is waiting for your consent: ";
  slot.appendChild(label);
  const url = run.auth_url;
  if (isGoogleAuthUrl(url)) {
    const link = document.createElement("a");
    link.href = url;
    link.target = "_blank";
    link.rel = "noopener noreferrer";
    link.textContent = "Authorize in Google";
    slot.appendChild(link);
  } else if (url) {
    // Unexpected URL: show it as inert text, never as a link.
    const inert = document.createElement("span");
    inert.textContent = String(url);
    slot.appendChild(inert);
  } else {
    const waiting = document.createElement("span");
    waiting.textContent = "waiting for the consent URL…";
    slot.appendChild(waiting);
  }
  slot.classList.remove("hidden");
}

function renderPill() {
  const pill = document.getElementById("ingest-pill");
  const run = activeRun();
  if (!ingestActive()) {
    pill.classList.add("hidden");
    return;
  }
  const parts = [];
  if (run) {
    parts.push(run.source === "mbox" ? "Importing" : "Scanning");
    parts.push(`${Number(run.messages_seen || 0).toLocaleString()} messages`);
    const rate = ingest.status.rate_per_sec;
    if (rate != null) parts.push(`${Number(rate).toLocaleString()}/s`);
    const progress = runProgress(run);
    if (progress != null) parts.push(`${Math.round(progress * 100)}%`);
  } else {
    // Flock held but no row yet: an ingester is just starting.
    parts.push("Ingester starting…");
  }
  document.getElementById("pill-text").textContent = parts.join(" · ");
  pill.classList.remove("hidden");
}

function renderPanel() {
  const panel = document.getElementById("ingest-panel");
  if (!ingestActive()) {
    panel.classList.add("hidden");
    ingest.logOpen = false;
    document.getElementById("run-log-view").classList.add("hidden");
    document.getElementById("run-log").textContent = "Show log";
    return;
  }
  const run = activeRun();
  const status = ingest.status;

  document.getElementById("run-source").textContent =
    run ? sourceLabel(run.source) : "Ingestion";
  stateChip(document.getElementById("run-state"), run ? run.state : "starting");

  const counts = document.getElementById("run-counts");
  counts.textContent = run
    ? `${Number(run.messages_seen || 0).toLocaleString()} seen · ` +
      `${Number(run.messages_new || 0).toLocaleString()} new`
    : "Waiting for the first progress report…";

  const rateEl = document.getElementById("run-rate");
  rateEl.textContent =
    status.rate_per_sec != null
      ? `${Number(status.rate_per_sec).toLocaleString()} msg/s`
      : "";

  const etaEl = document.getElementById("run-eta");
  etaEl.textContent =
    status.eta_seconds != null ? `~${formatDuration(status.eta_seconds)} left` : "";

  const bar = document.getElementById("run-bar");
  const fill = document.getElementById("run-bar-fill");
  const progress = runProgress(run);
  bar.classList.remove("hidden");
  if (progress != null) {
    bar.classList.remove("indeterminate");
    fill.style.width = `${(progress * 100).toFixed(1)}%`;
  } else {
    // No total yet: indeterminate "counting…" sweep.
    bar.classList.add("indeterminate");
    fill.style.width = "30%";
  }

  const stalled = document.getElementById("run-stalled");
  const heartbeatAge =
    run && status.now_unix ? Number(status.now_unix) - Number(run.updated_at_unix) : 0;
  if (run && heartbeatAge > STALL_AFTER_S) {
    stalled.textContent =
      `No heartbeat for ${formatDuration(heartbeatAge)} — the ingester may be stalled.`;
    stalled.classList.remove("hidden");
  } else {
    stalled.classList.add("hidden");
  }

  renderAuthSlot(run);

  // Cancel and Log exist only for runs this viewer process spawned; a
  // terminal-started (or pre-restart) run is watch-only here.
  const owned = status.owns_active_run === true && Boolean(run);
  document.getElementById("run-cancel").classList.toggle("hidden", !owned);
  document.getElementById("run-log").classList.toggle("hidden", !owned);
  if (!owned) {
    ingest.logOpen = false;
    document.getElementById("run-log-view").classList.add("hidden");
    document.getElementById("run-log").textContent = "Show log";
  } else if (ingest.logOpen) {
    refreshLog();
  }
  document.getElementById("run-origin").textContent = owned
    ? ""
    : "Started from the terminal — cancel it with Ctrl-C there.";

  panel.classList.remove("hidden");
}

function historyItem(run, nowUnix) {
  const li = document.createElement("li");

  const source = document.createElement("span");
  source.className = "badge";
  source.textContent = sourceLabel(run.source);

  const chip = document.createElement("span");
  stateChip(chip, run.state);

  const counts = document.createElement("span");
  counts.textContent =
    `${Number(run.messages_seen || 0).toLocaleString()} seen · ` +
    `${Number(run.messages_new || 0).toLocaleString()} new`;

  const when = document.createElement("span");
  when.className = "muted";
  const stamp = run.finished_at_unix || run.updated_at_unix;
  when.textContent = stamp ? relativeTime(nowUnix, stamp) : "";

  li.append(source, chip, counts, when);

  if (run.error) {
    const error = document.createElement("div");
    error.className = "history-error";
    // error_kind and error are ingester-written but rendered as inert text.
    error.textContent = run.error_kind ? `${run.error_kind}: ${run.error}` : run.error;
    li.appendChild(error);
  }

  const actions = historyActions(run);
  if (actions) li.appendChild(actions);
  return li;
}

// Retry/Resume affordances for finished runs (issue #26): interrupted imports
// resume from their byte offset (fingerprint-validated by the ingester);
// failed scans get a best-effort resume from the persisted page token; and
// policy_enforced steers to the Takeout card. Only rendered against the live
// viewer, and only while nothing is running.
function historyActions(run) {
  if (!ingest.available || ingestActive()) return null;
  const finished = ["failed", "cancelled", "abandoned"].includes(run.state);
  if (!finished) return null;

  const actions = document.createElement("div");
  actions.className = "history-actions";
  const addButton = (label, body) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = label;
    btn.addEventListener("click", () => startRun(body, null));
    actions.appendChild(btn);
  };

  if (run.source === "mbox") {
    if (Number(run.bytes_done) > 0 && run.mbox_path) {
      addButton("Resume import", { source: "mbox", resume_run_id: Number(run.run_id) });
    } else if (run.mbox_path) {
      addButton("Retry", { source: "mbox", path: String(run.mbox_path) });
    }
  } else if (run.source === "gmail_api") {
    if (run.state === "failed" || run.state === "abandoned") {
      addButton("Retry", { source: "gmail_api" });
    }
    if (Number(run.messages_seen) > 0) {
      addButton("Resume — may restart from the beginning", {
        source: "gmail_api",
        resume_run_id: Number(run.run_id),
      });
    } else if (run.state === "cancelled") {
      addButton("Start again", { source: "gmail_api" });
    }
  }

  if (run.error_kind === "policy_enforced") {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = "Use Takeout instead →";
    btn.addEventListener("click", showTakeoutCard);
    actions.appendChild(btn);
  }

  return actions.childElementCount > 0 ? actions : null;
}

function renderHistory(runs, nowUnix) {
  const section = document.getElementById("history");
  const list = document.getElementById("history-list");
  list.replaceChildren();
  if (!ingest.available || runs.length === 0) {
    section.classList.add("hidden");
    return;
  }
  for (const run of runs) {
    list.appendChild(historyItem(run, nowUnix));
  }
  section.classList.remove("hidden");
}

async function fetchRuns() {
  if (!ingest.available) return;
  let response;
  try {
    response = await fetch("/api/runs?limit=20");
  } catch (err) {
    return;
  }
  if (!response.ok) return;
  let body;
  try {
    body = await response.json();
  } catch (err) {
    return;
  }
  renderHistory(Array.isArray(body.runs) ? body.runs : [], body.now_unix || 0);
}

// The main view (loading/setup/error/stats) is owned by the summary flow, but
// status changes can flip it: onboarding appears when the DB is missing/empty
// and idle, and leaves as soon as a run starts or data exists.
function reconcileMainView() {
  if (shouldOnboard() || (state.pinnedSetup && !ingestActive())) {
    if (state.view === "stats" || state.view === "setup" || state.view === "loading") {
      showView("setup");
    }
    return;
  }
  state.pinnedSetup = false;
  if (state.view === "setup") {
    // Onboarding no longer applies (a run started, or data appeared).
    if (state.data) {
      update();
    } else {
      showView("loading");
    }
  }
}

function renderIngest() {
  if (!ingest.available) {
    hideIngestUi();
    return;
  }
  // Live viewer confirmed: reveal the launch controls (they start hidden so
  // the static demo never shows them, even before the first status probe).
  for (const el of document.querySelectorAll(".live-only")) {
    el.classList.toggle("hidden", !ingest.status);
  }
  renderPill();
  renderPanel();
  document
    .getElementById("mixed-banner")
    .classList.toggle("hidden", !(ingest.status && ingest.status.mixed_sources === true));
  reconcileMainView();
}

function scheduleStatusPoll(delayMs) {
  if (!ingest.available) return;
  if (ingest.timer) clearTimeout(ingest.timer);
  ingest.timer = setTimeout(pollStatus, delayMs);
}

function runsKeyOf(status) {
  if (!status) return "none";
  const a = status.active_run;
  const l = status.last_run;
  return [
    a ? `${a.run_id}:${a.state}` : "-",
    l ? `${l.run_id}:${l.state}` : "-",
  ].join("|");
}

async function pollStatus() {
  ingest.timer = null;
  if (!ingest.available || document.hidden) return;

  let response;
  try {
    response = await fetch("/api/status");
  } catch (err) {
    // Server unreachable (restart?): hide the ingestion UI, keep probing at
    // the idle cadence in case it comes back.
    hideIngestUi();
    ingest.delay = STATUS_POLL_IDLE_MAX_MS;
    scheduleStatusPoll(ingest.delay);
    return;
  }
  if (response.status === 404) {
    // The endpoint doesn't exist at all: static hosting (the Pages demo).
    // Hide every piece of ingestion UI and never poll again.
    ingest.available = false;
    hideIngestUi();
    return;
  }
  if (!response.ok) {
    // busy 503 and friends: keep whatever is on screen, try again shortly.
    scheduleStatusPoll(Math.min(STATUS_POLL_IDLE_MAX_MS, ingest.delay * 2));
    return;
  }
  let body;
  try {
    body = await response.json();
  } catch (err) {
    // 200 with a non-JSON body: an SPA-style static host, not the viewer.
    ingest.available = false;
    hideIngestUi();
    return;
  }

  ingest.status = body;
  ingest.csrf = typeof body.csrf_token === "string" ? body.csrf_token : null;
  renderIngest();

  const key = runsKeyOf(body);
  if (key !== ingest.runsKey) {
    ingest.runsKey = key;
    fetchRuns();
  }

  const active = ingestActive();
  if (active && Date.now() - ingest.lastSummaryAt > SUMMARY_REFRESH_MS) {
    refreshSummary();
  }
  if (ingest.wasActive && !active) {
    // Run just finished: pull the final numbers immediately.
    refreshSummary();
  }
  ingest.wasActive = active;

  ingest.delay = active
    ? STATUS_POLL_ACTIVE_MS
    : Math.min(STATUS_POLL_IDLE_MAX_MS, Math.max(ingest.delay, STATUS_POLL_ACTIVE_MS) * 2);
  scheduleStatusPoll(ingest.delay);
}

// --- summary ----------------------------------------------------------------

// The single site that talks to /api/summary (the demo build rewrites this
// exact call to read a static summary.json instead).
async function fetchSummary() {
  const response = await fetch("/api/summary");
  const body = await response.json();
  return { response, body };
}

/// Silent refresh during active runs: replaces the data but preserves every
/// piece of client state (search, page, page size, hidden senders — all
/// client-side already). A busy 503 keeps the previous table and dims the
/// freshness stamp instead of blanking anything.
async function refreshSummary() {
  ingest.lastSummaryAt = Date.now();
  let response;
  let body;
  try {
    ({ response, body } = await fetchSummary());
  } catch (err) {
    return;
  }
  const generated = document.getElementById("generated-at");
  if (!response.ok) {
    if (body && body.error === "busy") {
      generated.classList.add("stale");
    }
    return;
  }
  generated.classList.remove("stale");
  state.data = body;
  if (state.view === "stats" || state.view === "loading" || state.view === "setup") {
    update();
  }
}

function update() {
  if (shouldOnboard() || (state.pinnedSetup && !ingestActive())) {
    showView("setup");
    return;
  }
  const data = state.data;
  if (!data) return;
  const all = Array.isArray(data.senders) ? data.senders : [];

  // Tiles always exclude hidden senders; hiding subtracts from the total.
  const visible = all.filter(row => !state.hidden.has(row.sender));
  const hiddenCount = all.reduce(
    (sum, row) => sum + (state.hidden.has(row.sender) ? Number(row.mails_sent || 0) : 0),
    0,
  );
  document.getElementById("total-messages").textContent =
    Math.max(0, Number(data.total_messages || 0) - hiddenCount).toLocaleString();
  document.getElementById("distinct-senders").textContent =
    visible.length.toLocaleString();
  document.getElementById("top-sender").textContent =
    visible.length > 0 ? visible[0].sender : "—";

  // The list: hidden rows appear only with the toggle on, and search applies
  // to whatever is listed.
  const query = state.query.trim().toLowerCase();
  let listed = state.showHidden ? all : visible;
  if (query) {
    listed = listed.filter(row => String(row.sender).toLowerCase().includes(query));
  }

  const pages = Math.max(1, Math.ceil(listed.length / state.pageSize));
  state.page = Math.min(Math.max(1, state.page), pages);
  const start = (state.page - 1) * state.pageSize;
  const pageRows = listed.slice(start, start + state.pageSize);

  const tbody = document.getElementById("sender-rows");
  tbody.replaceChildren();
  for (const row of pageRows) {
    tbody.appendChild(senderRow(row, state.hidden.has(row.sender)));
  }

  document.getElementById("page-info").textContent =
    `Page ${state.page} of ${pages} · ${listed.length.toLocaleString()} senders` +
    (state.hidden.size > 0 && !state.showHidden
      ? ` (${state.hidden.size.toLocaleString()} hidden)` : "");
  document.getElementById("prev-page").disabled = state.page <= 1;
  document.getElementById("next-page").disabled = state.page >= pages;

  const generated = document.getElementById("generated-at");
  if (data.generated_at_unix) {
    generated.textContent =
      "Data as of " + new Date(data.generated_at_unix * 1000).toLocaleString();
  } else {
    generated.textContent = "";
  }

  showView("stats");
}

async function load() {
  showView("loading");
  let response;
  let body;
  try {
    ({ response, body } = await fetchSummary());
  } catch (err) {
    showError("Could not reach the server. Is it still running?");
    return;
  }
  if (!response.ok) {
    if (body && body.error === "missing_db") {
      // No data yet. If an ingester is already running, stay on the loading
      // view — the progress panel is visible and the summary refresh will
      // take over as soon as the first rows land. Otherwise: onboarding.
      if (ingestActive()) {
        showView("loading");
      } else {
        showView("setup");
      }
    } else if (body && body.error === "busy") {
      showError(body.message || "Database busy — retry shortly.");
    } else {
      showError((body && body.message) || "Unexpected server error.");
    }
    return;
  }
  state.data = body;
  state.page = 1;
  ingest.lastSummaryAt = Date.now();
  update();
}

document.getElementById("retry-setup").addEventListener("click", () => {
  state.pinnedSetup = false;
  load();
});
document.getElementById("retry-error").addEventListener("click", load);

// Launch buttons (Phase C). Guarded by ingest.available: they are hidden —
// and inert — on static hosting.
document.getElementById("start-scan").addEventListener("click", () => {
  if (!ingest.available) return;
  startRun({ source: "gmail_api" }, document.getElementById("scan-error"));
});
document.getElementById("start-import").addEventListener("click", () => {
  if (!ingest.available) return;
  const path = document.getElementById("mbox-path").value.trim();
  const errorEl = document.getElementById("import-error");
  if (!path) {
    errorEl.textContent = "Paste the absolute path to the .mbox file first.";
    errorEl.classList.remove("hidden");
    return;
  }
  startRun({ source: "mbox", path }, errorEl);
});
document.getElementById("mbox-path").addEventListener("keydown", event => {
  if (event.key === "Enter") document.getElementById("start-import").click();
});
document.getElementById("run-cancel").addEventListener("click", cancelActiveRun);
document.getElementById("run-log").addEventListener("click", () => {
  ingest.logOpen = !ingest.logOpen;
  const view = document.getElementById("run-log-view");
  const btn = document.getElementById("run-log");
  view.classList.toggle("hidden", !ingest.logOpen);
  btn.textContent = ingest.logOpen ? "Hide log" : "Show log";
  if (ingest.logOpen) {
    view.textContent = "Loading…";
    refreshLog();
  }
});
document.getElementById("search").addEventListener("input", event => {
  state.query = event.target.value;
  state.page = 1;
  update();
});
document.getElementById("page-size").value = String(state.pageSize);
document.getElementById("page-size").addEventListener("change", event => {
  const size = Number(event.target.value);
  state.pageSize = PAGE_SIZES.includes(size) ? size : DEFAULT_PAGE_SIZE;
  try {
    localStorage.setItem(PAGE_SIZE_KEY, String(state.pageSize));
  } catch (err) {
    // Storage unavailable — selection just won't survive reload.
  }
  state.page = 1;
  update();
});
document.getElementById("show-hidden").addEventListener("change", event => {
  state.showHidden = event.target.checked;
  state.page = 1;
  update();
});
document.getElementById("prev-page").addEventListener("click", () => {
  state.page -= 1;
  update();
});
document.getElementById("next-page").addEventListener("click", () => {
  state.page += 1;
  update();
});

// Copy buttons on the onboarding cards copy the adjacent CLI command.
for (const btn of document.querySelectorAll(".copy-btn")) {
  btn.addEventListener("click", () => {
    const pre = btn.parentElement.querySelector("pre");
    const text = pre ? pre.textContent : "";
    const done = () => {
      btn.textContent = "Copied";
      setTimeout(() => { btn.textContent = "Copy"; }, 1500);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done, () => selectText(pre));
    } else {
      selectText(pre);
    }
  });
}

function selectText(el) {
  if (!el) return;
  const range = document.createRange();
  range.selectNodeContents(el);
  const selection = window.getSelection();
  selection.removeAllRanges();
  selection.addRange(range);
}

// Pause status polling while the tab is hidden; poll immediately on return.
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    if (ingest.timer) clearTimeout(ingest.timer);
    ingest.timer = null;
  } else if (ingest.available) {
    pollStatus();
  }
});

load();
pollStatus();
