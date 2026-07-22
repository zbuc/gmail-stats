"use strict";

// Sender strings come from arbitrary From: headers and are attacker-controlled.
// This page only ever renders them via textContent — never innerHTML.

const PAGE_SIZE = 50;
const HIDDEN_KEY = "gmail-stats.hiddenSenders";

const views = ["loading", "setup", "error", "stats"];

const state = {
  data: null,
  query: "",
  page: 1,
  showHidden: false,
  hidden: loadHidden(),
};

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

function update() {
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

  const pages = Math.max(1, Math.ceil(listed.length / PAGE_SIZE));
  state.page = Math.min(Math.max(1, state.page), pages);
  const start = (state.page - 1) * PAGE_SIZE;
  const pageRows = listed.slice(start, start + PAGE_SIZE);

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
    response = await fetch("/api/summary");
    body = await response.json();
  } catch (err) {
    showError("Could not reach the server. Is it still running?");
    return;
  }
  if (!response.ok) {
    if (body && body.error === "missing_db") {
      showView("setup");
    } else if (body && body.error === "busy") {
      showError(body.message || "Database busy — retry shortly.");
    } else {
      showError((body && body.message) || "Unexpected server error.");
    }
    return;
  }
  state.data = body;
  state.page = 1;
  update();
}

document.getElementById("retry-setup").addEventListener("click", load);
document.getElementById("retry-error").addEventListener("click", load);
document.getElementById("search").addEventListener("input", event => {
  state.query = event.target.value;
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
load();
