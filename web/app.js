"use strict";

// Sender strings come from arbitrary From: headers and are attacker-controlled.
// This page only ever renders them via textContent — never innerHTML.

const views = ["loading", "setup", "error", "stats"];

function showView(name) {
  for (const id of views) {
    document.getElementById(id).classList.toggle("hidden", id !== name);
  }
}

function showError(message) {
  document.getElementById("error-message").textContent = message;
  showView("error");
}

function render(data) {
  const senders = Array.isArray(data.senders) ? data.senders : [];

  document.getElementById("total-messages").textContent =
    Number(data.total_messages || 0).toLocaleString();
  document.getElementById("distinct-senders").textContent =
    senders.length.toLocaleString();
  document.getElementById("top-sender").textContent =
    senders.length > 0 ? senders[0].sender : "—";

  const tbody = document.getElementById("sender-rows");
  tbody.replaceChildren();
  for (const row of senders) {
    const tr = document.createElement("tr");
    const senderCell = document.createElement("td");
    senderCell.className = "sender";
    senderCell.textContent = row.sender;
    const countCell = document.createElement("td");
    countCell.className = "num";
    countCell.textContent = Number(row.mails_sent || 0).toLocaleString();
    tr.append(senderCell, countCell);
    tbody.appendChild(tr);
  }

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
  render(body);
}

document.getElementById("retry-setup").addEventListener("click", load);
document.getElementById("retry-error").addEventListener("click", load);
load();
