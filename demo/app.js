"use strict";

// Vendored from the local viewer (web/app.js on the issue-11 branch), with
// the server-specific parts removed: data is a static ./summary.json, so
// there is no "missing DB" setup flow and no server error envelope.
// Sender strings are still only ever rendered via textContent — never
// innerHTML — to keep parity with the viewer's handling of untrusted input.

const views = ["loading", "error", "stats"];

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
      "Synthetic data generated " +
      new Date(data.generated_at_unix * 1000).toLocaleDateString();
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
    response = await fetch("./summary.json");
    body = await response.json();
  } catch (err) {
    showError("Could not load the demo data.");
    return;
  }
  if (!response.ok) {
    showError("Could not load the demo data.");
    return;
  }
  render(body);
}

document.getElementById("retry-error").addEventListener("click", load);
load();
