"use strict";

// Injected by tests/frontend/harness.py after app.js. Drives one scenario in
// headless Chrome and writes its observations into a <pre id="harness-results">
// element that the harness extracts from --dump-dom output. Request-side
// assertions (CSRF header, content type, body) are made by the harness's stub
// server, not here.

const SCENARIO = new URLSearchParams(new URL(document.currentScript.src).search)
  .get("scenario");

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function waitFor(check, what, timeout = 8000) {
  const started = Date.now();
  for (;;) {
    let value = null;
    try {
      value = check();
    } catch (err) {
      // keep polling
    }
    if (value) return value;
    if (Date.now() - started > timeout) {
      throw new Error(`timeout waiting for ${what}`);
    }
    await sleep(50);
  }
}

function hidden(id) {
  return document.getElementById(id).classList.contains("hidden");
}

function report(results) {
  const pre = document.createElement("pre");
  pre.id = "harness-results";
  pre.textContent = JSON.stringify(results);
  document.body.appendChild(pre);
}

async function run() {
  if (SCENARIO === "launch") {
    // Onboarding with the live viewer: launch controls appear; clicking them
    // POSTs with the CSRF header (asserted server-side).
    await waitFor(
      () => !document.getElementById("setup").classList.contains("hidden"),
      "setup view",
    );
    await waitFor(
      () => !document.getElementById("start-import").closest(".launch").classList.contains("hidden"),
      "launch controls revealed",
    );
    document.getElementById("mbox-path").value = "/tmp/harness.mbox";
    document.getElementById("start-import").click();
    await sleep(600);
    document.getElementById("start-scan").click();
    await sleep(600);
    report({
      scenario: SCENARIO,
      launchVisible: true,
    });
    return;
  }

  if (SCENARIO === "demo") {
    // Static hosting: /api/status 404s. No ingestion UI, no launch controls,
    // and (implicitly) no mutating calls — the stub records any POST.
    await waitFor(
      () => !document.getElementById("stats").classList.contains("hidden"),
      "stats view",
    );
    await sleep(800); // give the status probe time to conclude 404
    const launches = [...document.querySelectorAll(".live-only")];
    report({
      scenario: SCENARIO,
      allLaunchHidden: launches.every(el => el.classList.contains("hidden")),
      pillHidden: hidden("ingest-pill"),
      panelHidden: hidden("ingest-panel"),
      historyHidden: hidden("history"),
    });
    return;
  }

  if (SCENARIO === "owns_false" || SCENARIO === "owns_true") {
    await waitFor(() => !hidden("ingest-panel"), "progress panel");
    if (SCENARIO === "owns_false") {
      await sleep(400);
      report({
        scenario: SCENARIO,
        cancelHidden: hidden("run-cancel"),
        logHidden: hidden("run-log"),
        origin: document.getElementById("run-origin").textContent,
      });
      return;
    }
    await waitFor(() => !hidden("run-cancel"), "cancel button");
    document.getElementById("run-log").click();
    await waitFor(
      () => document.getElementById("run-log-view").textContent.includes("stub stderr line 2"),
      "log lines",
    );
    document.getElementById("run-cancel").click();
    await sleep(600);
    report({
      scenario: SCENARIO,
      cancelHidden: hidden("run-cancel"),
      logHidden: hidden("run-log"),
      logText: document.getElementById("run-log-view").textContent,
    });
    return;
  }

  if (SCENARIO === "mixed_unrepaired" || SCENARIO === "mixed_repaired") {
    await waitFor(
      () => !document.getElementById("stats").classList.contains("hidden"),
      "stats view",
    );
    if (SCENARIO === "mixed_unrepaired") {
      await waitFor(() => !hidden("mixed-banner"), "mixed banner");
    } else {
      await sleep(600); // give the status poll time to render (or not) the banner
    }
    report({
      scenario: SCENARIO,
      bannerHidden: hidden("mixed-banner"),
      text: document.getElementById("mixed-text").textContent,
      command: document.getElementById("mixed-command").textContent,
    });
    return;
  }

  if (SCENARIO === "auth_good" || SCENARIO === "auth_evil") {
    await waitFor(() => !hidden("run-auth"), "auth slot");
    const slot = document.getElementById("run-auth");
    const link = slot.querySelector("a");
    report({
      scenario: SCENARIO,
      hasLink: Boolean(link),
      href: link ? link.getAttribute("href") : null,
      target: link ? link.getAttribute("target") : null,
      rel: link ? link.getAttribute("rel") : null,
      text: slot.textContent,
    });
    return;
  }

  report({ scenario: SCENARIO, error: "unknown scenario" });
}

run().catch(err => report({ scenario: SCENARIO, error: String(err) }));
