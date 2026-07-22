#!/usr/bin/env python3
"""Build the static GitHub Pages demo from the local viewer's assets.

web/ is the single source of truth for the UI. This script copies the
viewer's index.html/app.css/app.js into an output directory and applies the
demo deltas mechanically: data comes from a committed ./summary.json instead
of the live /api/summary, asset paths become relative (Pages serves under a
subpath), and the page is labeled as a synthetic-data demo.

Every substitution asserts that its anchor text matched exactly once, so if
web/ drifts in a way this transform doesn't understand, the Pages deploy
fails instead of silently publishing a broken or stale demo.

Run:  python3 demo/build_demo.py [output-dir]   (default: _site)
"""

import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WEB = ROOT / "web"
DEMO = ROOT / "demo"

DEMO_BANNER = (
    '<div class="demo-banner">\n'
    "    Demo — all data on this page is synthetic, for a fictional Gmail account.\n"
    "    No real inbox was scanned.\n"
    "  </div>\n\n  "
)

DEMO_FOOTER = (
    '  <p class="muted">\n'
    "    This is a static demo of the\n"
    '    <a href="https://github.com/zbuc/gmail-stats">gmail-stats</a> local web\n'
    "    viewer, populated with synthetic data. All sender addresses use reserved\n"
    "    example domains; no real email data is involved.\n"
    "  </p>\n"
)

DEMO_EXTRA_CSS = """
/* Demo-only additions, appended by demo/build_demo.py */
.demo-banner {
  background: color-mix(in srgb, var(--accent) 12%, var(--card));
  border: 1px solid var(--accent);
  border-radius: 8px;
  padding: 0.6rem 0.9rem;
  margin: 0 0 1rem;
  font-size: 0.9rem;
}
a { color: var(--accent); }
"""


def replace_exactly(text, old, new, what):
    count = text.count(old)
    assert count == 1, (
        f"demo build: expected exactly one occurrence of {what!r} in the viewer "
        f"assets, found {count}. web/ has drifted — update demo/build_demo.py."
    )
    return text.replace(old, new)


def main():
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "_site"
    out.mkdir(parents=True, exist_ok=True)

    js = (WEB / "app.js").read_text()
    js = replace_exactly(js, 'fetch("/api/summary")', 'fetch("./summary.json")',
                         "the /api/summary fetch")
    (out / "app.js").write_text(js)

    (out / "app.css").write_text((WEB / "app.css").read_text() + DEMO_EXTRA_CSS)

    html = (WEB / "index.html").read_text()
    html = replace_exactly(html, '<link rel="stylesheet" href="/app.css">',
                           '<link rel="stylesheet" href="./app.css">', "the stylesheet link")
    html = replace_exactly(html, '<script src="/app.js" defer></script>',
                           '<script src="./app.js" defer></script>', "the script tag")
    html = replace_exactly(html, "<title>gmail-stats</title>",
                           "<title>gmail-stats · demo</title>", "the page title")
    html = replace_exactly(html, "<h1>gmail-stats <span>· local viewer</span></h1>",
                           "<h1>gmail-stats <span>· public demo</span></h1>", "the heading")
    html = replace_exactly(html, "<main>\n  ", "<main>\n  " + DEMO_BANNER, "the main open tag")
    html = replace_exactly(html, "</main>", DEMO_FOOTER + "</main>", "the main close tag")
    (out / "index.html").write_text(html)

    shutil.copy(DEMO / "summary.json", out / "summary.json")
    print(f"demo built into {out}: " + ", ".join(sorted(p.name for p in out.iterdir())))


if __name__ == "__main__":
    main()
