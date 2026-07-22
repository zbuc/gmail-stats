#!/usr/bin/env python3
"""Deterministically generate demo/summary.json from demo/dataset_spec.json.

The spec (designed on issue #15) describes a fictional, realistically
overgrown Gmail account: 84,000 messages across 1,419 senders, split into
categories with pinned "hero" senders and Zipf- or mixture-distributed tails.
All addresses use RFC 2606 reserved domains; all brands are fictional.

Run:  python3 demo/generate_summary.py
"""

import json
import random
from pathlib import Path

HERE = Path(__file__).parent
SPEC = json.loads((HERE / "dataset_spec.json").read_text())

# Fixed timestamp shown on the page ("Synthetic data generated ...").
GENERATED_AT_UNIX = 1784764800

# Per-category tail rules from the spec prose: either a zipf tail
# {n, budget, s, clamp} or a list of subgroups. Hero counts/budgets are taken
# from spec.top_senders, so tails must cover (total - heroes) exactly.
TAIL_RULES = {
    "retail_promo": [{"kind": "zipf", "clamp": (8, 1000)}],
    "social": [{"kind": "zipf", "clamp": (15, 600)}],
    "newsletters": [{"kind": "zipf", "clamp": (5, 900)}],
    "alerts": [{"kind": "zipf", "clamp": (20, 850)}],
    "receipts": [
        {"kind": "zipf", "n": 60, "budget": 2500, "clamp": (10, 500)},
        {"kind": "mixture", "n": 157, "budget": 382,
         "probs": {1: 0.35, 2: 0.25, 3: 0.20, 4: 0.12, 5: 0.08}},
    ],
    "finance": [{"kind": "zipf", "clamp": (24, 450)}],
    "platform": [{"kind": "zipf", "clamp": (5, 700)}],
    "forums_gaming": [{"kind": "zipf", "clamp": (10, 650)}],
    "travel": [{"kind": "zipf", "clamp": (3, 220)}],
    "gray_spam": [{"kind": "zipf", "clamp": (2, 90)}],
    "humans": [
        {"kind": "zipf", "n": 27, "budget": 755, "clamp": (8, 120)},
        {"kind": "mixture", "n": 90, "budget": 155,
         "probs": {1: 0.65, 2: 0.20, 3: 0.10, 4: 0.05}},
    ],
    "oneoff_tail": [{"kind": "mixture",
                     "probs": {1: 0.75, 2: 0.15, 3: 0.06, 4: 0.03, 5: 0.01}}],
}

ALLOWED_SUFFIXES = (".example", "example.com", "example.net", "example.org")


def expand_brands(words):
    """pre:/suf: entries combine pairwise into brands; bare entries stand alone."""
    standalone = [w for w in words if not w.startswith(("pre:", "suf:"))]
    pres = [w[4:] for w in words if w.startswith("pre:")]
    sufs = [w[4:] for w in words if w.startswith("suf:")]
    return standalone + [p + s for p in pres for s in sufs]


def zipf_counts(n, budget, s, clamp, rng):
    lo, hi = clamp
    assert lo * n <= budget <= hi * n, f"infeasible zipf tail: {n=} {budget=} {clamp=}"
    weights = [(1.0 / (i + 1) ** s) * rng.uniform(0.7, 1.3) for i in range(n)]
    total_w = sum(weights)
    counts = [max(lo, min(hi, round(budget * w / total_w))) for w in weights]
    diff = budget - sum(counts)
    i = 0
    while diff != 0:
        j = i % n
        if diff > 0 and counts[j] < hi:
            counts[j] += 1
            diff -= 1
        elif diff < 0 and counts[j] > lo:
            counts[j] -= 1
            diff += 1
        i += 1
    return sorted(counts, reverse=True)


def mixture_counts(n, budget, probs, rng):
    counts = []
    for value, p in sorted(probs.items()):
        counts.extend([value] * round(n * p))
    counts = counts[:n] + [1] * (n - len(counts))
    rng.shuffle(counts)
    cap = max(probs) + 1
    diff = budget - sum(counts)
    i = 0
    while diff != 0:
        j = i % n
        if diff > 0 and counts[j] < cap:
            counts[j] += 1
            diff -= 1
        elif diff < 0 and counts[j] > 1:
            counts[j] -= 1
            diff += 1
        i += 1
    return sorted(counts, reverse=True)


def human_addresses(cat, n, rng, used):
    firsts = [w[6:] for w in cat["brand_words"] if w.startswith("first:")]
    lasts = [w[5:] for w in cat["brand_words"] if w.startswith("last:")]
    patterns = cat["name_patterns"]
    out = []
    pairs = [(f, l) for f in firsts for l in lasts]
    rng.shuffle(pairs)
    for i, (first, last) in enumerate(pairs):
        if len(out) == n:
            break
        pattern = patterns[i % len(patterns)]
        addr = (pattern.replace("{first}", first).replace("{last}", last)
                .replace("{digits2}", f"{rng.randrange(10, 100)}"))
        if addr not in used:
            used.add(addr)
            out.append(addr)
    assert len(out) == n, f"not enough human addresses for {n=}"
    return out


def brand_addresses(cat, n, rng, used, brand_pool=None):
    brands = brand_pool if brand_pool is not None else expand_brands(cat["brand_words"])
    patterns = cat["name_patterns"]
    out = []
    # Cycle brands through patterns; a brand may appear under several local
    # parts (deals@ + sales@ at the same fictional shop), matching the spec.
    i = 0
    while len(out) < n and i < len(brands) * len(patterns):
        brand = brands[(i // len(patterns)) % len(brands)]
        pattern = patterns[i % len(patterns)]
        addr = pattern.replace("{brand}", brand)
        if addr not in used:
            used.add(addr)
            out.append(addr)
        i += 1
    assert len(out) == n, f"not enough addresses for {cat['key']} {n=}"
    return out


def main():
    rng = random.Random(SPEC["seed"])
    heroes_by_cat = {}
    for hero in SPEC.get("top_senders", []):
        heroes_by_cat.setdefault(hero["category"], []).append(hero)

    used = {h["sender"] for h in SPEC.get("top_senders", [])}
    rows = [(h["sender"], h["mails_sent"]) for h in SPEC.get("top_senders", [])]

    all_brand_words = [w for c in SPEC["categories"] for w in c.get("brand_words", [])
                      if not w.startswith(("first:", "last:"))]

    for cat in SPEC["categories"]:
        heroes = heroes_by_cat.get(cat["key"], [])
        tail_n = cat["sender_count"] - len(heroes)
        tail_budget = cat["total_messages"] - sum(h["mails_sent"] for h in heroes)
        rules = TAIL_RULES[cat["key"]]
        for rule in rules:
            n = rule.get("n", tail_n)
            budget = rule.get("budget", tail_budget)
            if rule["kind"] == "zipf":
                counts = zipf_counts(n, budget, cat.get("zipf_s", 1.0), rule["clamp"], rng)
            else:
                counts = mixture_counts(n, budget, rule["probs"], rng)
            if cat["key"] == "humans":
                addrs = human_addresses(cat, n, rng, used)
            elif cat["key"] == "oneoff_tail":
                pool = expand_brands(all_brand_words)
                rng.shuffle(pool)
                addrs = brand_addresses(cat, n, rng, used, brand_pool=pool)
            else:
                addrs = brand_addresses(cat, n, rng, used)
            rows.extend(zip(addrs, counts))

    # Validate against the spec's hard guarantees.
    total = sum(c for _, c in rows)
    assert total == sum(c["total_messages"] for c in SPEC["categories"]), total
    assert total >= SPEC["total_messages_min"], total
    assert len(rows) == SPEC["distinct_senders"], len(rows)
    assert len({a for a, _ in rows}) == len(rows), "duplicate sender address"
    for addr, _ in rows:
        domain = addr.split("@", 1)[1]
        assert domain.endswith(ALLOWED_SUFFIXES), f"non-reserved domain: {addr}"

    rows.sort(key=lambda r: (-r[1], r[0]))
    summary = {
        "total_messages": total,
        "generated_at_unix": GENERATED_AT_UNIX,
        "senders": [{"sender": a, "mails_sent": c} for a, c in rows],
    }
    (HERE / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(f"wrote summary.json: {len(rows)} senders, {total} messages, "
          f"top = {rows[0][0]} ({rows[0][1]})")


if __name__ == "__main__":
    main()
