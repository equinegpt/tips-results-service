#!/usr/bin/env python3
"""Lifeboat quarterly drill — prove the break-glass path still works
WITHOUT switching anything on.

A fallback that isn't exercised is a fallback that doesn't exist. This
drill runs the full live lifeboat chain against today's real card and
reports to 8F-Ops:

  1. Freshness: lifeboat_sectional_pars populated; lifeboat_connection
     _a2e max as_of not stale (tables are static builds — staleness is
     the lifeboat's main decay mode).
  2. Card adapter: fetch today's Racenet pages, parse, horse-match,
     upsert lifeboat_cards (the canary is the match rate).
  3. Generator: LIFEBOAT=1 DRY RUN (no --commit — the drill NEVER
     writes tips), count races tipped.

Exit 0 + green ntfy on pass; exit 1 + red ntfy on any failure.

Usage:
  RACING_DB_URL=... SCRAPER_API_KEY=... \
      python scripts/lifeboat/lifeboat_drill.py [--max-races N] [--no-ntfy]
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2

ROOT = Path(__file__).resolve().parent.parent.parent
NTFY_TOPIC = "8F-Ops"
A2E_STALE_DAYS = 120   # PIT snapshots are monthly; >4 months = decayed


def ntfy(title: str, msg: str, tags: str):
    try:
        import requests
        requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=msg.encode(),
                      headers={"Title": title, "Tags": tags}, timeout=15)
    except Exception as exc:
        print(f"[drill] ntfy failed: {exc}", flush=True)


def main() -> int:
    no_ntfy = "--no-ntfy" in sys.argv
    max_races = None
    if "--max-races" in sys.argv:
        max_races = sys.argv[sys.argv.index("--max-races") + 1]

    day = datetime.now(ZoneInfo("Australia/Melbourne")).date()
    checks: list[str] = []
    failures: list[str] = []

    # 1 — engine freshness
    url = os.environ.get("RACING_DB_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, connect_timeout=30)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM lifeboat_sectional_pars")
    pars = cur.fetchone()[0]
    (checks if pars >= 5000 else failures).append(f"pars cells: {pars}")
    cur.execute("SELECT max(as_of) FROM lifeboat_connection_a2e")
    a2e_max = cur.fetchone()[0]
    a2e_age = (day - a2e_max).days if a2e_max else 9999
    (checks if a2e_age <= A2E_STALE_DAYS else failures).append(
        f"A2E max as_of: {a2e_max} ({a2e_age}d old)")

    # 2 — live card adapter (today's real Racenet card)
    cmd = [sys.executable, str(ROOT / "scripts/lifeboat/lifeboat_card_adapter.py")]
    if max_races:
        cmd += ["--max-races", max_races]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    print(r.stdout, flush=True)
    m = re.search(r"DONE (\d+)/(\d+) races, (\d+)/(\d+) horses matched "
                  r"\(([\d.]+)%\)", r.stdout)
    if r.returncode == 0 and m:
        checks.append(f"cards: {m.group(1)}/{m.group(2)} races, "
                      f"{m.group(5)}% horses matched")
        races_fetched = int(m.group(1))
    else:
        failures.append(f"card adapter exit {r.returncode}: "
                        f"{(r.stdout + r.stderr)[-300:]}")
        races_fetched = 0

    # 3 — generator dry run on the fresh cards (never --commit)
    if races_fetched:
        env = dict(os.environ, LIFEBOAT="1")
        g = subprocess.run(
            [sys.executable, str(ROOT / "scripts/generate_8f_tips.py"),
             "--date", day.isoformat()],
            capture_output=True, text=True, timeout=1800, env=env)
        tipped = g.stdout.count("AI_BEST:")
        print(g.stdout[-2000:], flush=True)
        if g.returncode == 0 and tipped >= max(1, races_fetched // 2):
            checks.append(f"generator: {tipped}/{races_fetched} races tipped "
                          f"(dry run)")
        else:
            failures.append(f"generator exit {g.returncode}, "
                            f"{tipped}/{races_fetched} tipped: "
                            f"{(g.stdout + g.stderr)[-300:]}")

    ok = not failures
    body = "\n".join([f"OK  {c}" for c in checks]
                     + [f"FAIL  {f}" for f in failures])
    print(f"[drill] {'PASS' if ok else 'FAIL'}\n{body}", flush=True)
    if not no_ntfy:
        ntfy(f"Lifeboat drill {'PASS' if ok else 'FAIL'} — {day}",
             body + "\n(break-glass path exercised; nothing published)",
             "ring_buoy" if ok else "rotating_light")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
