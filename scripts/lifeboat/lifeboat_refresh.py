#!/usr/bin/env python3
"""Lifeboat monthly refresh — retrain the parked engines.

The lifeboat stays OFF (LIFEBOAT=1 is human-only), but its two data
engines decay if never rebuilt: pars drift as tracks/eras move, and
the PIT A2E table simply stops (monthly snapshots end at build time).
This job rebuilds both from racing-db, verifies the result, and
reports to 8F-Ops — so a real activation always finds fresh engines.

Runs monthly (Render cron lifeboat-monthly-refresh, 1st 12:00Z =
22:00 AEST) — before the quarterly drill's 22:00Z fire on quarter
months, so drills always exercise freshly-trained engines.

Usage: RACING_DB_URL=... python scripts/lifeboat/lifeboat_refresh.py [--no-ntfy]
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parent.parent.parent
NTFY_TOPIC = "8F-Ops"


def ntfy(title: str, msg: str, tags: str):
    try:
        import requests
        requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=msg.encode(),
                      headers={"Title": title, "Tags": tags}, timeout=15)
    except Exception as exc:
        print(f"[refresh] ntfy failed: {exc}", flush=True)


def run_builder(name: str) -> tuple[bool, str]:
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "lifeboat" / name)],
        capture_output=True, text=True, timeout=5400)
    tail = (r.stdout + r.stderr).strip().splitlines()[-3:]
    print(f"[refresh] {name} exit {r.returncode}\n  " + "\n  ".join(tail),
          flush=True)
    return r.returncode == 0, " | ".join(tail)


def main() -> int:
    no_ntfy = "--no-ntfy" in sys.argv
    checks: list[str] = []
    failures: list[str] = []

    # sanity first: racing-db must still be filling (the crawl is the
    # lifeboat's upstream — refreshing from a stalled DB bakes in decay)
    url = os.environ.get("RACING_DB_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, connect_timeout=30)
    cur = conn.cursor()
    cur.execute("""SELECT count(*) FROM races
                   WHERE race_date >= current_date - 14""")
    recent = cur.fetchone()[0]
    (checks if recent >= 100 else failures).append(
        f"racing-db races last 14d: {recent}")
    conn.close()

    if not failures:
        for name, table, floor in (
                ("build_pars.py", "lifeboat_sectional_pars", 5000),
                ("build_a2e.py", "lifeboat_connection_a2e", 100000)):
            ok, tail = run_builder(name)
            if not ok:
                failures.append(f"{name}: {tail}")
                continue
            conn = psycopg2.connect(url, connect_timeout=30)
            cur = conn.cursor()
            cur.execute(f"SELECT count(*) FROM {table}")  # noqa: S608
            n = cur.fetchone()[0]
            conn.close()
            (checks if n >= floor else failures).append(f"{table}: {n} rows")

    ok = not failures
    body = "\n".join([f"OK  {c}" for c in checks]
                     + [f"FAIL  {f}" for f in failures])
    print(f"[refresh] {'PASS' if ok else 'FAIL'}\n{body}", flush=True)
    if not no_ntfy:
        ntfy(f"Lifeboat refresh {'PASS' if ok else 'FAIL'} — {date.today()}",
             body, "ring_buoy" if ok else "rotating_light")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
