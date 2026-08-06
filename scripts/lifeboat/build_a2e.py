#!/usr/bin/env python3
"""Lifeboat Phase B.2 — own A2E (Actual-over-Expected) for connections.

PF's jockey/trainer A2E = wins achieved ÷ wins the market expected
(sum of SP-implied win probabilities). We hold closing odds on ~90% of
2.5M runs, so we build our own — with one upgrade on PF: POINT-IN-TIME
monthly snapshots, so backtests join the A2E as it stood BEFORE each
race (no hindsight leak).

Declared v1 limitation: race_results stores the jockey per run but NOT
the trainer per run — trainer A2E attributes each horse's history to
its CURRENT trainer (horses.trainer_id). Jockey A2E is exact.

Identity handling (the Daniels lesson, 2026-08-06): racing-db splits
riders across claim-variant records ("Ms Nadia Daniels (a)",
"(a3/52kg)", ...) which fractures histories and wrecks A2E. Entities
are merged on a CANONICAL NAME key (lowercase, parentheticals and
honorifics stripped) — which is also the right lookup key for the
lifeboat, since Racenet cards supply names, not our ids.

Writes racing-db `lifeboat_connection_a2e`:
    (entity_type, entity_id, as_of month, career runs/wins/exp/a2e,
     rolling-365d runs/wins/exp/a2e/strike)

Usage: RACING_DB_URL=... python scripts/lifeboat/build_a2e.py
"""
from __future__ import annotations

import os
import sys
from collections import defaultdict, deque
from datetime import date

import psycopg2
import psycopg2.extras

START_SNAP = date(2023, 1, 1)       # first monthly snapshot we keep
MIN_RUNS = 20                        # below this, A2E is noise — skip row

import re as _re


def canon(name: str) -> str:
    """Canonical connection key: strip parentheticals, honorifics, case."""
    s = _re.sub(r"\(.*?\)", "", name or "")
    s = s.lower().strip()
    for pre in ("ms ", "mrs ", "mr ", "miss ", "dr "):
        if s.startswith(pre):
            s = s[len(pre):]
    return " ".join(s.split())


def month_starts(lo: date, hi: date):
    y, m = lo.year, lo.month
    while (y, m) <= (hi.year, hi.month):
        yield date(y, m, 1)
        m += 1
        if m == 13:
            y, m = y + 1, 1


def snapshots(rows, label):
    """rows: (entity_id, race_date, won, exp) sorted by date.
    Yields (entity_id, as_of, career..., rolling365...) at month starts."""
    by_ent = defaultdict(list)
    for ent, d, won, exp in rows:
        if ent is not None:
            by_ent[ent].append((d, won, exp))
    print(f"[a2e] {label}: {len(by_ent)} entities")
    out = []
    hi = date.today()
    for ent, runs in by_ent.items():
        runs.sort()
        career_n = career_w = 0
        career_e = 0.0
        window = deque()
        win_n = win_w = 0
        win_e = 0.0
        last100 = deque(maxlen=None)   # trimmed to 100 below
        i = 0
        for snap in month_starts(START_SNAP, hi):
            while i < len(runs) and runs[i][0] < snap:
                d, won, exp = runs[i]
                career_n += 1
                career_w += won
                career_e += exp
                window.append((d, won, exp))
                win_n += 1
                win_w += won
                win_e += exp
                last100.append((won, exp))
                if len(last100) > 100:
                    last100.popleft()
                i += 1
            while window and (snap - window[0][0]).days > 365:
                d, won, exp = window.popleft()
                win_n -= 1
                win_w -= won
                win_e -= exp
            if career_n >= MIN_RUNS:
                l_n = len(last100)
                l_w = sum(w for w, _ in last100)
                l_e = sum(e for _, e in last100)
                out.append((label, ent, snap, career_n, career_w,
                            round(career_e, 3),
                            round(career_w / career_e, 3) if career_e > 0 else None,
                            win_n, win_w, round(win_e, 3),
                            round(win_w / win_e, 3) if win_e > 0 else None,
                            round(100.0 * win_w / win_n, 1) if win_n else None,
                            l_n, l_w,
                            round(l_w / l_e, 3) if l_e > 0 else None,
                            round(100.0 * l_w / l_n, 1) if l_n else None))
    return out


def main() -> int:
    url = os.environ.get("RACING_DB_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, connect_timeout=30)
    cur = conn.cursor()
    # staging + atomic swap — never leave the parked fallback tableless
    cur.execute("DROP TABLE IF EXISTS lifeboat_connection_a2e_new")
    conn.commit()
    cur.execute("""
        CREATE TABLE lifeboat_connection_a2e_new (
            entity_type TEXT NOT NULL,
            entity_key TEXT NOT NULL,
            as_of DATE NOT NULL,
            career_runs INT, career_wins INT, career_exp REAL, career_a2e REAL,
            runs_365 INT, wins_365 INT, exp_365 REAL, a2e_365 REAL, sr_365 REAL,
            runs_100 INT, wins_100 INT, a2e_100 REAL, sr_100 REAL,
            PRIMARY KEY (entity_type, entity_key, as_of))""")
    conn.commit()

    print("[a2e] pulling jockey runs…")
    cur.execute("""
        SELECT j.name, r.race_date,
               CASE WHEN rr.position = 1 THEN 1 ELSE 0 END,
               (1.0 / rr.odds_closing)::float8
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        JOIN jockeys j ON j.jockey_id = rr.jockey_id
        WHERE r.race_date >= '2015-01-01'
          AND COALESCE(rr.is_trial, false) = false
          AND rr.odds_closing > 1
          AND rr.position IS NOT NULL""")
    jock = snapshots([(canon(nm), d, w, e) for nm, d, w, e in cur.fetchall()],
                     "jockey")

    print("[a2e] pulling trainer runs (current-trainer attribution — declared v1 proxy)…")
    cur.execute("""
        SELECT tr.name, r.race_date,
               CASE WHEN rr.position = 1 THEN 1 ELSE 0 END,
               (1.0 / rr.odds_closing)::float8
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        JOIN horses h ON h.horse_id = rr.horse_id
        JOIN trainers tr ON tr.trainer_id = h.trainer_id
        WHERE r.race_date >= '2015-01-01'
          AND COALESCE(rr.is_trial, false) = false
          AND rr.odds_closing > 1
          AND rr.position IS NOT NULL""")
    trn = snapshots([(canon(nm), d, w, e) for nm, d, w, e in cur.fetchall()],
                    "trainer")

    rows = jock + trn
    print(f"[a2e] inserting {len(rows)} snapshot rows…")
    psycopg2.extras.execute_values(cur, """
        INSERT INTO lifeboat_connection_a2e_new
            (entity_type, entity_key, as_of, career_runs, career_wins,
             career_exp, career_a2e, runs_365, wins_365, exp_365, a2e_365, sr_365,
             runs_100, wins_100, a2e_100, sr_100)
        VALUES %s""", rows, page_size=2000)
    cur.execute("DROP TABLE IF EXISTS lifeboat_connection_a2e")
    cur.execute("ALTER TABLE lifeboat_connection_a2e_new "
                "RENAME TO lifeboat_connection_a2e")
    conn.commit()
    cur.execute("SELECT entity_type, count(DISTINCT entity_key), count(*) FROM lifeboat_connection_a2e GROUP BY 1")
    for r in cur.fetchall():
        print(f"[a2e] {r[0]}: {r[1]} entities, {r[2]} PIT rows")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
