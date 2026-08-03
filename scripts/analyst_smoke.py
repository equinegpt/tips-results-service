#!/usr/bin/env python3
"""Smoke test: run the 8F Analyst on archived races and compare to results.

Usage: SHARED_DATABASE_URL=... python scripts/analyst_smoke.py 2026-08-03

Uses the LATEST snapshot taken before race-day 09:00 AEST (23:00 UTC
day prior) — mirrors live morning generation, avoids the post-race
form leak (data contract, lookahead rule).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from app.analyst import score_race, select_tips, reasoning_line  # noqa: E402


def main(day: str) -> int:
    db = os.environ.get("SHARED_DATABASE_URL") or os.environ.get("DATABASE_URL")
    conn = psycopg2.connect(db, connect_timeout=20)
    cur = conn.cursor()

    # latest PRE-RACE-MORNING snapshot per race for the day
    cur.execute("""
        SELECT DISTINCT ON (meeting_id, race_number)
               meeting_id, race_number, payload
        FROM pf_ireel_races
        WHERE meeting_date::date = %s
          AND snapshot_ts::timestamptz <= ((%s::date - interval '1 day') + interval '23 hours') AT TIME ZONE 'UTC'
        ORDER BY meeting_id, race_number, snapshot_ts DESC""", (day, day))
    races = cur.fetchall()
    print(f"{len(races)} races with pre-morning snapshots for {day}")

    # results index (track, race, tab) -> finish
    cur.execute("""SELECT track_norm, race_number, tab_number,
        finishing_position, starting_price
        FROM pick_results WHERE pick_date::date = %s""", (day, ))
    res = {}
    for tn, rn, tab, fin, sp in cur.fetchall():
        res[(tn, rn, tab)] = (fin, float(sp or 0))

    def norm(t):
        return "".join(c for c in (t or "").lower() if c.isalnum())

    hits = n_scored = 0
    for mid, rn, payload in races:
        p = json.loads(payload) if isinstance(payload, str) else payload
        pl = (p or {}).get("payLoad") or {}
        track = ((pl.get("meeting") or {}).get("track") or {}).get("name") or ""
        scored = score_race(p)
        if len(scored) < 4:
            continue
        tips = select_tips(scored)
        ab = tips["ai_best"]
        key = (norm(track), rn, ab["tab_number"])
        r = res.get(key)
        fin = r[0] if r else None
        cond = pl.get("trackCondition") or ""
        mark = "WON" if fin == 1 else (f"{fin}th" if fin else "no result")
        n_scored += 1
        hits += 1 if fin == 1 else 0
        print(f"\n{track} R{rn} ({cond}) — field {len(scored)}")
        print(f"  AI Best: #{ab['tab_number']} {ab['horse_name']} [{mark}]")
        print(f"    {reasoning_line(ab, cond, len(scored), angle='best')}")
        if tips["danger"]:
            d = tips["danger"]
            df = res.get((norm(track), rn, d["tab_number"]))
            print(f"  Danger:  #{d['tab_number']} {d['horse_name']} "
                  f"[{'WON' if df and df[0]==1 else (str(df[0])+'th' if df and df[0] else '?')}]")
        if tips["value"]:
            v = tips["value"]
            vf = res.get((norm(track), rn, v["tab_number"]))
            print(f"  Value:   #{v['tab_number']} {v['horse_name']} "
                  f"[{'WON' if vf and vf[0]==1 else (str(vf[0])+'th' if vf and vf[0] else '?')}]")
    print(f"\nAI Best strike on {day}: {hits}/{n_scored}")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "2026-08-03"))
