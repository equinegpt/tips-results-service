#!/usr/bin/env python3
"""The product question: does (Clone R1 ∩ 8F AI Best) behave like the
old crown/consensus cohort (Clone R1 ∩ Gemini AI_BEST) did?

The 8F Analyst's actual job is INDEPENDENT CONSENSUS LEG — crowns, the
C lane, the danger-lay pool. Standalone strike matters less than what
the agreement cohort earns. Compares, on the same window:

  A. Clone R1 ∩ 8F AI Best        (the future)
  B. Clone R1 ∩ Gemini AI_BEST    (the past, recorded)

Settled at SP from ra_results / tip_outcomes respectively.

Usage: SHARED_DATABASE_URL=... TRS_DATABASE_URL=... \
    python scripts/analyst_consensus_sim.py [start] [end]
Reads output/analyst_backtest_tips.csv for the 8F side.
"""
from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2


def main():
    start = sys.argv[1] if len(sys.argv) > 1 else "2026-06-01"
    end = sys.argv[2] if len(sys.argv) > 2 else "2026-08-03"

    shared = psycopg2.connect(os.environ["SHARED_DATABASE_URL"], connect_timeout=20)
    scur = shared.cursor()
    # Clone R1 per race: latest rank_v1 run per (meeting, race), rank 1.
    scur.execute("""
        WITH latest AS (
            SELECT meeting_id, race_number, tab_number, meeting_date::date AS d,
                   ROW_NUMBER() OVER (PARTITION BY meeting_date, meeting_id, race_number
                                      ORDER BY run_ts DESC) AS rn
            FROM shadow_predictions
            WHERE model_name = 'rank_v1' AND predicted_rank = 1
              AND meeting_date::date BETWEEN %s AND %s)
        SELECT d, meeting_id, race_number, tab_number FROM latest WHERE rn = 1""",
        (start, end))
    clone_r1 = {(str(d), int(m), int(r)): int(t) for d, m, r, t in scur.fetchall()}
    print(f"Clone R1 races in window: {len(clone_r1)}")

    # A. 8F side from the backtest CSV
    tips_csv = Path(__file__).resolve().parent.parent / "output" / "analyst_backtest_tips.csv"
    a_n = a_w = 0
    a_ret = 0.0
    for row in csv.DictReader(open(tips_csv)):
        if row["tip_type"] != "AI_BEST" or row["void"] == "1":
            continue
        if not (start <= row["date"] <= end):
            continue
        key = (row["date"], int(row["pf_meeting_id"]), int(row["race"]))
        if clone_r1.get(key) == int(row["tab"]):
            a_n += 1
            if row["fin"] == "1":
                a_w += 1
                a_ret += float(row["sp"])
    print(f"\nA. Clone R1 ∩ 8F AI Best  ({start} → {end}):")
    if a_n:
        print(f"   n={a_n} strike={100*a_w/a_n:.1f}% ROI@SP={100*(a_ret-a_n)/a_n:+.1f}%")

    # B. recorded Gemini side
    trs = psycopg2.connect(os.environ["TRS_DATABASE_URL"], connect_timeout=20)
    tcur = trs.cursor()
    tcur.execute("""
        SELECT m.date, m.pf_meeting_id, r.race_number, t.tab_number,
               o.finish_position, o.starting_price, o.outcome_status
        FROM tips t
        JOIN tip_runs tr ON tr.id = t.tip_run_id
        JOIN races r ON r.id = t.race_id
        JOIN meetings m ON m.id = tr.meeting_id
        LEFT JOIN tip_outcomes o ON o.tip_id = t.id
        WHERE tr.source = 'Gemini' AND t.tip_type = 'AI_BEST'
          AND m.date BETWEEN %s AND %s""", (start, end))
    b_n = b_w = 0
    b_ret = 0.0
    for d, pfm, rn, tab, fin, sp, status in tcur.fetchall():
        if status not in ("WIN", "LOSE", "PLACE") or not sp:
            continue
        if clone_r1.get((str(d), int(pfm), int(rn))) == int(tab):
            b_n += 1
            if fin == 1:
                b_w += 1
                b_ret += float(sp)
    print(f"\nB. Clone R1 ∩ Gemini AI_BEST (recorded, same window):")
    if b_n:
        print(f"   n={b_n} strike={100*b_w/b_n:.1f}% ROI@SP={100*(b_ret-b_n)/b_n:+.1f}%")
    shared.close()
    trs.close()


if __name__ == "__main__":
    sys.exit(main())
