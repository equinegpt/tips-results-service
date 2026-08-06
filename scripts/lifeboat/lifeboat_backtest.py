#!/usr/bin/env python3
"""Lifeboat Phase C — Jennifer-lite backtest, 100% racing-db.

Grades the PF-loss world honestly: every settleable AU race in the
window gets a lifeboat payload (own pars, own PIT A2E, own history),
scored by Jennifer's UNCHANGED scorer, top pick settled against the
recorded finish and closing odds.

Race universe guards (the Phase B smoke lessons):
  - race_number is often NULL on fresh crawl rows → enumerate by
    race_id, never (date, track, number)
  - runner-count quirks → require exactly one recorded winner, field
    5-24, SP present on all but ≤2 runners, non-trial, country AU

Output: per-runner CSV (one row per runner per race) with the 8
z-components + won/sp — the input for the conditional-logit weight
refit (fit on TUNE, judge on HOLDOUT). Inline top-pick strike/ROI with
the shipped code weights prints at the end as the zero-fit baseline.

Usage:
  RACING_DB_URL=... python scripts/lifeboat/lifeboat_backtest.py \
      2026-06-01 2026-08-05 /tmp/lifeboat_holdout.csv [--limit 30]
"""
from __future__ import annotations

import csv
import os
import sys
import time
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts" / "lifeboat"))

from lifeboat_features import LifeboatLoader  # noqa: E402
from app.analyst.scorer import W, score_race  # noqa: E402

Z_KEYS = list(W)  # sect speed map dist track cond trend conn


def enumerate_races(cur, start, end):
    cur.execute("""
        SELECT r.race_id
        FROM races r
        JOIN tracks t ON t.track_id = r.track_id
        JOIN race_results rr ON rr.race_id = r.race_id
        WHERE r.race_date BETWEEN %s AND %s
          AND COALESCE(rr.is_trial, false) = false
          AND (t.country IS NULL OR t.country ILIKE 'aus%%')
        GROUP BY r.race_id
        HAVING count(*) BETWEEN 5 AND 24
           AND count(*) FILTER (WHERE rr.position = 1) = 1
           AND count(rr.odds_closing) >= count(*) - 2
        ORDER BY r.race_id""", (start, end))
    return [r[0] for r in cur.fetchall()]


def main() -> int:
    start, end, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
    limit = None
    if "--limit" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--limit") + 1])
    shard_i, shard_k = 0, 1
    if "--shard" in sys.argv:  # "--shard 3/8" → every 8th race from #3
        shard_i, shard_k = map(int, sys.argv[sys.argv.index("--shard") + 1].split("/"))

    url = os.environ.get("RACING_DB_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, connect_timeout=30)
    races = enumerate_races(conn.cursor(), start, end)
    races = races[shard_i::shard_k]
    if limit:
        races = races[:limit]
    print(f"[bt] {len(races)} settleable AU races {start}..{end}", flush=True)

    loader = LifeboatLoader(conn)
    fh = open(out_path, "w", newline="")
    wcsv = csv.writer(fh)
    wcsv.writerow(["race_id", "race_date", "track", "distance", "field",
                   "tab", "horse_id", "won", "sp", "has_sect"]
                  + [f"z_{k}" for k in Z_KEYS])

    picks = wins = 0
    roi = 0.0
    skipped = 0
    t0 = time.time()
    for n, rid in enumerate(races, 1):
        try:
            payload = loader.build_payload(rid)
        except Exception as exc:  # reconnect once, then skip
            conn.rollback() if not conn.closed else None
            try:
                conn = psycopg2.connect(url, connect_timeout=30)
                loader = LifeboatLoader(conn)
                payload = loader.build_payload(rid)
            except Exception:
                print(f"[bt] skip race {rid}: {exc}", flush=True)
                skipped += 1
                continue
        pl = payload["payLoad"]
        scored = score_race(payload)
        if len(scored) < 5:
            skipped += 1
            continue
        for s in scored:
            r = s["runner"]
            wcsv.writerow([rid, pl.get("_race_date") or "",
                           pl["meeting"]["track"]["name"], pl["distance"],
                           len(scored), s["tab_number"], r["_horse_id"],
                           1 if r["_position"] == 1 else 0,
                           r["_sp"] or "",
                           1 if any(x.get("last600Class") is not None
                                    for x in r["sectionalData"]) else 0]
                          + [s["z"][k] for k in Z_KEYS])
        top = scored[0]["runner"]
        if top["_sp"]:
            picks += 1
            if top["_position"] == 1:
                wins += 1
                roi += top["_sp"] - 1
            else:
                roi -= 1
        if n % 100 == 0 or n == len(races):
            el = time.time() - t0
            print(f"[bt] {n}/{len(races)} | strike {wins}/{picks} "
                  f"({100*wins/max(picks,1):.1f}%) roi {100*roi/max(picks,1):+.1f}% "
                  f"| {el/n:.2f}s/race eta {int(el/n*(len(races)-n)/60)}m",
                  flush=True)
            fh.flush()
    fh.close()
    print(f"[bt] DONE code-weights baseline: {wins}/{picks} "
          f"({100*wins/max(picks,1):.1f}%), roi {100*roi/max(picks,1):+.1f}%, "
          f"skipped {skipped}, csv {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
