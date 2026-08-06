#!/usr/bin/env python3
"""Lifeboat Phase B.1 — own sectional par benchmarks from racing-db.

Replaces PF's proprietary class-adjusted sectional figures with pars
fitted on OUR results history: median last-600m / last-200m time per
(track, distance bucket, condition group, class group), with parent
levels for sparse cells. A run's figure = (par − time) / 0.17s per
length → positive = faster than par, same shape as Jennifer's sect
component after her sign convention.

Writes racing-db table `lifeboat_sectional_pars` (all levels in one
table; the feature loader resolves most-specific-with-support first).
Window: 2019+ non-trial runs (~1.3M with times). Re-run monthly-ish;
pars drift slowly.

Usage: RACING_DB_URL=... python scripts/lifeboat/build_pars.py
"""
from __future__ import annotations

import os
import sys

import psycopg2

# SQL-side normalisers shared with the feature loader (keep in sync).
COND_CASE = """CASE
    WHEN track_condition ILIKE 'heavy%%' THEN 'heavy'
    WHEN track_condition ILIKE 'soft%%' THEN 'soft'
    WHEN track_condition ILIKE 'synth%%' THEN 'synthetic'
    WHEN track_condition IS NULL THEN 'good'
    ELSE 'good' END"""

CLASS_CASE = """CASE
    WHEN race_class ILIKE '%%group%%' OR race_class ILIKE '%%listed%%'
         OR race_class ~* '^G[123]' THEN 'stakes'
    WHEN race_class ILIKE '%%maiden%%' OR race_class ILIKE 'mdn%%' THEN 'maiden'
    WHEN race_class ~* '(^|[^0-9])(CL|Class )?\\s*[123]([^0-9]|$)'
         AND (race_class ILIKE 'cl%%' OR race_class ILIKE 'class%%') THEN 'class123'
    WHEN race_class ~* '(BM|Benchmark)\\s*([0-9]+)' THEN
        CASE WHEN (substring(race_class from '(?:BM|Benchmark)\\s*([0-9]+)'))::int <= 60
             THEN 'bm_low'
             WHEN (substring(race_class from '(?:BM|Benchmark)\\s*([0-9]+)'))::int <= 74
             THEN 'bm_mid'
             ELSE 'bm_high' END
    WHEN race_class ILIKE '%%open%%' OR race_class ILIKE '%%hcp%%'
         OR race_class ILIKE '%%handicap%%' THEN 'open'
    ELSE 'other' END"""

LEVELS = [
    # (level name, group-by columns beyond the always-on dist bucket)
    ("track_dist_cond_class", "track_id, dist_bucket, cond_group, class_group"),
    ("track_dist_cond", "track_id, dist_bucket, cond_group"),
    ("track_dist", "track_id, dist_bucket"),
    ("dist_cond_class", "dist_bucket, cond_group, class_group"),
    ("dist_cond", "dist_bucket, cond_group"),
    ("dist", "dist_bucket"),
]


def main() -> int:
    url = os.environ.get("RACING_DB_URL") or os.environ["DATABASE_URL"]
    conn = psycopg2.connect(url, connect_timeout=30)
    cur = conn.cursor()
    # build into a staging table, swap atomically at the end — a parked
    # fallback must never be left tableless by a mid-run failure
    cur.execute("DROP TABLE IF EXISTS lifeboat_sectional_pars_new")
    cur.execute("""
        CREATE TABLE lifeboat_sectional_pars_new (
            level TEXT NOT NULL,
            track_id INT,
            dist_bucket INT,
            cond_group TEXT,
            class_group TEXT,
            n INT NOT NULL,
            par_600 REAL,
            par_200 REAL,
            built_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
    conn.commit()

    base = f"""
        WITH runs AS (
            SELECT r.track_id,
                   (round(r.distance / 200.0) * 200)::int AS dist_bucket,
                   {COND_CASE} AS cond_group,
                   {CLASS_CASE} AS class_group,
                   rr.last_600m, rr.last_200m
            FROM race_results rr
            JOIN races r ON r.race_id = rr.race_id
            WHERE r.race_date >= '2019-01-01'
              AND COALESCE(rr.is_trial, false) = false
              AND rr.last_600m BETWEEN 28 AND 48
        )
    """
    total = 0
    for level, cols in LEVELS:
        sel_cols = cols.split(", ")
        null_cols = [c for c in ("track_id", "dist_bucket", "cond_group", "class_group")
                     if c not in sel_cols]
        types = {"track_id": "int", "dist_bucket": "int",
                 "cond_group": "text", "class_group": "text"}
        select_nulls = "".join(f", NULL::{types[c]} AS {c}" for c in null_cols)
        cur.execute(base + f"""
            INSERT INTO lifeboat_sectional_pars_new
                (level, track_id, dist_bucket, cond_group, class_group, n, par_600, par_200)
            SELECT '{level}', track_id, dist_bucket, cond_group, class_group,
                   count(*),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY last_600m),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY last_200m)
            FROM (SELECT {cols}{select_nulls}, last_600m, last_200m FROM runs) q
            GROUP BY track_id, dist_bucket, cond_group, class_group
            HAVING count(*) >= 20""")
        conn.commit()
        cur.execute("SELECT count(*) FROM lifeboat_sectional_pars_new WHERE level = %s", (level,))
        n = cur.fetchone()[0]
        total += n
        print(f"[pars] {level}: {n} cells")
    # atomic swap: readers see the old table until the instant the new
    # one is complete
    cur.execute("DROP TABLE IF EXISTS lifeboat_sectional_pars")
    cur.execute("ALTER TABLE lifeboat_sectional_pars_new RENAME TO lifeboat_sectional_pars")
    cur.execute("""CREATE INDEX IF NOT EXISTS ix_lifeboat_pars
        ON lifeboat_sectional_pars (level, track_id, dist_bucket, cond_group, class_group)""")
    conn.commit()
    print(f"[pars] done — {total} par cells")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
