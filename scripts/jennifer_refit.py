#!/usr/bin/env python3
"""Jennifer's weekly self-refit — Clone/NoMugs cadence, kept independent.

Every Sunday evening:
  1. Regenerate runner features over a rolling 16-week window (via
     analyst_backtest.py FEATURES_CSV mode — raw PF data + real results
     ONLY; nothing from SkyNet/Clone/NoMugs ever enters the fit).
  2. Fit candidate weights (per-race conditional logit) on the window
     MINUS the last 28 days.
  3. Validate BOTH the incumbent weights and the candidate on those
     untouched last 28 days (top-1 strike + ROI@SP).
  4. THE GUARD: ship the candidate only if it is no worse on BOTH
     metrics and better on at least one. Otherwise keep the incumbent.
     Either way, 8F-Ops gets the numbers.
  5. Shipping = insert an approved row into jennifer_weights; the daily
     generator picks it up on its next run (no deploy, version stamped
     on every tip_run).

Env: SHARED_DATABASE_URL (payload archive), DATABASE_URL (TRS),
     OPS_NTFY_TOPIC (default 8F-Ops).
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import psycopg2

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.analyst.scorer import W as CODE_W  # noqa: E402

FEATS = ["z_sect", "z_speed", "z_map", "z_dist", "z_track", "z_cond", "z_trend", "z_conn"]
WINDOW_WEEKS = 16
VALIDATE_DAYS = 28
OPS_TOPIC = os.environ.get("OPS_NTFY_TOPIC", "8F-Ops")


def ntfy(title: str, body: str, priority: int = 3) -> None:
    try:
        req = urllib.request.Request(
            "https://ntfy.sh", method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"topic": OPS_TOPIC, "title": title,
                             "message": body, "priority": priority}).encode())
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        print(f"[refit] ntfy failed: {e}")


def load_races(path, lo, hi):
    races = defaultdict(list)
    for r in csv.DictReader(open(path)):
        if not (lo <= r["date"] <= hi):
            continue
        z = np.array([float(r[f]) for f in FEATS])
        races[(r["date"], r["pf_meeting_id"], r["race"])].append(
            (z, r["won"] == "1", float(r["sp"])))
    return {k: v for k, v in races.items()
            if len(v) >= 4 and any(w for _, w, _ in v)}


def fit(races, iters=400, lr=0.05, l2=1e-3):
    w = np.array([CODE_W[k[2:]] for k in FEATS], dtype=float)
    w /= np.linalg.norm(w)
    keys = list(races)
    for _ in range(iters):
        grad = np.zeros(len(FEATS))
        for k in keys:
            Z = np.stack([z for z, _, _ in races[k]])
            y = np.array([won for _, won, _ in races[k]], dtype=float)
            s = Z @ w
            s -= s.max()
            p = np.exp(s)
            p /= p.sum()
            grad += Z[int(y.argmax())] - p @ Z
        grad -= l2 * w * len(keys)
        w += lr * grad / len(keys)
    # scale to sect=4.0 like the incumbents; clamp tiny negatives to 0
    scale = 4.0 / w[0] if w[0] > 0 else 1.0
    w = np.maximum(w * scale, 0.0)
    return {k[2:]: round(float(v), 3) for k, v in zip(FEATS, w)}


def evaluate(races, weights: dict):
    wv = np.array([weights.get(k[2:], 0.0) for k in FEATS])
    n = wins = 0
    ret = 0.0
    for runners in races.values():
        Z = np.stack([z for z, _, _ in runners])
        pick = runners[int(np.argmax(Z @ wv))]
        n += 1
        if pick[1]:
            wins += 1
            ret += pick[2]
    return (100 * wins / n if n else 0.0,
            100 * (ret - n) / n if n else 0.0, n)


def main() -> int:
    today = date.today()
    start = (today - timedelta(weeks=WINDOW_WEEKS)).isoformat()
    val_start = (today - timedelta(days=VALIDATE_DAYS)).isoformat()
    end = (today - timedelta(days=1)).isoformat()
    feats_csv = ROOT / "output" / "refit_features.csv"
    (ROOT / "output").mkdir(exist_ok=True)

    print(f"[refit] window {start} → {end} (validate ≥ {val_start})")
    env = dict(os.environ)
    env["FEATURES_CSV"] = str(feats_csv)
    env.setdefault("TRS_DATABASE_URL", os.environ.get("DATABASE_URL", ""))
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "analyst_backtest.py"), start, end],
        env=env, cwd=str(ROOT), capture_output=True, text=True, timeout=7200)
    if r.returncode != 0:
        ntfy("⛔ Jennifer refit FAILED (feature pass)", r.stderr[-400:], 5)
        return 1

    train = load_races(feats_csv, start, (date.fromisoformat(val_start)
                                          - timedelta(days=1)).isoformat())
    val = load_races(feats_csv, val_start, end)
    print(f"[refit] train races={len(train)} validate races={len(val)}")
    if len(train) < 800 or len(val) < 250:
        ntfy("⚠️ Jennifer refit skipped — thin data",
             f"train={len(train)} validate={len(val)}", 4)
        return 0

    # incumbent = latest approved row, else code defaults
    trs = psycopg2.connect(os.environ.get("TRS_DATABASE_URL")
                           or os.environ["DATABASE_URL"], connect_timeout=20)
    cur = trs.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS jennifer_weights (
        version SERIAL PRIMARY KEY,
        weights JSONB NOT NULL,
        metrics JSONB,
        approved BOOLEAN NOT NULL DEFAULT true,
        fitted_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
    trs.commit()
    cur.execute("""SELECT version, weights FROM jennifer_weights
        WHERE approved ORDER BY version DESC LIMIT 1""")
    row = cur.fetchone()
    incumbent = (row[1] if isinstance(row[1], dict) else json.loads(row[1])) if row else dict(CODE_W)
    inc_ver = f"v{row[0]}" if row else "v1-code"

    candidate = fit(train)
    inc_s, inc_roi, n = evaluate(val, incumbent)
    cand_s, cand_roi, _ = evaluate(val, candidate)

    ship = (cand_s >= inc_s and cand_roi >= inc_roi
            and (cand_s > inc_s or cand_roi > inc_roi))
    body = (f"Validation (last {VALIDATE_DAYS}d, {n} races):\n"
            f"incumbent {inc_ver}: {inc_s:.1f}% / {inc_roi:+.1f}%\n"
            f"candidate: {cand_s:.1f}% / {cand_roi:+.1f}%\n"
            f"decision: {'SHIP candidate' if ship else 'KEEP incumbent'}\n"
            f"candidate weights: {candidate}")
    print(f"[refit] {body}")
    if ship:
        cur.execute("""INSERT INTO jennifer_weights (weights, metrics, approved)
            VALUES (%s, %s, true) RETURNING version""",
            (json.dumps(candidate),
             json.dumps({"val_strike": cand_s, "val_roi": cand_roi,
                         "vs_incumbent": {"strike": inc_s, "roi": inc_roi},
                         "train_races": len(train), "val_races": n,
                         "window": [start, end]})))
        ver = cur.fetchone()[0]
        trs.commit()
        ntfy(f"✅ Jennifer refit SHIPPED v{ver}", body, 3)
    else:
        ntfy("Jennifer refit: kept incumbent", body, 2)
    trs.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
