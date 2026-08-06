#!/usr/bin/env python3
"""Lifeboat Phase C — fit + judge.

Reads the sharded per-runner CSVs from lifeboat_backtest.py, fits a
conditional logit on the TUNE window (same math and conventions as
jennifer_refit: scale to sect=4.0, clamp negatives), then judges on the
untouched HOLDOUT window:

  1. shipped Jennifer code weights on lifeboat features  (zero-fit)
  2. lifeboat-refit weights on lifeboat features         (the lifeboat)

The line to beat is live Jennifer on PF data: 22.3% / -11.0% ROI.
Also slices by sectional coverage (has_sect) — races where the top
pick had no class figures are the par-model's blind spot.

Usage:
  python scripts/lifeboat/lifeboat_fit.py \
      '/path/lb_tune_*.csv' '/path/lb_hold_*.csv'
"""
from __future__ import annotations

import csv
import glob
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
from app.analyst.scorer import W as CODE_W  # noqa: E402

FEATS = [f"z_{k}" for k in CODE_W]


def load(pattern):
    """→ {race_id: [(zvec, won, sp, has_sect)]} — races with exactly 1 winner."""
    races = defaultdict(list)
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="") as fh:
            for row in csv.DictReader(fh):
                races[row["race_id"]].append((
                    np.array([float(row[f]) for f in FEATS]),
                    int(row["won"]),
                    float(row["sp"]) if row["sp"] else None,
                    int(row["has_sect"])))
    return {k: v for k, v in races.items()
            if sum(won for _, won, _, _ in v) == 1 and len(v) >= 5}


def fit(races, iters=400, lr=0.05, l2=1e-3):
    w = np.array([CODE_W[k[2:]] for k in FEATS], dtype=float)
    w /= np.linalg.norm(w)
    keys = list(races)
    for _ in range(iters):
        grad = np.zeros(len(FEATS))
        for k in keys:
            Z = np.stack([z for z, _, _, _ in races[k]])
            y = np.array([won for _, won, _, _ in races[k]], dtype=float)
            s = Z @ w
            s -= s.max()
            p = np.exp(s)
            p /= p.sum()
            grad += Z[int(y.argmax())] - p @ Z
        grad -= l2 * w * len(keys)
        w += lr * grad / len(keys)
    scale = 4.0 / w[0] if w[0] > 0 else 1.0
    w = np.maximum(w * scale, 0.0)
    return {k[2:]: round(float(v), 3) for k, v in zip(FEATS, w)}


def evaluate(races, weights, label):
    wv = np.array([weights[k[2:]] for k in FEATS])
    n = wins = 0
    roi = 0.0
    cov = {0: [0, 0, 0.0], 1: [0, 0, 0.0]}   # has_sect → [n, wins, roi]
    for rows in races.values():
        scores = [float(z @ wv) for z, _, _, _ in rows]
        top = rows[int(np.argmax(scores))]
        _, won, sp, has_sect = top
        if sp is None:
            continue
        n += 1
        wins += won
        roi += (sp - 1) if won else -1
        cov[has_sect][0] += 1
        cov[has_sect][1] += won
        cov[has_sect][2] += (sp - 1) if won else -1
    print(f"  {label:38s} {wins}/{n} = {100*wins/max(n,1):.1f}%  "
          f"roi {100*roi/max(n,1):+.1f}%")
    for hs, (cn, cw, cr) in cov.items():
        if cn:
            print(f"      {'with class figs' if hs else 'NO class figs '}: "
                  f"{cw}/{cn} = {100*cw/cn:.1f}%  roi {100*cr/cn:+.1f}%")
    return wins, n, roi


def main():
    tune = load(sys.argv[1])
    hold = load(sys.argv[2])
    print(f"TUNE {len(tune)} races | HOLDOUT {len(hold)} races")
    print(f"\ncode weights: {CODE_W}")
    fitted = fit(tune)
    print(f"lifeboat fit: {fitted}")
    print("\n=== TUNE (in-sample, sanity only) ===")
    evaluate(tune, CODE_W, "code weights on lifeboat features")
    evaluate(tune, fitted, "lifeboat-fit weights")
    print("\n=== HOLDOUT (the verdict; live Jennifer = 22.3% / -11.0%) ===")
    evaluate(hold, CODE_W, "code weights on lifeboat features")
    evaluate(hold, fitted, "lifeboat-fit weights")
    return 0


if __name__ == "__main__":
    sys.exit(main())
