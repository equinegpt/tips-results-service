#!/usr/bin/env python3
"""Phase 3 tuner: fit cascade weights from data, TUNE window only.

Input: output/analyst_features.csv (runner-level z components + won/sp,
settled runners only) from analyst_backtest.py FEATURES_CSV mode.

Model: per-race conditional logit — P(win) = softmax(w · z) over the
race's runners; maximise winner log-likelihood by gradient ascent on
the TUNE window (dates <= 2026-05-31). Still a fixed linear formula —
data-informed weights, fully explainable, no black box.

Evaluation: top-1 (AI Best) strike + ROI@SP on TUNE and HOLDOUT for:
  A. baseline hand weights (the shipped scorer)
  B. fitted weights
Prints the weight vector for adoption into scorer.py if B wins.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

TUNE_END = "2026-05-31"
FEATS = ["z_sect", "z_speed", "z_map", "z_dist", "z_track",
         "z_cond", "z_trend", "z_conn"]
BASELINE = np.array([4.0, 2.0, 1.0, 0.8, 0.6, 0.6, 0.4, 0.2])


def load(path):
    races = defaultdict(list)   # (date, mid, race) -> [(z-vec, won, sp)]
    for r in csv.DictReader(open(path)):
        z = np.array([float(r[f]) for f in FEATS])
        races[(r["date"], r["pf_meeting_id"], r["race"])].append(
            (z, int(r["won"]), float(r["sp"])))
    # keep races with a settled winner and 4+ settled runners
    return {k: v for k, v in races.items()
            if len(v) >= 4 and any(won for _, won, _ in v)}


def fit(races, iters=400, lr=0.05, l2=1e-3):
    w = BASELINE.copy() / np.linalg.norm(BASELINE)
    keys = list(races)
    for it in range(iters):
        grad = np.zeros(len(FEATS))
        ll = 0.0
        for k in keys:
            Z = np.stack([z for z, _, _ in races[k]])
            y = np.array([won for _, won, _ in races[k]], dtype=float)
            s = Z @ w
            s -= s.max()
            p = np.exp(s)
            p /= p.sum()
            ll += float(np.log(p[y.argmax()] + 1e-12))
            grad += Z[y.argmax()] - p @ Z
        grad -= l2 * w * len(keys)
        w += lr * grad / len(keys)
        if it % 100 == 99:
            print(f"  iter {it+1}: mean LL {ll/len(keys):.4f}")
    return w


def evaluate(races, w, label):
    n = wins = 0
    ret = 0.0
    for k, runners in races.items():
        Z = np.stack([z for z, _, _ in runners])
        pick = int(np.argmax(Z @ w))
        n += 1
        if runners[pick][1]:
            wins += 1
            ret += runners[pick][2]
    print(f"  {label:26s}: n={n:5d} strike={100*wins/n:5.1f}% "
          f"ROI@SP={100*(ret-n)/n:+6.1f}%")
    return wins / n


def main():
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "output/analyst_features.csv")
    races = load(path)
    tune = {k: v for k, v in races.items() if k[0] <= TUNE_END}
    hold = {k: v for k, v in races.items() if k[0] > TUNE_END}
    print(f"races: total={len(races)} tune={len(tune)} holdout={len(hold)}")

    print("\n== A. baseline hand weights ==")
    evaluate(tune, BASELINE, "TUNE")
    evaluate(hold, BASELINE, "HOLDOUT")

    print("\n== fitting on TUNE only ==")
    w = fit(tune)
    print("fitted weights (scaled to sect=4.0 for comparability):")
    scale = 4.0 / w[0] if w[0] != 0 else 1.0
    for f, v in zip(FEATS, w * scale):
        print(f"  {f:8s}: {v:+.2f}")

    print("\n== B. fitted weights ==")
    evaluate(tune, w, "TUNE (in-sample)")
    evaluate(hold, w, "HOLDOUT (honest read)")


if __name__ == "__main__":
    sys.exit(main())
