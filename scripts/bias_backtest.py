#!/usr/bin/env python3
"""Intra-day track-bias backtest — the go/no-go gate for live re-tipping.

Question: when a meeting's early winners share a running-style/barrier
profile, does adjusting Jennifer's picks for the LATER races at that
meeting improve them?

Three stages, in order of scientific honesty:

  1. PERSISTENCE DIAGNOSTIC — is winner profile in the first half of a
     meeting correlated with the second half AT ALL? If not, "bias"
     isn't a persistent phenomenon in this data and the project dies
     here, before any strategy is fitted.
  2. GRID SEARCH on TUNE (Apr-May): thresholds + adjustment strength,
     scored on the races where the adjustment actually changed the pick.
  3. One HOLDOUT read (Jun-Aug) of the single best tune config.

Inputs: output/analyst_features_v2.csv (runner-level: z components,
won, sp, barrier, settle estimate, field size) from analyst_backtest.py.
All in-house; zero external calls.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

FEATS = ["z_sect", "z_speed", "z_map", "z_dist", "z_track", "z_cond", "z_trend", "z_conn"]
W = np.array([4.0, 0.0, 1.9, 0.9, 2.1, 1.45, 0.3, 2.3])
TUNE_END = "2026-05-31"


def load(path):
    meetings = defaultdict(dict)   # (date, mid) -> {race_no: [runner dicts]}
    for r in csv.DictReader(open(path)):
        runner = {
            "tab": int(r["tab"]),
            "z": np.array([float(r[f]) for f in FEATS]),
            "won": r["won"] == "1",
            "sp": float(r["sp"]),
            "settle_pct": (float(r["settle"]) / float(r["field"])
                           if r["settle"] and float(r["field"]) > 0 else None),
            "barrier_pct": (float(r["barrier"]) / float(r["field"])
                            if r["barrier"] and float(r["field"]) > 0 else None),
        }
        meetings[(r["date"], r["pf_meeting_id"])].setdefault(int(r["race"]), []).append(runner)
    # keep meetings with 5+ races and winners known
    out = {}
    for k, races in meetings.items():
        ok = {rn: rs for rn, rs in races.items() if any(x["won"] for x in rs)}
        if len(ok) >= 5:
            out[k] = dict(sorted(ok.items()))
    return out


def winner_profile(runners):
    w = next((x for x in runners if x["won"]), None)
    if w is None:
        return None, None
    return w["settle_pct"], w["barrier_pct"]


# ---------------------------------------------------- 1. persistence
def persistence(meetings, label):
    """Correlate mean winner on-paceness: first half vs second half."""
    a, b = [], []
    for races in meetings.values():
        rns = list(races)
        half = len(rns) // 2
        f = [winner_profile(races[rn])[0] for rn in rns[:half]]
        s = [winner_profile(races[rn])[0] for rn in rns[half:]]
        f = [x for x in f if x is not None]
        s = [x for x in s if x is not None]
        if len(f) >= 2 and len(s) >= 2:
            a.append(np.mean(f))
            b.append(np.mean(s))
    a, b = np.array(a), np.array(b)
    r = float(np.corrcoef(a, b)[0, 1]) if len(a) > 10 else float("nan")
    se = 1.0 / np.sqrt(max(len(a) - 3, 1))
    print(f"  {label}: n={len(a)} meetings | corr(first-half winner settle, "
          f"second-half) = {r:+.3f} (±{se:.3f} SE)")
    # barrier version
    a2, b2 = [], []
    for races in meetings.values():
        rns = list(races)
        half = len(rns) // 2
        f = [winner_profile(races[rn])[1] for rn in rns[:half]]
        s = [winner_profile(races[rn])[1] for rn in rns[half:]]
        f = [x for x in f if x is not None]
        s = [x for x in s if x is not None]
        if len(f) >= 2 and len(s) >= 2:
            a2.append(np.mean(f))
            b2.append(np.mean(s))
    r2 = float(np.corrcoef(a2, b2)[0, 1]) if len(a2) > 10 else float("nan")
    print(f"  {label}: n={len(a2)} meetings | corr(first-half winner barrier, "
          f"second-half) = {r2:+.3f}")
    return r, r2


# ---------------------------------------------------- 2/3. strategy replay
def replay(meetings, k_min, thresh, beta):
    """Returns (switched picks: n, wins, ret) + (same for baseline on the
    same switched races)."""
    sw_n = sw_w = 0
    sw_ret = 0.0
    bl_w = 0
    bl_ret = 0.0
    fired_meetings = 0
    for races in meetings.values():
        pace_evidence = []
        fired_here = False
        for rn, runners in races.items():
            # adjustment from evidence so far
            adj_applied = False
            if len(pace_evidence) >= k_min:
                sig = float(np.mean([0.5 - s for s in pace_evidence]))
                if abs(sig) >= thresh:
                    adj_applied = True
            base_scores = np.array([float(x["z"] @ W) for x in runners])
            base_pick = runners[int(np.argmax(base_scores))]
            if adj_applied:
                adj = np.array([
                    beta * sig * (0.5 - x["settle_pct"]) if x["settle_pct"] is not None else 0.0
                    for x in runners])
                pick = runners[int(np.argmax(base_scores + adj))]
                if pick["tab"] != base_pick["tab"]:
                    fired_here = True
                    sw_n += 1
                    if pick["won"]:
                        sw_w += 1
                        sw_ret += pick["sp"]
                    if base_pick["won"]:
                        bl_w += 1
                        bl_ret += base_pick["sp"]
            sp_w, _ = winner_profile(runners)
            if sp_w is not None:
                pace_evidence.append(sp_w)
        if fired_here:
            fired_meetings += 1
    return sw_n, sw_w, sw_ret, bl_w, bl_ret, fired_meetings


def report(tag, sw_n, sw_w, sw_ret, bl_w, bl_ret, fm):
    if not sw_n:
        print(f"  {tag}: never fired")
        return
    print(f"  {tag}: switched {sw_n} picks across {fm} meetings")
    print(f"    biased pick : strike={100*sw_w/sw_n:5.1f}% ROI@SP={100*(sw_ret-sw_n)/sw_n:+6.1f}%")
    print(f"    baseline    : strike={100*bl_w/sw_n:5.1f}% ROI@SP={100*(bl_ret-sw_n)/sw_n:+6.1f}%")


def main():
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "output/analyst_features_v2.csv")
    meetings = load(path)
    tune = {k: v for k, v in meetings.items() if k[0] <= TUNE_END}
    hold = {k: v for k, v in meetings.items() if k[0] > TUNE_END}
    print(f"meetings: total={len(meetings)} tune={len(tune)} holdout={len(hold)}")

    print("\n== 1. PERSISTENCE DIAGNOSTIC (does bias even persist?) ==")
    r_all, _ = persistence(meetings, "ALL")
    persistence(tune, "TUNE")
    persistence(hold, "HOLDOUT")
    if not np.isnan(r_all) and abs(r_all) < 0.05:
        print("  >>> near-zero persistence — expect the strategy to fail; "
              "grids below are the confirmation, not the hope.")

    print("\n== 2. GRID on TUNE (scored on switched picks only) ==")
    best = None
    for k_min in (2, 3, 4):
        for thresh in (0.05, 0.08, 0.12, 0.16):
            for beta in (2.0, 4.0, 8.0):
                sw_n, sw_w, sw_ret, bl_w, bl_ret, fm = replay(tune, k_min, thresh, beta)
                if sw_n < 40:
                    continue
                delta_roi = ((sw_ret - sw_n) - (bl_ret - sw_n)) / sw_n
                tag = f"k>={k_min} T={thresh} beta={beta}"
                print(f"  {tag:24s} n={sw_n:4d} biased ROI "
                      f"{100*(sw_ret-sw_n)/sw_n:+6.1f}% vs base "
                      f"{100*(bl_ret-sw_n)/sw_n:+6.1f}%  (Δ {100*delta_roi:+.1f}pp)")
                if best is None or delta_roi > best[0]:
                    best = (delta_roi, k_min, thresh, beta)
    if best is None:
        print("  no config fired 40+ times on tune — bias too rare to trade")
        return 0

    _, k_min, thresh, beta = best
    print(f"\n== 3. HOLDOUT read (single best tune config: k>={k_min} "
          f"T={thresh} beta={beta}) ==")
    report("HOLDOUT", *replay(hold, k_min, thresh, beta))
    return 0


if __name__ == "__main__":
    sys.exit(main())
