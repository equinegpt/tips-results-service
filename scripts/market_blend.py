#!/usr/bin/env python3
"""Jennifer upgrade #1 — the Benter market blend (fit + gates).

Two heads, one discipline:
  PURE Jennifer   — untouched, market-blind. The value-finder.
  BLENDED head    — conditional logit over [jennifer_score, log p_mkt]
                    within each race. The calibrated probability engine.

Market input = the TIMESTAMPED TAB OPEN from racing-db market_prices
(median 19.8h before the 05:00 generation). LEAK RULE: an open price
is usable only if tab_open_time <= race-day 05:00 AEST — later opens
are nulled and their races drop out of blend fit/eval (coverage is
reported, never silently absorbed).

Windows match every prior gate: TUNE <= 2026-05-31, HOLDOUT 2026-06-01+
(untouched). Gates on HOLDOUT:
  1. log-loss: blend must beat BOTH market-only and Jennifer-only
  2. value overlay (EV at the actual open price), settled @SP and
     @BSP net of 8% commission — reported across margins, honest

Usage:
  RACING_DB_URL=... python scripts/market_blend.py \
      [--features output/analyst_features_v2.csv]
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.analyst.scorer import W  # fitted Jennifer weights

TUNE_END = "2026-05-31"
BF_COMM = 0.08          # AU racing exchange commission on winnings


def load(features_path: str) -> pd.DataFrame:
    f = pd.read_csv(features_path, parse_dates=["date"])
    f["score"] = sum(W[k] * f[f"z_{k}"] for k in W)
    conn = psycopg2.connect(os.environ.get("RACING_DB_URL")
                            or os.environ["DATABASE_URL"], connect_timeout=25)
    mp = pd.read_sql("""
        SELECT pf_meeting_id, race_no AS race, tab_no AS tab,
               tab_open_price, tab_open_time, betfair_sp
        FROM market_prices WHERE meeting_date >= '2026-04-01'""", conn)
    conn.close()
    df = f.merge(mp, on=["pf_meeting_id", "race", "tab"], how="left")

    # leak rule: open must exist BEFORE race-day 05:00 AEST
    t = pd.to_datetime(df.tab_open_time, utc=True, errors="coerce")
    gen = (pd.to_datetime(df.date).dt.tz_localize("Australia/Melbourne")
           + pd.Timedelta(hours=5)).dt.tz_convert("UTC")
    late = t.notna() & (t > gen)
    df.loc[late, "tab_open_price"] = np.nan
    print(f"[load] {len(df)} runners; late-open nulled: {late.sum()} "
          f"({100*late.mean():.1f}%)")

    # market prob: overround-normalised within race from the open
    df["inv"] = 1.0 / df.tab_open_price
    grp = df.groupby(["pf_meeting_id", "race"])
    df["p_mkt"] = df.inv / grp.inv.transform("sum")
    # blend uses only races where EVERY runner has a usable open
    df["race_complete"] = grp.tab_open_price.transform(
        lambda s: s.notna().all())
    return df


class Race:
    __slots__ = ("X", "y", "sp", "bsp", "open_")

    def __init__(self, g, cols):
        self.X = g[cols].values
        self.y = g.won.values
        self.sp = g.sp.values
        self.bsp = g.betfair_sp.values
        self.open_ = g.tab_open_price.values


def races_of(df: pd.DataFrame, cols) -> list[Race]:
    out = []
    for _, g in df.groupby(["pf_meeting_id", "race"]):
        if g.won.sum() != 1 or len(g) < 5:
            continue
        out.append(Race(g, cols))
    return out


def fit_clogit(races, sl: slice, iters=2000, lr=0.05, l2=1e-4) -> np.ndarray:
    """Conditional logit on X[:, sl]: maximise log P(winner) per race."""
    k = races[0].X[:, sl].shape[1]
    w = np.zeros(k)
    n = len(races)
    for _ in range(iters):
        grad = np.zeros(k)
        for r in races:
            X = r.X[:, sl]
            s = X @ w
            s -= s.max()
            p = np.exp(s)
            p /= p.sum()
            grad += X[int(r.y.argmax())] - p @ X
        w += lr * (grad / n - l2 * w)
    return w


def probs(X, w):
    s = X @ w
    s -= s.max()
    p = np.exp(s)
    return p / p.sum()


def settle(i, r, agg):
    """Add one $1 bet on runner i to agg = [n, wins, roi_sp, bsp_n, roi_bsp]."""
    s = r.sp[i]
    if not (s and s > 1):
        return
    won = r.y[i] == 1
    agg[0] += 1
    agg[1] += won
    agg[2] += (s - 1) if won else -1
    b = r.bsp[i]
    if b and b > 1:
        agg[3] += 1
        agg[4] += (b - 1) * (1 - BF_COMM) if won else -1


def fmt(agg):
    n, wins, roi_sp, bsp_n, roi_bsp = agg
    return (f"n={n:5d} strike={100*wins/max(n,1):5.1f}% "
            f"roi@SP {100*roi_sp/max(n,1):+6.1f}%  "
            f"roi@BSP(net) {100*roi_bsp/max(bsp_n,1):+6.1f}% (bsp n={bsp_n})")


def evaluate(races, w, cols_slice, label):
    ll = 0.0
    agg = [0, 0, 0.0, 0, 0.0]
    for r in races:
        p = probs(r.X[:, cols_slice], w)
        ll -= np.log(max(p[int(r.y.argmax())], 1e-12))
        settle(int(p.argmax()), r, agg)
    ll /= len(races)
    print(f"  {label:28s} logloss={ll:.4f}  top-pick: {fmt(agg)}")
    return ll


def main() -> int:
    features = "output/analyst_features_v2.csv"
    if "--features" in sys.argv:
        features = sys.argv[sys.argv.index("--features") + 1]
    df = load(features)

    b = df[df.race_complete].copy()
    b["log_pmkt"] = np.log(b.p_mkt)
    # standardise on TUNE stats — the raw score spans ±20 and blows up
    # gradient ascent (first run: negative score weight, logloss 10.9)
    tmask = b.date <= TUNE_END
    for c in ("score", "log_pmkt"):
        mu, sd = b.loc[tmask, c].mean(), b.loc[tmask, c].std()
        b[c] = (b[c] - mu) / sd
    cols = ["score", "log_pmkt"]
    tune = races_of(b[b.date <= TUNE_END], cols)
    hold = races_of(b[b.date > TUNE_END], cols)
    print(f"[fit] complete-open races: tune={len(tune)} holdout={len(hold)}")

    w_jen = fit_clogit(tune, slice(0, 1))     # score only
    w_blend = fit_clogit(tune, slice(0, 2))
    w_mkt = fit_clogit(tune, slice(1, 2))     # log_pmkt only
    print(f"[fit] jennifer w={w_jen.round(3)} market w={w_mkt.round(3)} "
          f"blend w={w_blend.round(3)}")

    print("\n=== HOLDOUT (untouched, Jun 1+) ===")
    ll_j = evaluate(hold, w_jen, slice(0, 1), "Jennifer-only (calibrated)")
    ll_m = evaluate(hold, w_mkt, slice(1, 2), "Market-only (open)")
    ll_b = evaluate(hold, w_blend, slice(0, 2), "BLEND")
    print(f"\n  GATE 1 (log-loss): blend {ll_b:.4f} vs jennifer {ll_j:.4f} "
          f"vs market {ll_m:.4f} → "
          f"{'PASS' if ll_b < min(ll_j, ll_m) else 'FAIL'}")

    print("\n  GATE 2 — value overlay: bet where p_blend × open ≥ 1+m "
          "(EV at the ACTUAL open price)")
    for m in (0.00, 0.10, 0.20, 0.30):
        agg = [0, 0, 0.0, 0, 0.0]
        for r in hold:
            p = probs(r.X, w_blend)
            ev = p * r.open_ - 1
            for i in np.where(ev >= m)[0]:
                settle(i, r, agg)
        print(f"    m={m:.2f}: {fmt(agg)}")

    print("\n  blend top-pick in the validated $2.50-4.00 band:")
    agg = [0, 0, 0.0, 0, 0.0]
    for r in hold:
        p = probs(r.X, w_blend)
        i = int(p.argmax())
        if r.sp[i] and 2.50 <= r.sp[i] < 4.00:
            settle(i, r, agg)
    print(f"    {fmt(agg)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
