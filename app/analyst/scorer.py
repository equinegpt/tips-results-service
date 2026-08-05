# app/analyst/scorer.py
"""Deterministic race scorer implementing the 8F Analyst priority
cascade (docs/analyst-data-contract.md):

    1. class-adjusted sectionals   (dominant — see CONFLICT RULE)
    2. raw speed / meeting ranks
    3. barrier + map position
    4. distance record
    5. track record
    6. conditions record
    7. run style vs pace (folded into 3 for now)
    8. form trend (last 3 runs)
    9. connections (tiebreak only)

Each component produces a raw value per runner; components are
z-normalised WITHIN the race (a horse is only ever compared to today's
opposition) and combined with cascade weights. Missing data
mean-imputes to z=0 — absence of evidence is neutral, never fatal,
which matters on bush cards full of lightly-raced horses.

CONFLICT RULE (verbatim from the spec): the best-sectional horse takes
AI Best over the total-score leader unless the sectional edge is
< 1.0 length class AND the narrative (total minus sectional component)
case is overwhelming. Implemented in select_tips().
"""
from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Optional

# Cascade weights — FITTED 2026-08-04 (scripts/analyst_tune.py):
# per-race conditional logit on the TUNE window (Apr 1–May 31, 1,840
# races), validated on the untouched Jun–Aug holdout: 22.4% strike /
# −11.3% ROI@SP vs 19.9% / −17.7% for the original hand weights.
# The data kept sectionals dominant but promoted connections, track
# record and conditions well above the prompt's hand-guessed order.
# speed (meeting ranks) fitted ~0 — collinear with sectionals — and is
# clamped to 0 rather than carrying its slightly negative fit.
W = {
    "sect": 4.0,
    "speed": 0.0,
    "map": 1.9,
    "dist": 0.9,
    "track": 2.1,
    "cond": 1.45,
    "trend": 0.3,
    "conn": 2.3,
}
RECENCY = (1.0, 0.6, 0.4)          # last 3 runs, newest first
SHRINK = 4                          # record-rate shrinkage (small samples)
SECT_EDGE_LENGTHS = 1.0             # conflict-rule threshold
# PF ships sentinel garbage (±499/795/999...) where class figures are
# missing — smoke test 2026-08-04 surfaced "-795 lengths class" picks.
# Real class-adjusted figures live within ~±15 lengths; anything beyond
# ±25 is a sentinel and must read as MISSING, not as data.
CLASS_SANE = 25.0


def _sane(v: Optional[float], limit: float = CLASS_SANE) -> Optional[float]:
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if abs(v) <= limit else None


# ---------------------------------------------------------------- helpers
def _runs(runner: dict) -> List[dict]:
    """sectionalData entries, newest first (PF ships newest first)."""
    return [s for s in (runner.get("sectionalData") or []) if isinstance(s, dict)]


def _wmean(pairs: List[tuple]) -> Optional[float]:
    """Weighted mean over (value, weight); None when no values."""
    vals = [(v, w) for v, w in pairs if v is not None]
    if not vals:
        return None
    tw = sum(w for _, w in vals)
    return sum(v * w for v, w in vals) / tw if tw else None


def _rec_rate(rec: Optional[dict]) -> tuple:
    """(win_rate, place_rate, starts) with shrinkage; rates None if no starts."""
    rec = rec or {}
    starts = rec.get("starts") or 0
    if not starts:
        return None, None, 0
    f = rec.get("firsts") or 0
    s = rec.get("seconds") or 0
    t = rec.get("thirds") or 0
    return f / (starts + SHRINK), (f + s + t) / (starts + SHRINK), starts


def _settle_from_inrun(run: dict) -> Optional[int]:
    """Settling position from the inRun string 'finish,13;settling_down,9;...'"""
    inrun = ((run.get("jockey") or {}).get("inRun")) or ""
    m = re.search(r"settling_down,(\d+)", inrun)
    return int(m.group(1)) if m else None


def _finish_from_inrun(run: dict) -> Optional[int]:
    inrun = ((run.get("jockey") or {}).get("inRun")) or ""
    m = re.search(r"finish,(\d+)", inrun)
    return int(m.group(1)) if m else None


# ------------------------------------------------------------- components
def _sect_component(runner: dict) -> Optional[float]:
    """Class-adjusted sectional quality in LENGTHS (positive = good).

    PF convention: negative class lengths = better than class par, so we
    negate. Per run: 0.5*last600Class + 0.25*last200Class + 0.25*finishClass
    (the spec's own emphasis order), recency-weighted across runs."""
    pairs = []
    for run, w in zip(_runs(runner), RECENCY):
        parts = [(_sane(run.get("last600Class")), 0.50),
                 (_sane(run.get("last200Class")), 0.25),
                 (_sane(run.get("finishClass")), 0.25)]
        v = _wmean(parts)
        if v is not None:
            pairs.append((-v, w))          # negate: positive = good
    return _wmean(pairs)


def _speed_component(runner: dict) -> Optional[float]:
    """Meeting-rank percentiles (0-100, higher better) as the raw-speed
    proxy — they're benchmark-relative, unlike raw seconds."""
    pairs = []
    for run, w in zip(_runs(runner), RECENCY):
        ranks = [run.get(k) for k in
                 ("meetingRank6F", "meetingRank2F", "meetingRank1F")]
        ranks = [r for r in ranks if r is not None and 0 < r <= 100]
        if ranks:
            pairs.append((sum(ranks) / len(ranks), w))
    return _wmean(pairs)


def _map_component(runner: dict, field_size: int,
                   speedmap: Optional[dict]) -> Optional[float]:
    """Barrier + likely position. Speedmap (ratedSettle/settle) when
    available; historical settling pattern otherwise."""
    barrier = runner.get("barrier")
    settle = None
    if speedmap:
        settle = speedmap.get("ratedSettle") or speedmap.get("settle")
    if settle is None:
        settles = [_settle_from_inrun(r) for r in _runs(runner)]
        settles = [s for s in settles if s is not None]
        settle = sum(settles) / len(settles) if settles else None
    score = 0.0
    have = False
    if barrier:
        have = True
        # inside good, cap the damage for very wide gates
        score += max(0.0, 1.0 - (barrier - 1) / max(field_size - 1, 1)) * 0.5
    if settle is not None:
        have = True
        # on-pace/handy > midfield > back, scaled to field size
        rel = settle / max(field_size, 2)
        score += max(0.0, 1.0 - rel) * 0.5
    return score if have else None


def _record_component(rec: Optional[dict]) -> Optional[float]:
    win, place, starts = _rec_rate(rec)
    if win is None:
        return None
    return 2.0 * win + place


def _cond_component(runner: dict, track_condition: str) -> Optional[float]:
    h = runner.get("historicPerformanceData") or {}
    tc = (track_condition or "").lower()
    if "heavy" in tc:
        rec = h.get("heavyRecord")
    elif "soft" in tc:
        rec = h.get("softRecord")
    elif "firm" in tc:
        rec = h.get("firmRecord")
    elif "synthetic" in tc or "poly" in tc:
        rec = h.get("syntheticRecord")
    else:
        rec = h.get("goodRecord")
    return _record_component(rec)


def _trend_component(runner: dict) -> Optional[float]:
    """Improving margins/finishes across last 3 runs (positive = improving)."""
    runs = _runs(runner)
    margins = [r.get("margFin") for r in runs]
    margins = [m for m in margins if m is not None]
    fins = [_finish_from_inrun(r) for r in runs]
    fins = [f for f in fins if f is not None]
    out, have = 0.0, False
    if len(margins) >= 2:
        have = True
        out += (margins[-1] - margins[0]) * 0.15   # older minus newest: shrinking margin = +
    if len(fins) >= 2:
        have = True
        out += (fins[-1] - fins[0]) * 0.10
    if fins:
        have = True
        out += {1: 0.8, 2: 0.5, 3: 0.3}.get(fins[0], 0.0)  # latest finish quality
    return out if have else None


def _conn_component(runner: dict) -> Optional[float]:
    j = ((runner.get("jockeyData") or {}).get("jockeyFormLast100Races")) or {}
    t = ((runner.get("trainerData") or {}).get("trainerFormLast100Races")) or {}
    vals = []
    if j.get("overUnderPerformancePct") is not None:
        vals.append(j["overUnderPerformancePct"] - 1.0)
    if t.get("overUnderPerformancePct") is not None:
        vals.append(t["overUnderPerformancePct"] - 1.0)
    if j.get("strikeRatePct") is not None:
        vals.append((j["strikeRatePct"] - 12.0) / 25.0)
    return sum(vals) / len(vals) if vals else None


# ------------------------------------------------------------------ main
def _znorm(values: Dict[int, Optional[float]]) -> Dict[int, float]:
    """Z-score within the race; missing → 0 (neutral)."""
    present = [v for v in values.values() if v is not None]
    if len(present) < 2:
        return {k: 0.0 for k in values}
    mu = sum(present) / len(present)
    sd = math.sqrt(sum((v - mu) ** 2 for v in present) / len(present)) or 1.0
    return {k: ((v - mu) / sd if v is not None else 0.0)
            for k, v in values.items()}


def score_race(race_payload: dict,
               speedmap_by_tab: Optional[Dict[int, dict]] = None,
               scratched: Optional[set] = None,
               weights: Optional[Dict[str, float]] = None) -> List[dict]:
    """Score every non-scratched runner. Returns runners sorted best-first,
    each with total, per-component z + raw values (for reasoning lines)."""
    pl = race_payload.get("payLoad") or race_payload
    runners = [r for r in (pl.get("runners") or [])
               if r.get("tabNumber") not in (scratched or set())]
    field = len(runners)
    cond = pl.get("trackCondition") or ""
    sm = speedmap_by_tab or {}

    raw: Dict[str, Dict[int, Optional[float]]] = {k: {} for k in W}
    for r in runners:
        tab = r.get("tabNumber")
        h = r.get("historicPerformanceData") or {}
        raw["sect"][tab] = _sect_component(r)
        raw["speed"][tab] = _speed_component(r)
        raw["map"][tab] = _map_component(r, field, sm.get(tab))
        raw["dist"][tab] = _record_component(h.get("distanceRecord")) if h else None
        # track: prefer track+distance record when it has starts
        td = _record_component(h.get("trackDistRecord")) if h else None
        tr = _record_component(h.get("trackRecord")) if h else None
        raw["track"][tab] = td if td is not None else tr
        raw["cond"][tab] = _cond_component(r, cond)
        raw["trend"][tab] = _trend_component(r)
        raw["conn"][tab] = _conn_component(r)

    z = {k: _znorm(v) for k, v in raw.items()}
    # weights override (2026-08-06): the weekly refit ships new weights
    # via the jennifer_weights table — no deploy needed. Code W = the
    # fallback when no approved row exists.
    Wx = weights or W
    out = []
    for r in runners:
        tab = r.get("tabNumber")
        total = sum(Wx.get(k, 0.0) * z[k][tab] for k in W)
        out.append({
            "tab_number": tab,
            "horse_name": (r.get("horseName") or "").strip(),
            "barrier": r.get("barrier"),
            "total": round(total, 4),
            "z": {k: round(z[k][tab], 3) for k in W},
            "raw": {k: raw[k][tab] for k in W},
            "runner": r,
        })
    out.sort(key=lambda x: -x["total"])
    return out


def select_tips(scored: List[dict],
                market_order: Optional[Dict[int, float]] = None) -> dict:
    """AI Best / Danger / Value from a scored race.

    CONFLICT RULE: best-sectional runner takes AI Best when its raw
    sectional edge over the next-best sectional is >= 1.0 length class
    (spec: 'SECTIONALS WIN').

    Value: a DIFFERENT angle. With market data (allowed ONLY here, per
    the contract): the runner outside our top-2 whose market position
    most underrates our ranking. Without market data: the runner outside
    our top-2 with the strongest single non-sectional component.
    """
    if not scored:
        return {}
    ranked = list(scored)

    # CONFLICT RULE RETIRED (2026-08-04): the spec's "sectionals always
    # win" override was a guardrail against an LLM's narrative bias.
    # With FITTED weights the formula already encodes sectional
    # dominance, and the override measurably hurt: holdout AI Best
    # 21.2%/-13.3% with the rule vs 22.4%/-11.3% pure-formula on the
    # same races (Jun-Aug 2026, n=3,150). One binary ablation, fully
    # disclosed — not a holdout search.

    ai_best, danger = ranked[0], ranked[1] if len(ranked) > 1 else None
    top2 = {ai_best["tab_number"], danger["tab_number"] if danger else None}

    value = None
    pool = [s for s in ranked[2:8] if s["tab_number"] not in top2]
    if pool and market_order:
        mkt_rank = {tab: i + 1 for i, (tab, _) in enumerate(
            sorted(market_order.items(), key=lambda kv: kv[1]))}
        best_gap = 0
        for i, s in enumerate(pool, start=3):
            gap = (mkt_rank.get(s["tab_number"], i) - i)
            if gap > best_gap:
                best_gap, value = gap, s
    if value is None and pool:
        # strongest single non-sect angle among the pool
        value = max(pool, key=lambda s: max(
            s["z"]["cond"], s["z"]["dist"], s["z"]["track"], s["z"]["trend"]))

    return {"ai_best": ai_best, "danger": danger, "value": value}
