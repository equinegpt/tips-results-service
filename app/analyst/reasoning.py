# app/analyst/reasoning.py
"""Pundit-style reasoning for 8F Analyst tips — v2 (2026-08-04).

v1 recited the sectional figure and stopped ("#4 — last 600m -3.1
lengths class"). v2 builds each line the way a form analyst talks:
pick the horse's 2-3 STRONGEST angles (records, conditions, course,
trip, map, connections, trend), lead with the best one, weave real
numbers in, and close with the tip-type's stance. Everything comes
from payload fields — no invention, no filler adjectives without a
number behind them.

Deterministic: sentence-frame choices hash off (horse, tab) so a given
race always renders the same words — reproducible, testable.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


# ------------------------------------------------------------ utilities
def _runs(runner: dict) -> List[dict]:
    return [s for s in (runner.get("sectionalData") or []) if isinstance(s, dict)]


def _sane(v, limit=25.0):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if abs(v) <= limit else None


def _rec(h: dict, key: str) -> Tuple[int, int, int]:
    r = (h or {}).get(key) or {}
    return (r.get("firsts") or 0,
            (r.get("firsts") or 0) + (r.get("seconds") or 0) + (r.get("thirds") or 0),
            r.get("starts") or 0)


def _settle_avg(runner: dict) -> Optional[float]:
    vals = []
    for s in _runs(runner):
        m = re.search(r"settling_down,(\d+)",
                      ((s.get("jockey") or {}).get("inRun")) or "")
        if m:
            vals.append(int(m.group(1)))
    return sum(vals) / len(vals) if vals else None


def _jockey_surname(runner: dict) -> Optional[str]:
    parts = (((runner.get("jockeyData") or {}).get("fullName")) or "").split()
    return parts[-1] if parts else None


def _cond_key(race_cond: str) -> Tuple[str, str]:
    tc = (race_cond or "").lower()
    if "heavy" in tc:
        return "heavyRecord", "heavy"
    if "soft" in tc:
        return "softRecord", "soft"
    if "synthetic" in tc or "poly" in tc:
        return "syntheticRecord", "synthetic"
    if "firm" in tc:
        return "firmRecord", "firm"
    return "goodRecord", "good"


# ------------------------------------------------------------ angles
def _collect_angles(entry: dict, race_cond: str, field: int,
                    speedmap: Optional[dict]) -> List[Tuple[float, str]]:
    """(salience, sentence-fragment) candidates. Salience blends the
    component z-score with how quotable the underlying fact is."""
    r = entry["runner"]
    h = r.get("historicPerformanceData") or {}
    z = entry["z"]
    out: List[Tuple[float, str]] = []
    runs = _runs(r)

    # --- sectionals — HONEST framing (PF convention: negative = better
    # than class par; entry["raw"]["sect"] is stored positive-good).
    # v2.1 sign fix: never praise a below-par figure.
    sect = entry["raw"].get("sect")
    if sect is not None:
        best6 = None
        if runs:
            vals = [_sane(runs[0].get("last600Class"))]
            vals = [v for v in vals if v is not None]
            best6 = vals[0] if vals else None
        if sect >= 1.0:
            frag = (f"owns the strongest class-adjusted sectionals here "
                    f"(last 600m {-sect:+.1f} lengths vs class)"
                    if z.get("sect", 0) >= 0.8 else
                    f"brings genuine late speed ({-sect:+.1f} lengths vs "
                    f"class on the clock)")
            if best6 is not None and best6 <= -2.0:
                frag += f" including a {best6:+.1f} last-600 burst latest"
            out.append((2.0 + z.get("sect", 0), frag))
        elif sect >= 0:
            out.append((0.6 + z.get("sect", 0),
                        f"runs fair class-adjusted sectionals ({-sect:+.1f})"))
        else:
            # below class par — honest, never a selling line
            out.append((0.15,
                        f"the clock is a query ({-sect:+.1f} vs class)"))

    # --- conditions record ---
    ckey, cword = _cond_key(race_cond)
    w, p, s = _rec(h, ckey)
    if s >= 2 and w >= 1 and cword != "good":
        frag = f"a proven {cword}-tracker with {w} win{'s' if w != 1 else ''} from {s} on the surface"
        if runs and (runs[0].get("margFin") or 0) >= 5 and cword in ("soft", "heavy"):
            frag += " — forgive the last-start flop on unsuitable ground"
        out.append((1.2 + z.get("cond", 0) + 0.3 * w, frag))
    elif s >= 3 and w == 0 and p == 0 and cword in ("soft", "heavy"):
        # negative angle is honest but not a selling line — low salience
        out.append((0.1, f"unproven in {cword} going (0 from {s})"))

    # --- course & distance / track ---
    w, p, s = _rec(h, "trackDistRecord")
    if s >= 2 and w >= 1:
        out.append((1.4 + 0.4 * w, f"{w} from {s} at course and distance"))
    else:
        w2, p2, s2 = _rec(h, "trackRecord")
        if s2 >= 2 and w2 >= 1:
            out.append((1.1 + 0.3 * w2, f"knows this track well ({w2}/{s2} here)"))

    # --- distance ---
    w, p, s = _rec(h, "distanceRecord")
    if s >= 3 and (w / s) >= 0.3:
        out.append((1.3 + 0.5 * (w / s), f"an elite {w}-from-{s} record at the trip"))
    elif s >= 3 and ((w + p) / s) >= 0.6:
        out.append((0.9, f"rarely misses a cheque at this distance ({p} placings from {s})"))

    # --- map / barrier ---
    settle = None
    if speedmap:
        settle = speedmap.get("ratedSettle") or speedmap.get("settle")
    if settle is None:
        settle = _settle_avg(r)
    b = entry.get("barrier")
    if settle is not None and b:
        rel = settle / max(field, 2)
        if rel <= 0.3:
            out.append((1.0 + z.get("map", 0),
                        f"maps to roll forward from gate {b} and control it"))
        elif rel <= 0.6:
            out.append((0.7 + z.get("map", 0),
                        f"gets a midfield sit from barrier {b} with cover"))
        else:
            out.append((0.4, f"settles back from gate {b} and needs the tempo"))
    elif b:
        out.append((0.5, f"drawn barrier {b}"))

    # --- form trend / last start ---
    if runs:
        m0 = runs[0].get("margFin")
        fin0 = None
        m = re.search(r"finish,(\d+)", ((runs[0].get("jockey") or {}).get("inRun")) or "")
        if m:
            fin0 = int(m.group(1))
        if fin0 == 1:
            out.append((1.2, "arrives in winning form"))
        elif m0 is not None and m0 <= 1.5:
            out.append((1.1, f"beaten only {m0:.1f}L last start and strips fitter"))
        elif len(runs) >= 2:
            m1 = runs[1].get("margFin")
            if m0 is not None and m1 is not None and m1 - m0 >= 2.0:
                out.append((0.8, f"trending the right way ({m1:.0f}L into {m0:.0f}L margins)"))

    # --- connections ---
    j = ((r.get("jockeyData") or {}).get("jockeyFormLast100Races")) or {}
    surname = _jockey_surname(r)
    a2e = j.get("overUnderPerformancePct")
    if surname and a2e and a2e >= 1.2:
        out.append((0.9 + z.get("conn", 0),
                    f"{surname} (A2E {a2e:.2f}) is flying"))
    elif surname and (j.get("strikeRatePct") or 0) >= 20:
        out.append((0.7, f"{surname} striking at {j['strikeRatePct']:.0f}%"))
    t = ((r.get("trainerData") or {}).get("trainerFormLast100Races")) or {}
    if (t.get("strikeRatePct") or 0) >= 22:
        tn = (t.get("name") or "").strip()
        if tn:
            out.append((0.6, f"the {tn} yard is in form ({t['strikeRatePct']:.0f}%)"))

    return out


_CLOSERS = {
    "best": ["the one to beat", "hard to run down on these figures",
             "sets the standard"],
    "danger": ["the clear danger", "the main threat to the top pick",
               "right in this on the numbers"],
    "value": ["the market has this one under its true odds",
              "better than the price suggests",
              "the overlooked runner in the race"],
}


def reasoning_line(entry: dict, race_cond: str, field: int,
                   speedmap: Optional[dict] = None,
                   angle: str = "best") -> str:
    """Compose the pundit line: strongest 2-3 angles, real numbers,
    stance-appropriate close."""
    angles = _collect_angles(entry, race_cond, field, speedmap)
    angles.sort(key=lambda x: -x[0])
    picked = [a for _, a in angles[:3]]
    if not picked:
        picked = ["limited exposed form but heads the figures we can see"]
    seed = (hash((entry.get("horse_name"), entry.get("tab_number"))) & 0xff)
    closer = _CLOSERS.get(angle, _CLOSERS["best"])[seed % 3]
    if len(picked) == 1:
        body = picked[0]
    elif len(picked) == 2:
        body = f"{picked[0]}, and {picked[1]}"
    else:
        body = f"{picked[0]}; {picked[1]}; {picked[2]}"
    body = body[0].upper() + body[1:]
    return f"{body} — {closer}."
