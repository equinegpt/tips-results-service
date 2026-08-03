# app/analyst/reasoning.py
"""Pundit-style reasoning lines for 8F Analyst tips.

Spec mandates per pick (docs/analyst-data-contract.md):
  - REQUIRED first: class-adjusted sectional lengths
  - REQUIRED: barrier + map position
  - then 1-2 extras (raw times / margins / A2E / dist-track-cond record)
Plain English, real numbers, no vague 'strong metrics' filler.
"""
from __future__ import annotations

import re
from typing import Optional


def _fmt_sect(raw_sect: Optional[float]) -> Optional[str]:
    if raw_sect is None:
        return None
    # raw component is positive-good; present it in PF's negative-good idiom
    return f"last 600m {-raw_sect:+.1f} lengths class"


def _style_word(runner: dict, speedmap: Optional[dict], field: int) -> str:
    settle = None
    if speedmap:
        settle = speedmap.get("ratedSettle") or speedmap.get("settle")
    if settle is None:
        settles = []
        for s in (runner.get("sectionalData") or []):
            m = re.search(r"settling_down,(\d+)",
                          ((s.get("jockey") or {}).get("inRun")) or "")
            if m:
                settles.append(int(m.group(1)))
        settle = sum(settles) / len(settles) if settles else None
    if settle is None:
        return "maps flexibly"
    rel = settle / max(field, 2)
    if rel <= 0.3:
        return "maps on pace"
    if rel <= 0.6:
        return "maps midfield"
    return "gets back, needs luck"


def _extras(entry: dict, race_cond: str) -> list:
    r = entry["runner"]
    h = r.get("historicPerformanceData") or {}
    out = []
    runs = r.get("sectionalData") or []
    if runs and runs[0].get("last600Time"):
        out.append(f"ran {runs[0]['last600Time']:.2f}s home last start")
    if runs and runs[0].get("margFin") is not None and runs[0]["margFin"] <= 2.0:
        out.append(f"beaten just {runs[0]['margFin']:.1f}L last time")
    tc = (race_cond or "").lower()
    rec_map = [("heavy", "heavyRecord", "Heavy"), ("soft", "softRecord", "Soft"),
               ("synthetic", "syntheticRecord", "synthetic"),
               ("good", "goodRecord", "Good")]
    for key, field_, label in rec_map:
        if key in tc:
            rec = h.get(field_) or {}
            if (rec.get("starts") or 0) >= 2 and (rec.get("firsts") or 0) >= 1:
                out.append(f"{rec['firsts']} from {rec['starts']} on {label} going")
            break
    td = h.get("trackDistRecord") or {}
    if (td.get("starts") or 0) >= 2 and (td.get("firsts") or 0) >= 1:
        out.append(f"{td['firsts']} from {td['starts']} at course and distance")
    j = ((r.get("jockeyData") or {}).get("jockeyFormLast100Races")) or {}
    if (j.get("overUnderPerformancePct") or 0) >= 1.15:
        out.append(f"jockey A2E {j['overUnderPerformancePct']:.2f}")
    return out


def reasoning_line(entry: dict, race_cond: str, field: int,
                   speedmap: Optional[dict] = None,
                   angle: str = "best") -> str:
    """One pundit sentence: sectional first, barrier+map, then extras."""
    r = entry["runner"]
    bits = []
    sect = _fmt_sect(entry["raw"]["sect"])
    bits.append(sect if sect else "limited sectional exposure")
    b = entry.get("barrier")
    style = _style_word(r, speedmap, field)
    bits.append(f"barrier {b}, {style}" if b else style)
    bits.extend(_extras(entry, race_cond)[:2])
    tail = {
        "best": "clear sectional standout" if entry["z"]["sect"] >= 1.0
                else "ticks the most boxes on the cascade",
        "danger": "obvious threat on the figures",
        "value": "market has this one under its true chance",
    }.get(angle, "")
    line = ", ".join(bits[:-1]) + f" — {bits[-1]}" if len(bits) > 2 else ", ".join(bits)
    return f"{line}. {tail.capitalize()}." if tail else line + "."
