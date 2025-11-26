# app/services/summary.py
from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Iterable, Any, Dict, List

STAKE_PER_TIP = 10.0


def build_summary(tips: Iterable[Any]) -> Dict[str, Any]:
    """
    Aggregate a collection of tip rows into a summary dict matching your day/meeting
    summary structure:

    {
      'tips': int,
      'wins': int,
      'strike_rate_pct': float,
      'turnover': float,
      'pnl': float,
      'quinellas': int,
      'trifectas': int,
    }

    NOTE: Replace the P&L / quinella / trifecta logic with whatever you use
    for your existing day_summary.
    """
    tips_list = list(tips)
    n = len(tips_list)
    if n == 0:
        return {
            "tips": 0,
            "wins": 0,
            "strike_rate_pct": 0.0,
            "turnover": 0.0,
            "pnl": 0.0,
            "quinellas": 0,
            "trifectas": 0,
        }

    # Basic counts
    wins = sum(1 for t in tips_list if getattr(t, "result", None) == "WIN")
    turnover = n * STAKE_PER_TIP

    # Example P&L logic – WIN bets at SP, rest lose their stake.
    # ⚠️ If you already have your own P&L calculation for day_summary,
    #     call that here instead.
    pnl = 0.0
    for t in tips_list:
        res = getattr(t, "result", None)
        sp = getattr(t, "sp", None)
        if res == "WIN" and sp is not None:
            # Win bet returns stake * SP. Net profit is (SP - 1) * stake.
            pnl += (float(sp) - 1.0) * STAKE_PER_TIP
        else:
            pnl -= STAKE_PER_TIP

    # Quinellas / Trifectas – adjust to match your schema.
    # If you store them explicitly, replace these with your actual fields.
    quinellas = sum(
        1 for t in tips_list if getattr(t, "is_quinella", False)
    )
    trifectas = sum(
        1 for t in tips_list if getattr(t, "is_trifecta", False)
    )

    strike_rate_pct = (wins / n * 100.0) if n > 0 else 0.0

    return {
        "tips": n,
        "wins": wins,
        "strike_rate_pct": strike_rate_pct,
        "turnover": turnover,
        "pnl": pnl,
        "quinellas": quinellas,
        "trifectas": trifectas,
    }


def build_track_stats(
    tips: Iterable[Any],
    bet_focus: str = "all",
) -> List[Dict[str, Any]]:
    """
    Group tips by track and build a list like:

    [
      {
        "track_name": "Flemington",
        "state": "VIC",
        "summary": { ... same structure as build_summary(...) ... },
      },
      ...
    ]

    bet_focus influences the sort order (what we care about most).
    """
    buckets: dict[tuple[str, str], list[Any]] = defaultdict(list)

    for t in tips:
        meeting = getattr(t, "meeting", None)
        if meeting is None:
            continue
        track_name = getattr(meeting, "track_name", "Unknown")
        state = getattr(meeting, "state", "??")
        key = (track_name, state)
        buckets[key].append(t)

    rows: List[Dict[str, Any]] = []
    for (track_name, state), bucket in buckets.items():
        rows.append(
            {
                "track_name": track_name,
                "state": state,
                "summary": build_summary(bucket),
            }
        )

    # Sort according to focus
    def sort_key(row: Dict[str, Any]) -> float:
        s = row["summary"]
        if bet_focus == "quinellas":
            return float(s.get("quinellas", 0))
        if bet_focus == "trifectas":
            return float(s.get("trifectas", 0))
        if bet_focus == "wins":
            return float(s.get("strike_rate_pct", 0.0))
        # default: P&L
        return float(s.get("pnl", 0.0))

    rows.sort(key=sort_key, reverse=True)
    return rows


def build_daily_stats(tips: Iterable[Any]) -> List[Dict[str, Any]]:
    """
    Group tips by date and build:

    [
      {
        "date": datetime.date,
        "summary": { ... same as build_summary(...) ... },
      },
      ...
    ]
    """
    buckets: dict[date, list[Any]] = defaultdict(list)

    for t in tips:
        d = getattr(t, "date", None)
        if d is None:
            continue
        buckets[d].append(t)

    rows: List[Dict[str, Any]] = []
    for d, bucket in buckets.items():
        rows.append(
            {
                "date": d,
                "summary": build_summary(bucket),
            }
        )

    rows.sort(key=lambda r: r["date"])
    return rows
