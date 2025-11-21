# app/stats_rollup.py
from __future__ import annotations

from datetime import date as date_type
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from sqlalchemy.orm import Session

from . import models


def _safe_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def _add_money(acc: Decimal, val: Any) -> Decimal:
    return acc + _safe_decimal(val)


def compute_day_rollup(
    db: Session,
    target_date: date_type,
    stake_per_tip: float = 10.0,
) -> Dict[str, Any]:
    """
    Simple performance rollup for a given date.

    Rules:
      - Every tip (AI_BEST / DANGER / VALUE) is staked for `stake_per_tip` (e.g., $10).
      - A WIN is any tip whose runner finished 1st.
      - Return = stake_per_tip * SP for winners; 0 for others.
      - P&L = Return - Turnover.
      - Strike % = wins / tips * 100.
      - Quinella per race:
          any two of (AI_BEST, DANGER, VALUE) finish 1st & 2nd in any order.
      - Trifecta per race:
          AI_BEST, DANGER & VALUE collectively fill 1st / 2nd / 3rd in any order.
    """

    stake_dec = _safe_decimal(stake_per_tip)

    # --------------------------------------------------
    # 1) Fetch all tips (and their races/meetings) for the day
    # --------------------------------------------------
    rows = (
        db.query(models.Tip, models.Race, models.Meeting)
        .join(models.Race, models.Tip.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .filter(models.Meeting.date == target_date)
        .all()
    )

    if not rows:
        return {
            "date": target_date.isoformat(),
            "stake_per_tip": float(stake_dec),
            "meetings": [],
            "totals": {
                "tips_total": 0,
                "wins": 0,
                "turnover": 0.0,
                "return": 0.0,
                "profit": 0.0,
                "success_pct": 0.0,
                "quinellas": 0,
                "trifectas": 0,
            },
        }

    # --------------------------------------------------
    # 2) Build RaceResult index: (race_id, tab_number) -> best RaceResult
    #    Prefer provider="PF" over "RA" if both exist.
    # --------------------------------------------------
    race_ids = {race.id for (_, race, _) in rows}

    rr_index: Dict[Tuple[str, int], models.RaceResult] = {}
    if race_ids:
        rr_rows = (
            db.query(models.RaceResult)
            .filter(models.RaceResult.race_id.in_(race_ids))
            .all()
        )
        for rr in rr_rows:
            key = (rr.race_id, rr.tab_number)
            existing = rr_index.get(key)
            if existing is None or (existing.provider != "PF" and rr.provider == "PF"):
                rr_index[key] = rr

    # --------------------------------------------------
    # 3) Per-race accumulation
    # --------------------------------------------------
    race_stats: Dict[Tuple[str, str], Dict[str, Any]] = {}

    # For quinella & trifecta detection:
    #   race_id -> {"AI_BEST": [positions], "DANGER": [positions], "VALUE": [positions]}
    quinella_positions: Dict[str, Dict[str, List[int]]] = {}

    for tip, race, meeting in rows:
        rk = (meeting.id, race.id)
        if rk not in race_stats:
            race_stats[rk] = {
                "meeting_id": meeting.id,
                "race_id": race.id,
                "track_name": meeting.track_name,
                "state": meeting.state,
                "race_number": race.race_number,
                "race_name": race.name,
                "tips_total": 0,
                "wins": 0,
                "turnover": Decimal("0"),
                "return": Decimal("0"),
                "profit": Decimal("0"),   # filled later
                "success_pct": 0.0,       # filled later
                "quinella_hit": False,    # filled later
                "trifecta_hit": False,    # filled later
            }

        stats = race_stats[rk]

        # Every tip is staked
        stats["tips_total"] += 1
        stats["turnover"] = _add_money(stats["turnover"], stake_dec)

        # Get best RaceResult (PF preferred over RA)
        rr = rr_index.get((race.id, tip.tab_number))
        pos = rr.finish_position if rr is not None else None

        # Quinella / Trifecta tracking (positions for AI_BEST / DANGER / VALUE)
        if pos is not None and tip.tip_type in ("AI_BEST", "DANGER", "VALUE"):
            q = quinella_positions.setdefault(
                race.id,
                {"AI_BEST": [], "DANGER": [], "VALUE": []},
            )
            q[tip.tip_type].append(pos)

        # WIN = finished 1st
        if pos == 1:
            stats["wins"] += 1
            sp = _safe_decimal(rr.starting_price if rr is not None else None)
            payout = stake_dec * sp
            stats["return"] = _add_money(stats["return"], payout)

    # --------------------------------------------------
    # 4) Finish per-race metrics (profit, strike %, quinella, trifecta)
    # --------------------------------------------------
    for (meeting_id, race_id), stats in race_stats.items():
        # Profit
        stats["profit"] = stats["return"] - stats["turnover"]

        # Strike rate for that race
        if stats["tips_total"] > 0:
            stats["success_pct"] = (
                stats["wins"] / stats["tips_total"]
            ) * 100.0
        else:
            stats["success_pct"] = 0.0

        # Quinella / Trifecta?
        q = quinella_positions.get(race_id)
        quinella_hit = False
        trifecta_hit = False

        if q:
            # Quinella: any two of AI_BEST / DANGER / VALUE fill 1st & 2nd
            quinella_all_positions: List[int] = []
            quinella_all_positions.extend(q.get("AI_BEST", []))
            quinella_all_positions.extend(q.get("DANGER", []))
            quinella_all_positions.extend(q.get("VALUE", []))

            if 1 in quinella_all_positions and 2 in quinella_all_positions:
                quinella_hit = True

            # Trifecta: AI_BEST, DANGER & VALUE collectively fill 1,2,3
            a_positions = q.get("AI_BEST", [])
            d_positions = q.get("DANGER", [])
            v_positions = q.get("VALUE", [])

            if a_positions and d_positions and v_positions:
                for pa in a_positions:
                    for pd in d_positions:
                        for pv in v_positions:
                            if {pa, pd, pv} == {1, 2, 3}:
                                trifecta_hit = True
                                break
                        if trifecta_hit:
                            break
                    if trifecta_hit:
                        break

        stats["quinella_hit"] = quinella_hit
        stats["trifecta_hit"] = trifecta_hit

    # --------------------------------------------------
    # 5) Roll up to meetings
    # --------------------------------------------------
    meetings: Dict[str, Dict[str, Any]] = {}

    for (meeting_id, race_id), stats in race_stats.items():
        m = meetings.get(meeting_id)
        if m is None:
            m = {
                "meeting_id": stats["meeting_id"],
                "track_name": stats["track_name"],
                "state": stats["state"],
                "races": [],
                "totals": {
                    "tips_total": 0,
                    "wins": 0,
                    "turnover": Decimal("0"),
                    "return": Decimal("0"),
                    "profit": Decimal("0"),
                    "success_pct": 0.0,  # filled later
                    "quinellas": 0,
                    "trifectas": 0,
                },
            }
            meetings[meeting_id] = m

        m["races"].append(stats)

        mt = m["totals"]
        mt["tips_total"] += stats["tips_total"]
        mt["wins"] += stats["wins"]
        mt["turnover"] = _add_money(mt["turnover"], stats["turnover"])
        mt["return"] = _add_money(mt["return"], stats["return"])
        mt["profit"] = _add_money(mt["profit"], stats["profit"])
        if stats["quinella_hit"]:
            mt["quinellas"] += 1
        if stats["trifecta_hit"]:
            mt["trifectas"] += 1

    # Meeting-level strike %
    for m in meetings.values():
        mt = m["totals"]
        if mt["tips_total"] > 0:
            mt["success_pct"] = (
                mt["wins"] / mt["tips_total"]
            ) * 100.0
        else:
            mt["success_pct"] = 0.0

    # --------------------------------------------------
    # 6) Day-level totals
    # --------------------------------------------------
    day_totals = {
        "tips_total": 0,
        "wins": 0,
        "turnover": Decimal("0"),
        "return": Decimal("0"),
        "profit": Decimal("0"),
        "success_pct": 0.0,  # filled later
        "quinellas": 0,
        "trifectas": 0,
    }

    for m in meetings.values():
        mt = m["totals"]
        day_totals["tips_total"] += mt["tips_total"]
        day_totals["wins"] += mt["wins"]
        day_totals["turnover"] = _add_money(day_totals["turnover"], mt["turnover"])
        day_totals["return"] = _add_money(day_totals["return"], mt["return"])
        day_totals["profit"] = _add_money(day_totals["profit"], mt["profit"])
        day_totals["quinellas"] += mt["quinellas"]
        day_totals["trifectas"] += mt["trifectas"]

    if day_totals["tips_total"] > 0:
        day_totals["success_pct"] = (
            day_totals["wins"] / day_totals["tips_total"]
        ) * 100.0
    else:
        day_totals["success_pct"] = 0.0

    # --------------------------------------------------
    # 7) Normalise Decimals -> floats for JSON / Jinja
    # --------------------------------------------------
    def _norm_money(d: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in d.items():
            if isinstance(v, Decimal):
                out[k] = float(v)
            else:
                out[k] = v
        return out

    for m in meetings.values():
        m["totals"] = _norm_money(m["totals"])
        for st in m["races"]:
            for key in ("turnover", "return", "profit"):
                st[key] = float(st[key])

    day_totals = _norm_money(day_totals)

    return {
        "date": target_date.isoformat(),
        "stake_per_tip": float(stake_dec),
        "meetings": list(meetings.values()),
        "totals": day_totals,
    }
