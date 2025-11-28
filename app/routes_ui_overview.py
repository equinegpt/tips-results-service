# app/routes_ui_overview.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from .database import get_db
from .models import Meeting, Race, Tip, TipOutcome

router = APIRouter()


def _today_melb() -> date:
    return datetime.now(ZoneInfo("Australia/Melbourne")).date()


def _parse_date_param(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


@router.get("/ui/overview")
def ui_overview(
    date_from: Optional[str] = Query(
        None,
        description="Start date (YYYY-MM-DD). Default: 7 days ago (incl. today).",
    ),
    date_to: Optional[str] = Query(
        None,
        description="End date (YYYY-MM-DD). Default: today (Melbourne).",
    ),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    JSON overview of tips & results across tracks for a date window.

    This is intentionally backend-only JSON for now. The frontend (or you via browser)
    can call /ui/overview?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD and render however
    you like.

    For each track we return:
      - tips
      - wins
      - places (WIN or PLACE)
      - winStrikeRate
      - placeStrikeRate
      - nominal ROI (per stake unit) based on SP + stake_units
    """
    today = _today_melb()

    d_to = _parse_date_param(date_to) or today
    d_from = _parse_date_param(date_from) or (d_to - timedelta(days=6))

    # Fetch all tips + outcomes in that window
    # Join chain: Tip -> Race -> Meeting, left join TipOutcome
    rows = (
        db.query(Tip, TipOutcome, Race, Meeting)
        .join(Race, Tip.race_id == Race.id)
        .join(Meeting, Race.meeting_id == Meeting.id)
        .outerjoin(TipOutcome, TipOutcome.tip_id == Tip.id)
        .filter(Meeting.date >= d_from, Meeting.date <= d_to)
        .all()
    )

    # Aggregate per (track, state)
    agg: Dict[tuple[str, str], Dict[str, Any]] = {}

    for tip, outcome, race, meeting in rows:
        key = (meeting.track_name, meeting.state)
        if key not in agg:
            agg[key] = {
                "track": meeting.track_name,
                "state": meeting.state,
                "tips": 0,
                "wins": 0,
                "places": 0,
                "stakes": Decimal("0.0"),
                "return": Decimal("0.0"),
            }

        bucket = agg[key]
        bucket["tips"] += 1

        # Stake units (Numeric in DB) → Decimal
        stake_units = Decimal(str(tip.stake_units or 1))
        bucket["stakes"] += stake_units

        status = outcome.outcome_status if outcome is not None else "PENDING"

        if status == "WIN":
            bucket["wins"] += 1
            bucket["places"] += 1
        elif status == "PLACE":
            bucket["places"] += 1
        elif status in ("SCRATCHED", "NO_RESULT", "PENDING"):
            # Treat as zero P&L
            pass
        else:  # LOSE or anything else
            pass

        # Simple ROI calc: (SP - 1) * stake for winners, -stake for losers.
        sp = outcome.starting_price if outcome is not None else None
        if isinstance(sp, (Decimal, float, int)):
            sp_dec = Decimal(str(sp))
        else:
            sp_dec = None

        if status == "WIN" and sp_dec is not None:
            # Profit is (SP - 1) * stake
            bucket["return"] += stake_units * sp_dec
        elif status == "LOSE":
            # Return is 0; we already counted stake on the stakes side
            pass

    # Convert aggregates to list + compute strike rates & ROI
    tracks: list[Dict[str, Any]] = []
    for (track_name, state), b in agg.items():
        tips = b["tips"]
        wins = b["wins"]
        places = b["places"]
        stakes = b["stakes"]
        ret = b["return"]

        win_sr = float(wins) / tips if tips else 0.0
        place_sr = float(places) / tips if tips else 0.0
        roi = float(ret / stakes) - 1.0 if stakes > 0 else 0.0

        tracks.append(
            {
                "track": track_name,
                "state": state,
                "tips": tips,
                "wins": wins,
                "places": places,
                "winStrikeRate": win_sr,
                "placeStrikeRate": place_sr,
                "stakes": float(stakes),
                "return": float(ret),
                "roi": roi,
            }
        )

    # Sort: best ROI first, then by tips desc
    tracks.sort(key=lambda t: (t["roi"], t["tips"]), reverse=True)

    return {
        "dateFrom": d_from.isoformat(),
        "dateTo": d_to.isoformat(),
        "tracks": tracks,
    }
