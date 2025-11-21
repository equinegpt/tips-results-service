# app/routes_debug.py
from __future__ import annotations

from datetime import date as date_type
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from .database import get_db
from . import models

router = APIRouter()


@router.get("/debug/ra-results")
def debug_ra_results(
    meeting_date: date_type = Query(..., alias="date"),
    db: Session = Depends(get_db),
):
    """
    Debug-only: show RA race results for a given date, joined with Meeting/Race.
    """
    q = (
        db.query(models.RaceResult, models.Race, models.Meeting)
        .join(models.Race, models.RaceResult.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .filter(models.Meeting.date == meeting_date)
        .order_by(
            models.Meeting.track_name,
            models.Race.race_number,
            models.RaceResult.finish_position,
        )
    )

    out: list[dict[str, Any]] = []
    for rr, race, meeting in q:
        out.append(
            {
                "track_name": meeting.track_name,
                "state": meeting.state,
                "race_number": race.race_number,
                "race_name": race.name,
                "tab_number": rr.tab_number,
                "horse_name": rr.horse_name,
                "finish_position": rr.finish_position,
                "status": rr.status,
                "margin_text": rr.margin_text,
                "starting_price": float(rr.starting_price)
                if rr.starting_price is not None
                else None,
            }
        )
    return out
