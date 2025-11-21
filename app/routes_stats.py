# app/routes_stats.py
from __future__ import annotations

from datetime import date as date_type

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from .database import get_db
from . import schemas, analytics
from .stats_rollup import compute_day_rollup

router = APIRouter()

@router.get("/debug/day-summary")
def debug_day_summary(
    meeting_date: date_type = Query(..., alias="date"),
    stake_per_tip: float = Query(10.0),
    db: Session = Depends(get_db),
):
    """
    Debug JSON view of per-race / per-meeting / day stats.

    Example:
      /debug/day-summary?date=2025-11-18&stake_per_tip=10
    """
    return compute_day_rollup(
        db=db,
        target_date=meeting_date,
        stake_per_tip=stake_per_tip,
    )

@router.get("/stats/day", response_model=schemas.DayStatsOut)
def stats_day(
    meeting_date: date_type = Query(..., alias="date"),
    provider: str = Query("RA"),
    stake_per_tip: float = Query(10.0),
    track_name: str | None = Query(None),
    state: str | None = Query(None),
    db: Session = Depends(get_db),
):
    return analytics.compute_day_stats(
        db=db,
        target_date=meeting_date,
        provider=provider,
        stake_per_tip=stake_per_tip,
        track_name=track_name,
        state=state,
    )


@router.get("/stats/range", response_model=schemas.RangeStatsOut)
def stats_range(
    date_from: date_type = Query(..., alias="from"),
    date_to: date_type = Query(..., alias="to"),
    provider: str = Query("RA"),
    stake_per_tip: float = Query(10.0),
    track_name: str | None = Query(None),
    state: str | None = Query(None),
    db: Session = Depends(get_db),
):
    return analytics.compute_range_stats(
        db=db,
        date_from=date_from,
        date_to=date_to,
        provider=provider,
        stake_per_tip=stake_per_tip,
        track_name=track_name,
        state=state,
    )
