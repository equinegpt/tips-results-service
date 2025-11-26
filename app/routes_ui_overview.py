# app/api/routes_ui_overview.py
from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.models import Tip, Meeting
from app.summary import (
    build_summary,
    build_track_stats,
    build_daily_stats,
)
from app.tracks import get_all_tracks

templates = Jinja2Templates(directory="templates")
router = APIRouter()


@router.get("/ui/overview", response_class=HTMLResponse)
def tips_overview(
    request: Request,
    from_date: Optional[date] = Query(None),
    to_date: Optional[date] = Query(None),
    track_code: Optional[str] = Query(None),
    bet_focus: str = Query("all"),
    db: Session = Depends(get_db),
):
    """
    Overview dashboard across multiple days and tracks.

    - Filters by date range + optional track_code.
    - bet_focus controls how we sort the track table (wins vs quinellas vs trifectas vs P&L).
    """

    # 1) Normalise dates – default to last 14 days
    today = date.today()
    if not to_date:
        to_date = today
    if not from_date:
        from_date = to_date - timedelta(days=13)

    # 2) Base query: all tips in range, joined to Meeting for track/state
    q = (
        db.query(Tip)
        .join(Tip.meeting)
        .filter(Tip.date >= from_date, Tip.date <= to_date)
    )

    # 3) Track filter – track_code is "state|track" lowercased
    if track_code:
        state_part, _, track_part = track_code.partition("|")
        if state_part and track_part:
            q = q.filter(
                func.lower(Meeting.state) == state_part.lower(),
                func.lower(Meeting.track_name) == track_part.lower(),
            )

    tip_rows = q.all()
    has_data = len(tip_rows) > 0

    display_range = f"{from_date:%d %b %Y} – {to_date:%d %b %Y}"

    # Always provide tracks + filters for the template
    all_tracks = get_all_tracks(db)

    if not has_data:
        context = {
            "request": request,
            "has_data": False,
            "from_date": from_date,
            "to_date": to_date,
            "display_range": display_range,
            "filter_track_code": track_code,
            "bet_focus": bet_focus,
            "all_tracks": all_tracks,
            "overall_summary": None,
            "track_stats": [],
            "daily_stats": [],
        }
        return templates.TemplateResponse("overview.html", context)

    # 4) Build summaries
    overall = build_summary(tip_rows)
    track_stats = build_track_stats(tip_rows, bet_focus=bet_focus)
    daily_stats = build_daily_stats(tip_rows)

    # Add "days" and "tracks" counts expected by the template
    overall["days"] = len(daily_stats)
    overall["tracks"] = len(track_stats)

    context = {
        "request": request,
        "has_data": True,
        "from_date": from_date,
        "to_date": to_date,
        "display_range": display_range,
        "filter_track_code": track_code,
        "bet_focus": bet_focus,
        "all_tracks": all_tracks,
        "overall_summary": overall,
        "track_stats": track_stats,
        "daily_stats": daily_stats,
    }
    return templates.TemplateResponse("overview.html", context)
