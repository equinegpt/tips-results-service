# app/routes_meeting_best.py
"""
Routes for Meeting Best analytics.
Meeting Best = AI_BEST tip matches Skynet #1 ranked horse.
"""
from __future__ import annotations

from datetime import date as date_type, timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from .database import get_db
from .meeting_best_analytics import compute_meeting_best_trends

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/api/meeting-best")
def api_meeting_best(
    from_date: date_type = Query(..., alias="from"),
    to_date: date_type = Query(..., alias="to"),
    db: Session = Depends(get_db),
):
    """
    JSON API for Meeting Best analytics.

    Meeting Best = AI_BEST tip matches Skynet rank=1 horse for same race.

    Example:
        /api/meeting-best?from=2025-12-01&to=2025-12-07
    """
    return compute_meeting_best_trends(
        db=db,
        date_from=from_date,
        date_to=to_date,
    )


@router.get("/ui/meeting-best", response_class=HTMLResponse)
def ui_meeting_best(
    request: Request,
    from_date: date_type | None = Query(None),
    to_date: date_type | None = Query(None),
    db: Session = Depends(get_db),
):
    """
    HTML dashboard for Meeting Best analytics.

    If no dates provided, shows empty state with date picker.
    """
    # If no dates, show empty page with date picker
    if from_date is None or to_date is None:
        today = date_type.today()

        # Pre-compute quick select dates
        quick_7_from = (today - timedelta(days=7)).isoformat()
        quick_14_from = (today - timedelta(days=14)).isoformat()
        quick_30_from = (today - timedelta(days=30)).isoformat()
        today_iso = today.isoformat()

        return templates.TemplateResponse(
            "meeting_best.html",
            {
                "request": request,
                "data": None,
                "from_date": today - timedelta(days=7),
                "to_date": today,
                "display_range": None,
                "has_data": False,
                "empty_state": True,
                "quick_7_from": quick_7_from,
                "quick_14_from": quick_14_from,
                "quick_30_from": quick_30_from,
                "today_iso": today_iso,
            },
        )

    # Dates provided - fetch and compute
    data = compute_meeting_best_trends(
        db=db,
        date_from=from_date,
        date_to=to_date,
    )

    display_range = f"{from_date.strftime('%d %b %Y')} - {to_date.strftime('%d %b %Y')}"

    return templates.TemplateResponse(
        "meeting_best.html",
        {
            "request": request,
            "data": data,
            "from_date": from_date,
            "to_date": to_date,
            "display_range": display_range,
            "has_data": data.get("has_data", False),
            "empty_state": False,
        },
    )
