# app/routes_trends.py
"""
Routes for trend analysis dashboard and API endpoints.
"""
from __future__ import annotations

from datetime import date as date_type, timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from .database import get_db
from .trends_analytics import compute_trends

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/api/trends")
def api_trends(
    from_date: date_type | None = Query(None, alias="from"),
    to_date: date_type | None = Query(None, alias="to"),
    db: Session = Depends(get_db),
):
    """
    JSON API for trend analysis data.

    Example:
        /api/trends?from=2025-11-01&to=2025-12-31
    """
    # Default to last 30 days if no dates specified
    if to_date is None:
        to_date = date_type.today()
    if from_date is None:
        from_date = to_date - timedelta(days=30)

    return compute_trends(
        db=db,
        date_from=from_date,
        date_to=to_date,
    )


@router.get("/ui/trends", response_class=HTMLResponse)
def ui_trends(
    request: Request,
    from_date: date_type | None = Query(None),
    to_date: date_type | None = Query(None),
    db: Session = Depends(get_db),
):
    """
    HTML dashboard for trend analysis.
    """
    # Default to all available data if no dates specified
    if to_date is None:
        to_date = date_type.today()
    if from_date is None:
        # Default to 60 days back for a good sample
        from_date = to_date - timedelta(days=60)

    trends_data = compute_trends(
        db=db,
        date_from=from_date,
        date_to=to_date,
    )

    display_range = f"{from_date.strftime('%d %b %Y')} - {to_date.strftime('%d %b %Y')}"

    return templates.TemplateResponse(
        "trends.html",
        {
            "request": request,
            "trends": trends_data,
            "from_date": from_date,
            "to_date": to_date,
            "display_range": display_range,
            "has_data": trends_data.get("has_data", False),
        },
    )
