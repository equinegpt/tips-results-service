# app/routes_reasoning.py
"""
Routes for reasoning analysis - Phase 2 analytics.
Analyzes which tip reasoning phrases correlate with winning results.
"""
from __future__ import annotations

from datetime import date as date_type, timedelta

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .reasoning_analytics import compute_reasoning_trends

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/api/reasoning")
def api_reasoning(
    from_date: date_type | None = Query(None, alias="from"),
    to_date: date_type | None = Query(None, alias="to"),
):
    """
    JSON API for reasoning analysis data.

    Example:
        /api/reasoning?from=2025-12-01&to=2025-12-30
    """
    if to_date is None:
        to_date = date_type.today()
    if from_date is None:
        from_date = to_date - timedelta(days=30)

    return compute_reasoning_trends(
        date_from=from_date,
        date_to=to_date,
    )


@router.get("/ui/reasoning", response_class=HTMLResponse)
def ui_reasoning(
    request: Request,
    from_date: date_type | None = Query(None),
    to_date: date_type | None = Query(None),
):
    """
    HTML dashboard for reasoning analysis.
    """
    if to_date is None:
        to_date = date_type.today()
    if from_date is None:
        from_date = to_date - timedelta(days=30)

    data = compute_reasoning_trends(
        date_from=from_date,
        date_to=to_date,
    )

    display_range = f"{from_date.strftime('%d %b %Y')} - {to_date.strftime('%d %b %Y')}"

    return templates.TemplateResponse(
        "reasoning.html",
        {
            "request": request,
            "data": data,
            "from_date": from_date,
            "to_date": to_date,
            "display_range": display_range,
            "has_data": data.get("has_data", False),
        },
    )
