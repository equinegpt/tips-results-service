# app/routes_cron.py
from __future__ import annotations

from datetime import date as date_type, datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from .database import get_db
from . import results_ra, pf_results

router = APIRouter()


@router.post("/cron/fetch-ra-results")
def cron_fetch_ra_results(
    target_date: date_type | None = Query(
        None,
        alias="date",
        description=(
            "Date whose results to fetch (YYYY-MM-DD). "
            "If omitted, uses *yesterday* in Australia/Melbourne."
        ),
    ),
    db: Session = Depends(get_db),
):
    if target_date is None:
        mel_today = datetime.now(ZoneInfo("Australia/Melbourne")).date()
        target_date = mel_today - timedelta(days=1)

    inserted_rows = results_ra.fetch_results_for_date(target_date, db=db)

    return {
        "ok": True,
        "date": target_date.isoformat(),
        "race_results_inserted": inserted_rows,
    }


@router.post("/cron/fetch-pf-results")
def cron_fetch_pf_results(
    target_date: date_type | None = Query(
        None,
        alias="date",
        description=(
            "Date whose PF results to fetch (YYYY-MM-DD). "
            "If omitted, uses *yesterday* in Australia/Melbourne."
        ),
    ),
    db: Session = Depends(get_db),
):
    if target_date is None:
        mel_today = datetime.now(ZoneInfo("Australia/Melbourne")).date()
        target_date = mel_today - timedelta(days=1)

    inserted_rows = pf_results.import_pf_results_for_date(target_date, db=db)

    return {
        "ok": True,
        "date": target_date.isoformat(),
        "race_results_inserted": inserted_rows,
    }
