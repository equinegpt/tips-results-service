# app/routes_cron.py
from __future__ import annotations

from datetime import date as date_type, datetime, timedelta
from zoneinfo import ZoneInfo
import traceback

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from .database import get_db
from . import results_ra, pf_results, stats_rollup, daily_generator, schemas  # or similar


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


@router.post("/cron/fetch-pf-results", response_model=schemas.FetchPfResultsOut)
def cron_fetch_pf_results(
    date_str: str = Query(..., alias="date"),
    db: Session = Depends(get_db),
):
    """
    Cron endpoint: import PuntingForm post-race + Skynet prices
    for all Australian races on a given date.

    If anything blows up, we:
      - log the traceback
      - return ok=False with an 'error' string (status 200)
    """
    target_date = date_type.fromisoformat(date_str)

    try:
        race_results_inserted = pf_results.import_pf_results_for_date(
            db=db,
            target_date=target_date,
        )
    except Exception as e:
        print(
            f"[CRON] fetch-pf-results FAILED for {target_date}: {repr(e)}"
        )
        traceback.print_exc()
        db.rollback()

        return schemas.FetchPfResultsOut(
            ok=False,
            date=target_date.isoformat(),
            race_results_inserted=0,
            error=str(e),
        )

    return schemas.FetchPfResultsOut(
        ok=True,
        date=target_date.isoformat(),
        race_results_inserted=race_results_inserted,
        error=None,
    )