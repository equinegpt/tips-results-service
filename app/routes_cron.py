# app/routes_cron.py
from __future__ import annotations

from datetime import date as date_type, datetime, timedelta
from zoneinfo import ZoneInfo
import traceback

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from .database import get_db
from . import (
    results_ra,
    pf_results,
    stats_rollup,
    daily_generator,
    schemas,
    models,
)

# pull the internal helpers we’re using from pf_results
from .pf_results import (
    _fetch_pf_post_race,
    _fetch_skynet_prices_for_date,
    _attach_tip_outcomes_from_existing_results_for_date,
    _apply_skynet_sp_to_existing_results_for_date,
    _to_int,
    _to_decimal,
)

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


# app/routes_cron.py

@router.post("/cron/fetch-pf-results", response_model=schemas.FetchPfResultsOut)
def cron_fetch_pf_results(
    date: date_type = Query(..., alias="date"),
    db: Session = Depends(get_db),
):
    """
    For a given date:

      1) For every Meeting on that date with a pf_meeting_id, and every Race
         in that meeting, call PF /v2/ireel/post-race.
      2) Upsert RaceResult rows (provider='PF') per runner.
      3) Attach Skynet tabCurrentPrice as starting_price via a
         (meetingId, raceNumber, tabNumber) lookup.
      4) Overlay Skynet SP onto any existing RaceResult (RA or PF) that still
         has no starting_price.
      5) Backfill TipOutcome rows from RaceResult.
    """
    target_date = date
    print(f"[CRON] fetch-pf-results for {target_date}")

    # ----------------------------------------
    # 1) Meetings for this date
    # ----------------------------------------
    try:
        meetings = (
            db.query(models.Meeting)
            .filter(models.Meeting.date == target_date)
            .all()
        )
    except Exception as e:
        err = f"DB query for meetings failed: {repr(e)}"
        print(f"[PF] {err}")
        return schemas.FetchPfResultsOut(
            ok=False,
            date=target_date.isoformat(),
            race_results_inserted=0,
            error=err,
        )

    if not meetings:
        print(f"[CRON] fetch-pf-results: no meetings for {target_date}")
        return schemas.FetchPfResultsOut(
            ok=True,
            date=target_date.isoformat(),
            race_results_inserted=0,
            error=None,
        )

    # ----------------------------------------
    # 2) Skynet price map for the whole day
    # ----------------------------------------
    try:
        skynet_prices = pf_results._fetch_skynet_prices_for_date(target_date)
        print(
            f"[DEBUG] Skynet map size for {target_date}: "
            f"{len(skynet_prices)} entries"
        )
    except Exception as e:
        err = f"Skynet prices fetch failed for {target_date}: {repr(e)}"
        print(f"[PF] {err}")
        skynet_prices = {}

    race_results_inserted = 0

    # ----------------------------------------
    # 3) Loop meetings & races → PF post-race
    # ----------------------------------------
    for meeting in meetings:
        pf_meeting_id = getattr(meeting, "pf_meeting_id", None)
        if not pf_meeting_id:
            # Can't map to PF / Skynet without a meetingId
            continue

        print(
            f"[PF] processing meeting pf_meeting_id={pf_meeting_id} "
            f"{meeting.track_name} {meeting.state} on {meeting.date}"
        )

        for race in sorted(meeting.races, key=lambda r: r.race_number or 0):
            race_no = race.race_number
            if race_no is None:
                continue

            runners = pf_results._fetch_pf_post_race(pf_meeting_id, race_no)
            if not runners:
                continue

            for r in runners:
                if not isinstance(r, dict):
                    continue

                # Normalise core keys
                meeting_id = pf_results._to_int(
                    r.get("meetingId") or pf_meeting_id
                )
                race_number = pf_results._to_int(
                    r.get("raceNo") or race_no
                )
                tab_no = pf_results._to_int(r.get("tabNo"))

                if not (meeting_id and race_number and tab_no):
                    continue

                # -----------------------------
                # Skynet SP lookup
                # -----------------------------
                sp = skynet_prices.get((meeting_id, race_number, tab_no))

                # Fallback: any SP-ish field from PF if Skynet missing
                if sp is None:
                    sp = pf_results._to_decimal(
                        r.get("tabCurrentPrice")
                        or r.get("startingPrice")
                        or r.get("sp")
                    )
                if sp is not None and sp == 0:
                    sp = None

                # -----------------------------
                # Upsert RaceResult (provider='PF')
                # -----------------------------
                rr = (
                    db.query(models.RaceResult)
                    .filter(
                        models.RaceResult.race_id == race.id,
                        models.RaceResult.tab_number == tab_no,
                        models.RaceResult.provider == "PF",
                    )
                    .one_or_none()
                )

                if rr is None:
                    rr = models.RaceResult(
                        race_id=race.id,
                        provider="PF",
                        tab_number=tab_no,
                    )
                    db.add(rr)
                    race_results_inserted += 1

                rr.finish_position = pf_results._to_int(r.get("posFin"))
                rr.margin = r.get("margFin")
                rr.starting_price = sp
                if hasattr(rr, "raw"):
                    rr.raw = r

    db.commit()

    # ----------------------------------------
    # 4) Overlay Skynet SP onto any existing rows missing SP (RA or PF)
    # ----------------------------------------
    overlay_count = 0
    if skynet_prices:
        try:
            overlay_count = pf_results._apply_skynet_sp_to_existing_results_for_date(
                target_date=target_date,
                skynet_prices=skynet_prices,
                db=db,
            )
            if overlay_count:
                db.commit()
            print(
                f"[CRON] fetch-pf-results overlay: "
                f"set SP on {overlay_count} existing RaceResult rows "
                f"for {target_date}"
            )
        except Exception as e:
            print(
                f"[PF] error while overlaying Skynet SP for {target_date}: "
                f"{repr(e)}"
            )

    # ----------------------------------------
    # 5) Backfill TipOutcome rows from RaceResult
    # ----------------------------------------
    try:
        attached = pf_results._attach_tip_outcomes_from_existing_results_for_date(
            target_date, db
        )
        if attached:
            db.commit()
        print(
            f"[CRON] fetch-pf-results: attached {attached} TipOutcome rows "
            f"for {target_date}"
        )
    except Exception as e:
        print(
            f"[PF] error while attaching TipOutcome rows for {target_date}: "
            f"{repr(e)}"
        )

    return schemas.FetchPfResultsOut(
        ok=True,
        date=target_date.isoformat(),
        race_results_inserted=race_results_inserted,
        error=None,
    )
