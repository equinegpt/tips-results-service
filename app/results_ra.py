# app/results_ra.py

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any, Dict, List

import httpx
from sqlalchemy.orm import Session

from .config import settings
from . import models


def _fetch_ra_results_from_crawler(target_date: date) -> List[Dict[str, Any]]:
    """
    Fetch RA results for a given date from the RA crawler service.

    We assume an endpoint roughly like:

      GET {ra_crawler_base_url}/results?date=YYYY-MM-DD

    returning JSON shaped *approximately* as:

      {
        "date": "2025-11-18",
        "meetings": [
          {
            "track": "bet365 Park Kyneton",
            "state": "VIC",
            "country": "AUS",
            "ra_meetcode": "VIC_KYNT_20251118",
            "races": [
              {
                "raceNo": 7,
                "raceName": "Global Turf Handicap",
                "results": [
                  {
                    "tabNumber": 3,
                    "horseName": "Winner Horse",
                    "finishPosition": 1,
                    "status": "RUN",
                    "margin": "1.5L",
                    "startingPrice": 3.6
                  },
                  ...
                ]
              },
              ...
            ]
          },
          ...
        ]
      }

    Because I can't see the actual crawler JSON from here, the mapping
    below is *deliberately* defensive and uses multiple key options.
    You should tweak the `get(...)` calls to match your real shape.
    """
    base = settings.ra_crawler_base_url.rstrip("/")
    url = f"{base}/results"
    params = {"date": target_date.isoformat()}

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"[RA] error fetching results from crawler: {e}")
        return []

    # Normalise to a list of "meeting" dicts
    meetings: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        meetings = data.get("meetings") or data.get("data") or data.get("results") or []
    elif isinstance(data, list):
        # Could be a list of meetings already
        meetings = data
    else:
        return []

    if not isinstance(meetings, list):
        return []

    print(f"[RA] crawler returned {len(meetings)} meetings worth of results for {target_date}")
    return meetings


def _to_decimal_or_none(val: Any) -> Decimal | None:
    if val is None:
        return None
    try:
        # Often SP comes back as float or string
        return Decimal(str(val))
    except Exception:
        return None


def fetch_results_for_date(target_date: date, db: Session) -> int:
    """
    Main entrypoint: fetch RA results from crawler for `target_date` and
    upsert into RaceResult (and, if necessary, Meeting/Race).

    Returns the number of RaceResult rows inserted/updated.
    """
    meetings_json = _fetch_ra_results_from_crawler(target_date)
    if not meetings_json:
        print(f"[RA] no results returned for {target_date}")
        return 0

    total_updates = 0

    for m in meetings_json:
        if not isinstance(m, dict):
            continue

        track_name = (
            m.get("track")
            or m.get("track_name")
            or m.get("meeting_name")
        )
        if not isinstance(track_name, str):
            continue

        state = (m.get("state") or m.get("State") or "").strip() or "VIC"
        country = (m.get("country") or "AUS").strip() or "AUS"
        ra_meetcode = (
            m.get("ra_meetcode")
            or m.get("meetCode")
            or m.get("meetingCode")
        )

        # --- Find or create Meeting ---
        query = db.query(models.Meeting)
        meeting: models.Meeting | None = None

        if ra_meetcode:
            meeting = query.filter(models.Meeting.ra_meetcode == ra_meetcode).first()

        if not meeting:
            meeting = (
                query.filter(
                    models.Meeting.date == target_date,
                    models.Meeting.track_name == track_name,
                    models.Meeting.state == state,
                )
                .first()
            )

        if not meeting:
            # If this meeting wasn't created via daily_generator,
            # create it now so results still have a home.
            meeting = models.Meeting(
                date=target_date,
                track_name=track_name,
                state=state,
                country=country,
                pf_meeting_id=None,
                ra_meetcode=ra_meetcode,
            )
            db.add(meeting)
            db.flush()
            print(f"[RA] created Meeting for {track_name} {state} {target_date}")

        races_json = m.get("races") or m.get("results") or []
        if not isinstance(races_json, list):
            continue

        for rj in races_json:
            if not isinstance(rj, dict):
                continue

            race_no = rj.get("raceNo") or rj.get("race_number") or rj.get("number")
            try:
                race_number = int(race_no)
            except Exception:
                continue

            race_name = (
                rj.get("raceName")
                or rj.get("name")
                or rj.get("race_name")
            )

            # --- Find or create Race ---
            race = (
                db.query(models.Race)
                .filter(
                    models.Race.meeting_id == meeting.id,
                    models.Race.race_number == race_number,
                )
                .first()
            )

            if not race:
                race = models.Race(
                    meeting_id=meeting.id,
                    race_number=race_number,
                    name=race_name,
                    distance_m=None,
                    class_text=None,
                    scheduled_start=None,
                )
                db.add(race)
                db.flush()
                print(
                    f"[RA] created Race R{race_number} for "
                    f"{meeting.track_name} {meeting.date}"
                )

            runners = (
                rj.get("results")
                or rj.get("runners")
                or rj.get("participants")
                or []
            )
            if not isinstance(runners, list):
                continue

            for runner in runners:
                if not isinstance(runner, dict):
                    continue

                tab_raw = (
                    runner.get("tabNumber")
                    or runner.get("tab_number")
                    or runner.get("tabNo")
                    or runner.get("saddle_number")
                    or runner.get("saddle")
                    or runner.get("number")
                )
                try:
                    tab_number = int(tab_raw)
                except Exception:
                    continue

                horse_name = (
                    runner.get("horseName")
                    or runner.get("horse_name")
                    or runner.get("runner_name")
                    or runner.get("name")
                )
                if not isinstance(horse_name, str):
                    horse_name = f"Runner #{tab_number}"

                finish_raw = (
                    runner.get("finishPosition")
                    or runner.get("finish_position")
                    or runner.get("position")
                    or runner.get("pos")
                )
                try:
                    finish_position = int(finish_raw) if finish_raw is not None else None
                except Exception:
                    finish_position = None

                status = (
                    runner.get("status")
                    or runner.get("result_status")
                    or runner.get("race_status")
                )

                margin_text = (
                    runner.get("margin_text")
                    or runner.get("margin")
                    or runner.get("marginText")
                )

                sp_raw = (
                    runner.get("startingPrice")
                    or runner.get("starting_price")
                    or runner.get("sp")
                    or runner.get("win_dividend")
                )
                starting_price = _to_decimal_or_none(sp_raw)

                # --- Upsert RaceResult ---
                rr = (
                    db.query(models.RaceResult)
                    .filter(
                        models.RaceResult.provider == "RA",
                        models.RaceResult.race_id == race.id,
                        models.RaceResult.tab_number == tab_number,
                    )
                    .first()
                )

                if not rr:
                    rr = models.RaceResult(
                        provider="RA",
                        race_id=race.id,
                        tab_number=tab_number,
                        horse_name=horse_name,
                        finish_position=finish_position,
                        status=status,
                        margin_text=margin_text,
                        starting_price=starting_price,
                    )
                    db.add(rr)
                    total_updates += 1
                else:
                    rr.horse_name = horse_name
                    rr.finish_position = finish_position
                    rr.status = status
                    rr.margin_text = margin_text
                    rr.starting_price = starting_price
                    total_updates += 1

    db.commit()
    print(f"[RA] upserted {total_updates} RaceResult rows for {target_date}")
    return total_updates
