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


def _normalize(s: str) -> str:
    """Lowercase, strip sponsors, collapse spaces for fuzzy matching."""
    import re
    t = s.lower().strip()
    for sp in ("sportsbet", "ladbrokes", "bet365", "picklebet",
               "thomas farms", "aquis park", "aquis", "tabtouch"):
        t = t.replace(sp, "")
    t = re.sub(r"[^a-z0-9]+", " ", t).strip()
    return t


def fetch_results_for_date(target_date: date, db: Session) -> int:
    """
    Main entrypoint: fetch RA results from crawler for `target_date` and
    attach to EXISTING meetings/races that have tips.

    Does NOT create new Meeting/Race rows — only attaches results
    to meetings that were tipped (M/P/big-maiden C).

    Returns the number of RaceResult rows inserted/updated.
    """
    from .ra_results_client import RAResultsClient

    client = RAResultsClient()
    try:
        ra_rows = client.fetch_results_for_date(target_date)
    except Exception as e:
        print(f"[RA] error fetching results: {e}")
        return 0
    finally:
        client.close()

    if not ra_rows:
        print(f"[RA] no results returned for {target_date}")
        return 0

    print(f"[RA] fetched {len(ra_rows)} result rows for {target_date}")

    # Load ALL existing meetings for this date (these are the ones with tips)
    existing_meetings = (
        db.query(models.Meeting)
        .filter(models.Meeting.date == target_date)
        .all()
    )

    if not existing_meetings:
        print(f"[RA] no existing meetings in TRS for {target_date} — nothing to match")
        return 0

    # Build a fuzzy lookup: normalised(track_name) + state → Meeting
    meeting_lookup: dict[tuple, models.Meeting] = {}
    for m in existing_meetings:
        key = (_normalize(m.track_name), (m.state or "").upper())
        meeting_lookup[key] = m
        # Also add without state for looser matching
        meeting_lookup[(_normalize(m.track_name),)] = m

    print(f"[RA] {len(existing_meetings)} existing meetings to match against")

    # Group RA results by (track, state)
    from collections import defaultdict
    grouped: dict[tuple, list] = defaultdict(list)
    for rr in ra_rows:
        key = (rr.track, rr.state)
        grouped[key].append(rr)

    total_updates = 0

    for (track_name, state), rows in grouped.items():
        if not track_name:
            continue

        # --- Find existing Meeting (fuzzy match) ---
        norm_track = _normalize(track_name)
        norm_state = (state or "").upper()

        meeting = meeting_lookup.get((norm_track, norm_state))

        # Try without state
        if not meeting:
            meeting = meeting_lookup.get((norm_track,))

        # Try substring match (e.g., "swan hill" in "swan hill racecourse")
        if not meeting:
            for (key_parts), m in meeting_lookup.items():
                if len(key_parts) >= 1:
                    existing_norm = key_parts[0] if isinstance(key_parts, tuple) else key_parts
                else:
                    continue
                if isinstance(existing_norm, str) and (
                    norm_track in existing_norm or existing_norm in norm_track
                ):
                    meeting = m
                    break

        if not meeting:
            # No tipped meeting for this track — skip (don't create orphans)
            continue

        # Group rows by race_no
        races_grouped: dict[int, list] = defaultdict(list)
        for rr in rows:
            races_grouped[rr.race_no].append(rr)

        for race_number, runners in races_grouped.items():
            # --- Find existing Race (don't create new ones) ---
            race = (
                db.query(models.Race)
                .filter(
                    models.Race.meeting_id == meeting.id,
                    models.Race.race_number == race_number,
                )
                .first()
            )

            if not race:
                # No tipped race for this number — skip
                continue

            for runner in runners:
                tab_number = runner.tab_number
                horse_name = runner.horse_name or f"Runner #{tab_number}"
                finish_position = runner.finishing_pos
                is_scratched = runner.is_scratched

                if is_scratched:
                    status = "SCRATCHED"
                elif finish_position is not None:
                    status = "RUN"
                else:
                    status = "NO_RESULT"

                margin_text = str(runner.margin_lens) if runner.margin_lens is not None else None
                starting_price = _to_decimal_or_none(runner.starting_price)

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
