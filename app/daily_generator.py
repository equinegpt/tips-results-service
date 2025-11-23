from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Tuple

import re
import httpx
from zoneinfo import ZoneInfo

from .config import settings
from . import schemas


def today_mel() -> date:
    """Return 'today' in Australia/Melbourne."""
    return datetime.now(ZoneInfo("Australia/Melbourne")).date()


# ----------------------------
# TRACK NAME MATCHING (RA ↔ PF)
# ----------------------------

def _normalize_track_name(name: str | None) -> str:
    """Lowercase, collapse spaces; safe on None."""
    if not name:
        return ""
    return re.sub(r"\s+", " ", name).strip().lower()


def _tracks_match(a: str, b: str) -> bool:
    """
    Fuzzy match RA vs PF track names.

    Handles things like:
      - 'bet365 Park Kyneton' vs 'Kyneton'
      - 'Canterbury Park' vs 'Canterbury'
      - 'Caulfield Heath' vs 'Caulfield'
    """
    na = _normalize_track_name(a)
    nb = _normalize_track_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    # Starts/ends with
    if na.startswith(nb) or na.endswith(nb) or nb.startswith(na) or nb.endswith(na):
        return True
    # Simple substring
    if nb in na or na in nb:
        return True
    return False


# ----------------------------
# RA RACES
# ----------------------------

def _fetch_ra_races_for_date(target_date: date) -> List[Dict[str, Any]]:
    """
    Call RA Crawler /races for a specific date.

    RA Crawler shape (example):

      [
        {
          "id": 4120,
          "race_no": 3,
          "date": "2025-11-22",
          "state": "WA",
          "meetingId": null,
          "track": "Ascot",
          "type": "M",
          "description": "RAILWAY STAKES",
          "prize": 1500000,
          "condition": "Hcp",
          "class": "G1",
          "age": "3+",
          "sex": "Open",
          "distance_m": 1600,
          "bonus": "...",
          "url": "https://www.racingaustralia.horse/FreeFields/RaceProgram.aspx?Key=..."
        },
        ...
      ]
    """
    base = settings.ra_crawler_base_url.rstrip("/")
    url = f"{base}/races"
    params = {"date": target_date.isoformat()}

    with httpx.Client(timeout=20.0) as client:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

    if isinstance(data, dict) and "races" in data:
        races = data["races"]
    else:
        races = data

    if not isinstance(races, list):
        raise ValueError("Unexpected /races response format; expected list or {'races': [...]}")

    target_str = target_date.isoformat()
    filtered: List[Dict[str, Any]] = []
    for r in races:
        race_date = r.get("date") or r.get("meeting_date")
        if isinstance(race_date, str) and race_date[:10] != target_str:
            continue
        filtered.append(r)

    return filtered


# ----------------------------
# PF SCRATCHINGS (TODAY, NO DATE PARAM)
# ----------------------------

def _fetch_pf_scratchings_lookup(target_date: date) -> Dict[Tuple[str, int], List[int]]:
    """
    Hit PF scratchings grouped endpoint and build a lookup by (track, raceNo).

    NOTE: we now ignore target_date and always use *today's* PF scratchings,
    via GET /scratchings/grouped with NO date param. This is designed for
    a cron that runs the morning of the races.
    """
    base = settings.pf_scratchings_base_url.rstrip("/")
    url = f"{base}/scratchings/grouped"

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"[SCR] error fetching scratchings: {e}")
        return {}

    meetings = data.get("meetings") if isinstance(data, dict) else None
    if not isinstance(meetings, list):
        return {}

    lookup: Dict[Tuple[str, int], List[int]] = {}

    for m in meetings:
        if not isinstance(m, dict):
            continue
        track = m.get("track")
        if not isinstance(track, str):
            continue
        races = m.get("races") or []
        for rr in races:
            if not isinstance(rr, dict):
                continue
            race_no = rr.get("raceNo") or rr.get("race_number")
            if race_no is None:
                continue
            try:
                rn = int(race_no)
            except Exception:
                continue
            scr_list = rr.get("scratchings") or []
            if not isinstance(scr_list, list):
                continue
            ints: List[int] = []
            for s in scr_list:
                try:
                    ints.append(int(s))
                except Exception:
                    pass
            lookup[(track, rn)] = ints

    print(
        f"[SCR] fetched scratchings for {len(lookup)} (track, raceNo) keys "
        f"(PF today, ignoring target_date={target_date})"
    )
    return lookup


# ----------------------------
# PF CONDITIONS (flat, TODAY, NO DATE PARAM)
# ----------------------------

def _fetch_pf_track_conditions(target_date: date) -> Dict[str, str]:
    """
    Fetch track conditions per meeting from PF service via /conditions/flat.

    NOTE: we now ignore target_date and always use *today's* PF conditions,
    via GET /conditions/flat with NO date param. PF is responsible for
    returning today's meetings/conditions.
    """
    base = settings.pf_scratchings_base_url.rstrip("/")
    url = f"{base}/conditions/flat"

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"[COND] error fetching track conditions: {e}")
        return {}

    rows = None
    if isinstance(data, dict):
        rows = data.get("rows") or data.get("meetings") or data.get("data")
    if rows is None and isinstance(data, list):
        rows = data
    if not isinstance(rows, list):
        return {}

    lookup: Dict[str, str] = {}

    for row in rows:
        if not isinstance(row, dict):
            continue

        track = row.get("track") or row.get("track_name")
        if not isinstance(track, str):
            continue

        cond = (
            row.get("trackCondition")
            or row.get("track_condition")
            or row.get("condition")
            or row.get("trackConditionText")
            or row.get("code")
        )
        number = row.get("trackConditionNumber") or row.get("conditionNumber")

        if not isinstance(cond, str):
            continue

        cond_str = cond
        if isinstance(number, (int, str)) and str(number).strip():
            cond_str = f"{cond}{str(number).strip()}"

        lookup[track] = cond_str

    print(
        f"[COND] fetched {len(lookup)} track conditions from PF "
        f"(ignoring target_date={target_date})"
    )
    return lookup


# ----------------------------
# BUILD PAYLOADS
# ----------------------------

def build_generate_tips_payloads_for_date(
    target_date: date,
    project_id: str,
    *,
    track_types: set[str] | None = None,
) -> List[schemas.GenerateTipsIn]:
    """
    Build one GenerateTipsIn payload per meeting for the given date,
    using RA Crawler /races as the source of truth, PF for scratchings + conditions.

    - Includes AUS meetings for that date (HK/NZ excluded)
    - If track_types is not None, only keep RA races whose `type` is in that set
      (e.g. {'M', 'P'} for Metro + Provincial).
    - PF scratchings + conditions always use PF's "today" view.
    - pf_meeting_id is taken from RA /races (meetingId/pf_meeting_id/etc).
    """
    races = _fetch_ra_races_for_date(target_date)
    scratchings_lookup = _fetch_pf_scratchings_lookup(target_date)
    conditions_lookup = _fetch_pf_track_conditions(target_date)

    # Group by (date, track_name, state)
    meetings: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)

    for r in races:
        state = r.get("state")
        country = r.get("country")

        # Exclude HK and NZ
        if state in {"HK", "NZ"} or country in {"HK", "NZ"}:
            continue

        # RA meeting type: 'M', 'P', 'C', etc.
        track_type = (r.get("type") or r.get("track_type") or "").strip().upper()
        if track_types is not None and track_type and track_type not in track_types:
            # e.g. skip 'C' when track_types = {'M', 'P'}
            continue

        track_name = r.get("track") or r.get("track_name")
        if not track_name:
            continue

        date_str = (
            r.get("date")
            or r.get("meeting_date")
            or target_date.isoformat()
        )

        key = (date_str[:10], track_name, state or "")
        meetings[key].append(r)

    payloads: List[schemas.GenerateTipsIn] = []

    for (date_str, track_name, state), race_list in meetings.items():
        # Parse date if we can
        meeting_date = target_date
        try:
            meeting_date = date.fromisoformat(date_str[:10])
        except Exception:
            pass

        # --- pf_meeting_id from RA /races (meetingId etc) ---
        pf_meeting_id: int | None = None
        for rec in race_list:
            raw_mid = (
                rec.get("pf_meeting_id")
                or rec.get("pfMeetingId")
                or rec.get("meetingId")
                or rec.get("meeting_id")
            )
            if raw_mid is None:
                continue
            try:
                pf_meeting_id = int(raw_mid)
                break
            except Exception:
                continue

        if pf_meeting_id is None:
            print(
                f"[PFID] no pf_meeting_id found in RA races for "
                f"{track_name} {state} on {meeting_date}"
            )

        # --- Track condition: exact, then fuzzy via _tracks_match ---
        track_condition_for_meeting: str | None = None

        # Exact key match first
        if track_name in conditions_lookup:
            track_condition_for_meeting = conditions_lookup[track_name]
        else:
            # Fuzzy match RA track_name vs PF track keys
            for cond_track, cond_value in conditions_lookup.items():
                if _tracks_match(track_name, cond_track):
                    track_condition_for_meeting = cond_value
                    print(
                        f"[COND] matched RA track '{track_name}' to PF track "
                        f"'{cond_track}' → condition='{cond_value}'"
                    )
                    break

        # Meeting dict (now includes pf_meeting_id)
        meeting_dict = {
            "date": meeting_date,
            "track_name": track_name,
            "state": state,
            "country": "AUS",
            "pf_meeting_id": pf_meeting_id,
            "ra_meetcode": race_list[0].get("ra_meetcode"),
        }

        # Build races array (race + scratchings + track_condition)
        races_ctx: List[Dict[str, Any]] = []

        def _race_sort_key(rec: Dict[str, Any]) -> int:
            return rec.get("race_number") or rec.get("race_no") or 0

        for r in sorted(race_list, key=_race_sort_key):
            race_no = r.get("race_number") or r.get("race_no")
            race_dict = {
                "race_number": race_no,
                "name": r.get("name") or r.get("race_name") or r.get("description"),
                "distance_m": r.get("distance_m") or r.get("distance"),
                "class_text": r.get("class_text") or r.get("class"),
                "scheduled_start": None,  # can be wired from RA later
            }

            # --- Scratchings: exact, then fuzzy track match with same raceNo ---
            scratchings: List[int] = []
            rn = None
            if race_no is not None:
                try:
                    rn = int(race_no)
                except Exception:
                    rn = None

            if rn is not None:
                # Exact (track_name, rn)
                scratchings = scratchings_lookup.get((track_name, rn), []) or []

                if not scratchings:
                    for (scr_track, scr_rn), scr_list in scratchings_lookup.items():
                        if scr_rn != rn:
                            continue
                        if _tracks_match(track_name, scr_track):
                            scratchings = scr_list
                            print(
                                f"[SCR] matched RA track '{track_name}' R{rn} "
                                f"to PF track '{scr_track}' → scratchings={scratchings}"
                            )
                            break

            races_ctx.append(
                {
                    "race": race_dict,
                    "scratchings": scratchings,
                    "track_condition": track_condition_for_meeting,
                }
            )

        tip_run_dict = {
            "source": "iReel",
            "model_version": "ireel-auto-v1",
            "project_id": project_id,
            "meta": {
                "generated_by": "daily_generator",
                "ra_source": "ra-crawler",
                "project_id": project_id,
            },
        }

        payloads.append(
            schemas.GenerateTipsIn(
                tip_run=tip_run_dict,
                meeting=meeting_dict,
                races=races_ctx,
            )
        )

    return payloads
