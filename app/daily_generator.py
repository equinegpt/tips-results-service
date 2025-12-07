from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, List, Tuple, Optional, Sequence, Set

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


def _meeting_has_big_maiden(
    race_list: List[Dict[str, Any]],
    threshold: int = 29000,
) -> bool:
    """
    Return True if ANY race in this meeting is a Maiden-class event
    with prize money strictly above `threshold` (e.g. > 29,000).

    Maiden detection rules (RA schema):
      • PRIMARY: class field contains "Maiden"/"MDN"
          e.g. class="Maiden"
      • BACKUP: description/name contains "Maiden"/"MDN"
          e.g. class="BM65", description="F&M Maiden Plate"

    A race qualifies as a "big maiden" if:
      (class_is_maiden OR desc_is_maiden) AND prize > threshold
    """

    for r in race_list:
        # -----------------------------
        # 1) Class + Description text
        # -----------------------------
        class_text = (
            r.get("class")
            or r.get("class_text")
            or r.get("raceClass")
            or r.get("race_class")
            or ""
        )

        desc_text = (
            r.get("description")
            or r.get("race_name")
            or r.get("name")
            or ""
        )

        lc_class = str(class_text).lower()
        lc_desc  = str(desc_text).lower()

        # PRIMARY: class is Maiden
        class_is_maiden = ("maiden" in lc_class) or ("mdn" in lc_class)

        # BACKUP: description contains Maiden (even if class is BM65 etc.)
        desc_is_maiden = ("maiden" in lc_desc) or ("mdn" in lc_desc)

        is_maiden = class_is_maiden or desc_is_maiden
        if not is_maiden:
            continue

        # -----------------------------
        # 2) Prize / prizemoney
        # -----------------------------
        raw_prize = (
            r.get("prize")           # your RA example: "prize": 42500
            or r.get("prizemoney")
            or r.get("prize_total")
            or r.get("total_prize")
        )

        if raw_prize is None:
            continue

        # Be tolerant of "42500", "42,500", "42500.0"
        prize_val: int
        try:
            prize_val = int(raw_prize)
        except (TypeError, ValueError):
            if isinstance(raw_prize, str):
                digits = "".join(ch for ch in raw_prize if ch.isdigit())
                if not digits:
                    continue
                prize_val = int(digits)
            else:
                try:
                    prize_val = int(float(raw_prize))
                except Exception:
                    continue

        if prize_val > threshold:
            # Optional debug – helps confirm Werribee is being picked up:
            # print(f"[BIG-MDN] {r.get('track')} R{r.get('race_no')} "
            #       f"{desc_text!r} class={class_text!r} prize={prize_val}")
            return True

    return False

# ----------------------------
# BUILD PAYLOADS
# ----------------------------

def build_generate_tips_payloads_for_date(
    target_date: date,
    project_id: str,
    *,
    force_all_meetings: bool = False,
    track_types: Optional[Sequence[str]] = None,
) -> List[schemas.GenerateTipsIn]:
    """
    Build one GenerateTipsIn payload per meeting for the given date,
    using RA Crawler /races as the source of truth, PF for scratchings + conditions.

    Selection rules:

      • Always include Metro (type == "M") meetings.
      • Always include Provincial (type == "P") meetings.
      • Treat everything else as Country:

          - If force_all_meetings=False (normal daily cron):
              Include a Country meeting ONLY if it has at least one
              Maiden-class race with prize > 29,000.

          - If force_all_meetings=True (single-meeting override):
              Include all meetings (M / P / C), regardless of prize.

      • HK / NZ are always excluded by state/country.

    track_types (optional):
      • If None (default):
          Use the above rules with M + P always allowed and C subject
          to the big-maiden rule.
      • If provided (e.g. ["M", "P"] or ["M", "P", "C"]):
          Acts as an explicit allowlist of meeting types when
          force_all_meetings is False.

    Manual overrides:
      - /cron/generate-meeting-tips uses force_all_meetings=True so you
        can always pull ANY meeting (even small Country cards) by pf_meeting_id.
    """
    races = _fetch_ra_races_for_date(target_date)
    scratchings_lookup = _fetch_pf_scratchings_lookup(target_date)
    conditions_lookup = _fetch_pf_track_conditions(target_date)

    # Normalise track_types into an allowlist of meeting types.
    # Only applied when force_all_meetings == False.
    if track_types is not None:
        include_types: Set[str] = {t.upper() for t in track_types}
    else:
        # Default: we conceptually allow M, P, C; C is then filtered by the
        # big-maiden rule below.
        include_types = {"M", "P", "C"}

    # Group by (date, track_name, state)
    meetings: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)

    for r in races:
        state = r.get("state")
        country = r.get("country")

        # Exclude HK and NZ
        if state in {"HK", "NZ"} or country in {"HK", "NZ"}:
            continue

        track_name = r.get("track") or r.get("track_name")
        if not track_name:
            continue

        date_str = r.get("date") or target_date.isoformat()
        key = (date_str, track_name, state or "")

        meetings[key].append(r)

    payloads: List[schemas.GenerateTipsIn] = []

    for (date_str, track_name, state), race_list in meetings.items():
        # Parse date if we can
        meeting_date = target_date
        try:
            meeting_date = date.fromisoformat(date_str[:10])
        except Exception:
            pass

        # ---------------------------
        # Meeting type: M / P / C
        # ---------------------------
        raw_type = (
            race_list[0].get("type")
            or race_list[0].get("meeting_type")
            or race_list[0].get("location")  # fallback if PF-style data present
            or ""
        )
        meeting_type = str(raw_type).strip().upper() or "C"

        is_metro = meeting_type == "M"
        is_prov = meeting_type == "P"
        is_country = not (is_metro or is_prov)  # treat everything else as Country

        # When force_all_meetings=True, we deliberately bypass *all* filters
        # (meeting_type allowlist + big-maiden rule) so overrides always work.
        if not force_all_meetings:
            # 1) Honour explicit type allowlist if provided
            if meeting_type not in include_types:
                print(
                    f"[DG] Skipping meeting {track_name} {state} on {meeting_date} "
                    f"(type={meeting_type}) – not in track_types filter {include_types}"
                )
                continue

            # 2) Country big-maiden rule
            if is_country:
                if not _meeting_has_big_maiden(race_list, threshold=29000):
                    print(
                        f"[DG] Skipping COUNTRY meeting {track_name} {state} on "
                        f"{meeting_date} (type={meeting_type}) – no Maiden > 29k"
                    )
                    continue
            # Metro & Provincial always included if we reach here

        # --- pf_meeting_id from RA /races (must be explicitly mapped) ---
        pf_meeting_id: int | None = None
        for rec in race_list:
            raw_mid = rec.get("pf_meeting_id") or rec.get("pfMeetingId")
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

        # Meeting dict (same schema as before)
        meeting_dict = {
            "date": meeting_date,
            "track_name": track_name,
            "state": state,
            "country": "AUS",
            "pf_meeting_id": pf_meeting_id,
            "ra_meetcode": (
                race_list[0].get("ra_meetcode")
                or race_list[0].get("meetingId")
                or race_list[0].get("meeting_id")
            ),
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
