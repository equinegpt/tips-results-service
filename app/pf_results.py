# app/pf_results.py
from __future__ import annotations

from datetime import date as date_type
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy.orm import Session

from . import models

# ---- PF config ----

PF_POST_RACE_URL = "https://api.puntingform.com.au/v2/ireel/post-race"
PF_POST_RACE_API_KEY = "c867b2f9-d740-4cce-b772-801708c8191d"

SKYNET_PRICES_URL = "https://puntx.puntingform.com.au/api/skynet/getskynetprices"
SKYNET_API_KEY = "1eb003d7-00a7-4233-944c-88e6c7fbf246"

PF_TIMEOUT = httpx.Timeout(
    timeout=60.0,   # overall
    connect=10.0,   # connection phase
    read=50.0,      # waiting for body
    write=10.0,     # sending body (we barely use this)
)

def _pf_get_json(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Wrapper around httpx GET for PuntingForm that:
      - uses a generous timeout
      - catches ReadTimeout and logs instead of blowing up the whole run
    """
    try:
        with httpx.Client(timeout=PF_TIMEOUT) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.ReadTimeout:
        print(f"[PF] Read timeout for URL={url} params={params}")
        return None
    except Exception as e:
        print(f"[PF] Error fetching {url} params={params}: {e}")
        return None

def _pf_post_json(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        with httpx.Client(timeout=PF_TIMEOUT) as client:
            resp = client.post(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.ReadTimeout:
        print(f"[PF] Read timeout (POST) for URL={url} params={params}")
        return None
    except Exception as e:
        print(f"[PF] Error (POST) fetching {url} params={params}: {e}")
        return None

# -----------------------
# Helpers
# -----------------------

def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        return int(s)
    except (ValueError, TypeError):
        return None


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        return Decimal(s)
    except (ValueError, TypeError, ArithmeticError):
        return None


def _classify_outcome(pos_fin: Optional[int]) -> str:
    """
    Very simple WIN/PLACE/LOSE classifier based on finishing position.
    """
    if pos_fin is None or pos_fin <= 0:
        return "UNKNOWN"
    if pos_fin == 1:
        return "WIN"
    if pos_fin in (2, 3):
        return "PLACE"
    return "LOSE"


def _extract_post_race_rows(data: Any) -> List[Dict[str, Any]]:
    """
    Legacy helper – keeps the older parsing logic around.

    Punting Form's post-race endpoint can vary slightly in shape.
    We try to be tolerant here:

    - { statusCode, payLoad: [ {...runner...}, ... ] }
    - { statusCode, payLoad: { runners: [ {...}, ...], ... } }
    - [ {...}, {...} ]
    """
    if data is None:
        return []

    # Common wrapper: {"statusCode": 200, ..., "payLoad": ...}
    if isinstance(data, dict) and "payLoad" in data:
        payload = data["payLoad"]
    else:
        payload = data

    # payload might be a list of runners directly
    if isinstance(payload, list):
        return payload

    # Or a dict with "runners"
    if isinstance(payload, dict):
        runners = payload.get("runners") or payload.get("Runners")
        if isinstance(runners, list):
            return runners

    # Fallback: nothing we recognise
    return []


def _extract_pf_runners_from_payload(data: Any) -> List[Dict[str, Any]]:
    """
    Normalise PF /v2/ireel/post-race payload into a list of plain runner dicts
    that always have runnerName / tabNo / posFin on the top level.

    Handles both:
      • payLoad["runners"] = [{ runnerName, tabNo, posFin, ... }, ...]
      • payLoad["runners"] = [
            {
              rating={ runnerName, tabNo, posFin, ... },
              sectional={...},
              benchmark={...},
              jockey={...}
            }, ...
        ]
    """
    # First use the legacy extractor to pull out whatever looks like runners
    base_rows = _extract_post_race_rows(data)
    if not isinstance(base_rows, list) or not base_rows:
        return []

    first = base_rows[0]

    # Newer "ireel" shape: each entry has rating/sectional/benchmark/jockey
    if isinstance(first, dict) and "rating" in first and isinstance(first["rating"], dict):
        flattened: List[Dict[str, Any]] = []
        for r in base_rows:
            rating = r.get("rating") or {}
            if not isinstance(rating, dict):
                continue

            # Start with rating block (contains runnerName, tabNo, posFin, etc.)
            merged: Dict[str, Any] = dict(rating)

            # If some keys happen to be on the outer dict, copy them in
            for key in ("runnerName", "tabNo", "posFin", "meetingDate", "track", "status"):
                if key not in merged and key in r:
                    merged[key] = r[key]

            flattened.append(merged)
        return flattened

    # Older flat shape: already runner dicts with runnerName/tabNo/posFin
    return base_rows


def _parse_runner_tab_no(r: Dict[str, Any]) -> Optional[int]:
    """
    Extract TAB number from a PF runner row.

    Handles:
      • runner["tabNo"]
      • runner["tabNumber"], runner["tab_no"] as fallbacks
    """
    raw_tab = (
        r.get("tabNo")
        or r.get("tabNumber")
        or r.get("tab_no")
    )
    return _to_int(raw_tab)


def _parse_runner_result(r: Dict[str, Any]) -> Tuple[Optional[int], str]:
    """
    Extract (finish_position, status) from a PF runner row.

    • posFin → numeric finishing position (1,2,3,...) or None
    • status / posFinText → 'FIN', 'SCR', etc. Blank = 'FIN' if we have a position.
    """
    raw_pos = (
        r.get("posFin")
        or r.get("finishPos")
        or r.get("pos_fin")
    )
    raw_status = (r.get("status") or r.get("posFinText") or "").strip()

    pos_fin = _to_int(raw_pos)

    if not raw_status:
        raw_status = "FIN" if pos_fin is not None else ""

    return pos_fin, raw_status


def _fetch_pf_post_race(meeting_id: int, race_number: int) -> List[Dict[str, Any]]:
    """
    Call PF /v2/ireel/post-race for a single meeting/race and
    normalise into a list of flat runner dicts.

    We merge data from rating/sectional/benchmark/jockey blocks
    and make sure runnerName / tabNo / posFin / margFin etc are
    present at the top level.
    """
    params = {
        "meetingId": meeting_id,
        "raceNumber": race_number,
        "apiKey": PF_POST_RACE_API_KEY,
    }

    raw = _pf_get_json(PF_POST_RACE_URL, params)
    if raw is None:
        # Already logged in _pf_get_json
        print(
            f"[PF]  post-race: no data for meetingId={meeting_id}, "
            f"raceNumber={race_number} (timeout or HTTP error)"
        )
        return []

    # Unwrap the common {"statusCode": ..., "payLoad": ...} shape
    if isinstance(raw, dict):
        payload = raw.get("payLoad") or raw.get("payload") or raw
    else:
        payload = raw

    # Extract the raw runners list
    if isinstance(payload, dict):
        raw_runners = (
            payload.get("runners")
            or payload.get("Runners")
            or payload.get("runner")
            or []
        )
    else:
        raw_runners = payload

    if not isinstance(raw_runners, list):
        top_keys = list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__
        print(
            f"[PF]  post-race returned non-list runners for "
            f"meetingId={meeting_id}, raceNumber={race_number}, "
            f"top_keys={top_keys}"
        )
        return []

    if not raw_runners:
        print(
            f"[PF]  post-race meetingId={meeting_id}, raceNumber={race_number} "
            f"→ 0 runners"
        )
        return []

    normalised: List[Dict[str, Any]] = []

    for r in raw_runners:
        if not isinstance(r, dict):
            continue

        rating = r.get("rating")
        sectional = r.get("sectional")
        benchmark = r.get("benchmark")
        jockey = r.get("jockey")

        blocks = (rating, sectional, benchmark, jockey)

        # Newer PF "ireel" shape – merge rating/sectional/benchmark/jockey
        if any(isinstance(block, dict) for block in blocks):
            merged: Dict[str, Any] = {}

            # Merge all blocks; if the same key appears multiple times,
            # keep the first non-null value.
            for block in blocks:
                if isinstance(block, dict):
                    for k, v in block.items():
                        if v is not None and k not in merged:
                            merged[k] = v

            # Copy useful top-level fields if they exist
            for k in (
                "meetingDate",
                "track",
                "meetingId",
                "raceId",
                "runnerId",
                "raceNo",
                "runnerName",
                "tabNo",
                "posFin",    # finishing position
                "margFin",   # margin
                "status",
            ):
                v = r.get(k)
                if v is not None and k not in merged:
                    merged[k] = v

            normalised.append(merged)
        else:
            # Older flat shape: already a simple runner dict
            normalised.append(r)

    if normalised:
        first = normalised[0]
        if isinstance(first, dict):
            print(
                f"[PF]  post-race meetingId={meeting_id}, raceNumber={race_number} "
                f"→ {len(normalised)} runners"
            )
            print(
                f"[PF]   sample runner keys for meetingId={meeting_id}, "
                f"raceNumber={race_number}: {list(first.keys())}"
            )
    else:
        top_keys = list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__
        print(
            f"[PF]  post-race normalisation produced no runners for "
            f"meetingId={meeting_id}, raceNumber={race_number}, "
            f"top_keys={top_keys}"
        )

    return normalised

def _fetch_skynet_prices_for_date(target_date: date_type) -> Dict[Tuple[int, int, int], Decimal]:
    """
    Fetch Skynet prices for the whole date and build a lookup:

        (meetingId, raceNumber, tabNumber) → tabCurrentPrice (Decimal)
    """
    # PF expects something like "18-nov-2025"
    meeting_date_param = target_date.strftime("%d-%b-%Y").lower()

    params = {
        "meetingDate": meeting_date_param,
        "apikey": SKYNET_API_KEY,
    }

    data = _pf_get_json(SKYNET_PRICES_URL, params)
    price_map: Dict[Tuple[int, int, int], Decimal] = {}

    if data is None:
        print(
            f"[PF] Skynet prices: no data for {target_date} "
            f"(timeout or HTTP error)"
        )
        return price_map

    # Data is usually a list of rows; sometimes wrapped in {"data": [...]}
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        rows = data["data"]
    else:
        rows = data

    if not isinstance(rows, list):
        print(
            f"[PF] Skynet prices unexpected shape for {target_date}: "
            f"{type(rows).__name__}"
        )
        return price_map

    count = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            meeting_id = _to_int(row.get("meetingId"))
            race_no = _to_int(row.get("raceNumber"))
            tab_no = _to_int(row.get("tabNumber"))
            sp = _to_decimal(row.get("tabCurrentPrice"))
        except Exception:
            continue

        if not (meeting_id and race_no and tab_no and sp is not None):
            continue

        price_map[(meeting_id, race_no, tab_no)] = sp
        count += 1

    print(
        f"[PF] Skynet prices: built map for {count} runner prices on {target_date}"
    )
    return price_map

def _attach_tip_outcomes_from_existing_results_for_date(
    target_date: date_type,
    db: Session,
) -> int:
    """
    For the given date, ensure every Tip has a TipOutcome row when we
    already have matching RaceResult rows in the DB.

    This is especially useful for meetings where we only have RA
    results (provider='RA') or other providers, but no PF post-race yet.

    Logic:
      * Look at all Tip rows for meetings on this date.
      * If a Tip already has an outcome, leave it alone.
      * Otherwise, try to find a RaceResult for the same race/tab,
        preferring PF, then RA, then any other provider.
    """
    tips: List[models.Tip] = (
        db.query(models.Tip)
        .join(models.Race, models.Tip.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .filter(models.Meeting.date == target_date)
        .all()
    )

    if not tips:
        return 0

    tip_ids = [t.id for t in tips]

    # Existing outcomes → set of tip_ids we should skip
    existing_outcome_rows = (
        db.query(models.TipOutcome.tip_id)
        .filter(models.TipOutcome.tip_id.in_(tip_ids))
        .all()
    )
    existing_tip_ids = {row[0] for row in existing_outcome_rows}

    # Build an index of RaceResult rows for all races on this date:
    #   (race_id, tab_number) -> best RaceResult (PF > RA > other)
    race_ids = {t.race_id for t in tips}
    rr_index: Dict[Tuple[int, int], models.RaceResult] = {}

    if race_ids:
        rr_rows = (
            db.query(models.RaceResult)
            .filter(models.RaceResult.race_id.in_(race_ids))
            .all()
        )
        for rr in rr_rows:
            key = (rr.race_id, rr.tab_number)
            existing = rr_index.get(key)
            if existing is None:
                rr_index[key] = rr
            else:
                # Prefer PF, then RA, then whatever we had before
                existing_provider = (existing.provider or "").upper()
                new_provider = (rr.provider or "").upper()

                if existing_provider == new_provider:
                    continue
                if existing_provider == "PF":
                    continue
                if existing_provider == "RA" and new_provider != "PF":
                    continue

                rr_index[key] = rr

    count = 0

    for tip in tips:
        if tip.id in existing_tip_ids:
            continue

        rr = rr_index.get((tip.race_id, tip.tab_number))
        if rr is None:
            continue

        kwargs: Dict[str, Any] = {
            "tip_id": tip.id,
            "race_result_id": rr.id,
        }
        if hasattr(models.TipOutcome, "provider"):
            kwargs["provider"] = rr.provider or "PF"

        outcome = models.TipOutcome(**kwargs)
        outcome.finish_position = rr.finish_position
        outcome.starting_price = rr.starting_price
        outcome.outcome_status = _classify_outcome(rr.finish_position)

        db.add(outcome)
        count += 1

    if count:
        print(
            f"[PF] pre-attached {count} TipOutcome rows from existing RaceResult rows "
            f"for {target_date}"
        )

    return count

# -----------------------
# Main import function
# -----------------------

def import_pf_results_for_date(target_date: date_type, db: Session) -> int:
    """
    For a given date:

      1) Attach TipOutcome rows to any existing RaceResult rows (PF, RA, etc.)
         where the Tip has no outcome yet.
      2) For each Meeting with pf_meeting_id:
         - For each Race:
           * Call PF post-race and upsert RaceResult rows (provider='PF')
           * Attach / update TipOutcome rows (placing + WIN/PLACE/LOSE + SP)
      3) Enrich with Skynet SP prices for that date.

    Returns number of RaceResult rows (provider='PF') now in the DB
    for that date (not just newly inserted, but total PF rows).
    """
    # Pre-fetch Skynet SP map for the entire date
    skynet_price_map = _fetch_skynet_prices_for_date(target_date)

    # Step 0: make sure any existing results (e.g. RA) are wired to tips
    total_outcomes_upserted = _attach_tip_outcomes_from_existing_results_for_date(
        target_date, db
    )

    meetings = (
        db.query(models.Meeting)
        .filter(
            models.Meeting.date == target_date,
            models.Meeting.pf_meeting_id.isnot(None),
        )
        .all()
    )

    if not meetings:
        print(f"[PF] No Meetings with pf_meeting_id for {target_date}")

    for meeting in meetings:
        pf_meeting_id = meeting.pf_meeting_id
        if not pf_meeting_id:
            continue

        print(
            f"[PF] Importing post-race results for Meeting "
            f"pf_meeting_id={pf_meeting_id} "
            f"({meeting.track_name} {meeting.state} {meeting.date})"
        )

        for race in sorted(meeting.races, key=lambda r: r.race_number):
            race_no = race.race_number

            # Fetch runners from PF
            try:
                runners = _fetch_pf_post_race(pf_meeting_id, race_no)
            except httpx.HTTPError as e:
                print(
                    f"[PF]  HTTP error for meetingId={pf_meeting_id}, "
                    f"raceNumber={race_no}: {e}"
                )
                continue

            if not runners:
                continue

            for idx, runner in enumerate(runners):
                # Extract TAB number + result in a tolerant way
                tab_no = _parse_runner_tab_no(runner)
                pos_fin, status = _parse_runner_result(runner)

                runner_name = (
                    runner.get("runnerName")
                    or runner.get("horseName")
                    or runner.get("horse")
                )

                if tab_no is None:
                    raw_tab_no = (
                        runner.get("tabNo")
                        or runner.get("tabNumber")
                        or runner.get("tab_no")
                    )
                    print(
                        f"[PF]   skipping runner without tab_no "
                        f"(meetingId={pf_meeting_id}, race={race_no}, "
                        f"idx={idx}, raw_tab_no={raw_tab_no!r})"
                    )
                    continue

                # Skynet SP for this runner, if any
                sp = skynet_price_map.get(
                    (pf_meeting_id, race_no, tab_no)
                )

                # --- Upsert RaceResult (provider='PF') ---
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
                        tab_number=tab_no,
                        provider="PF",
                    )
                    db.add(rr)

                # Update fields
                if runner_name:
                    rr.horse_name = str(runner_name)
                rr.finish_position = pos_fin
                rr.starting_price = sp

                if status:
                    rr.status = str(status)

                    margin = (
                        runner.get("margFin")      # PF margin field
                        or runner.get("margin")
                        or runner.get("margin_text")
                        or runner.get("marginText")
                    )
                    if margin is not None:
                        rr.margin_text = str(margin)

                # --- Tie to any tips on this race / TAB ---
                tips = (
                    db.query(models.Tip)
                    .filter(
                        models.Tip.race_id == race.id,
                        models.Tip.tab_number == tab_no,
                    )
                    .all()
                )

                for tip in tips:
                    # IMPORTANT:
                    # tip_outcomes.tip_id is UNIQUE, so we must have
                    # at most one outcome row per tip.
                    # Look up by tip_id ONLY and update it, instead of
                    # creating multiple rows per tip.
                    outcome = (
                        db.query(models.TipOutcome)
                        .filter(models.TipOutcome.tip_id == tip.id)
                        .one_or_none()
                    )

                    if outcome is None:
                        # First time we are creating an outcome for this tip
                        kwargs: Dict[str, Any] = {
                            "tip_id": tip.id,
                            "race_result_id": rr.id,
                        }
                        if hasattr(models.TipOutcome, "provider"):
                            kwargs["provider"] = "PF"
                        outcome = models.TipOutcome(**kwargs)
                        db.add(outcome)
                    else:
                        # Re-run / switching provider: keep the same row,
                        # just point it at the current PF RaceResult.
                        outcome.race_result_id = rr.id
                        if hasattr(models.TipOutcome, "provider"):
                            outcome.provider = "PF"

                    # Update the outcome details
                    outcome.finish_position = pos_fin
                    outcome.starting_price = sp
                    outcome.outcome_status = _classify_outcome(pos_fin)
                    total_outcomes_upserted += 1

    # Commit once at the end (covers both RA-based and PF-based updates)
    db.commit()

    # At the end, count how many PF RaceResult rows we *now* have for that date
    total_rr = (
        db.query(models.RaceResult)
        .join(models.Race)
        .join(models.Meeting)
        .filter(
            models.Meeting.date == target_date,
            models.RaceResult.provider == "PF",
        )
        .count()
    )

    print(
        f"[PF] upserted TipOutcomes={total_outcomes_upserted} "
        f"and now have {total_rr} PF RaceResult rows for {target_date}"
    )

    return total_rr
