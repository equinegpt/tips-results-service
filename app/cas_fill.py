# app/cas_fill.py
#
# Fills the Canned Answers Service (CAS) with synthesised Top3 picks
# for races that don't have a Gemini-generated tip. Picks come from
# the Skynet model rankings (the same backend powering SkyNet "crowns").
#
# Frontend never sees the source — picks are surfaced as ordinary
# AI_BEST / DANGER / VALUE tips with neutral reasoning.

from __future__ import annotations

import os
from datetime import date as date_type
from typing import Any, Dict, List, Optional, Tuple

import httpx

from .pf_results import _pf_get_json, _to_int


SKYNET_PRICES_URL = os.getenv(
    "SKYNET_PRICES_URL",
    "https://puntx.puntingform.com.au/api/skynet/getskynetprices",
)
SKYNET_API_KEY = os.getenv("SKYNET_API_KEY", "1eb003d7-00a7-4233-944c-88e6c7fbf246")

CAS_BASE_URL = os.getenv("CAS_BASE_URL", "https://canned-answers-service.onrender.com").rstrip("/")


# Neutral reasoning text. Deliberately generic so nothing about the upstream
# data source leaks into the app's UI.
_REASON_AI_BEST = (
    "Strongest combination of recent form, class and race setup. Looks the "
    "horse to beat."
)
_REASON_DANGER = (
    "Genuine threat to the top pick — consistent form line and well placed "
    "in today's conditions."
)
_REASON_VALUE = (
    "Worth keeping safe at the odds. Form profile supports an each-way chance."
)


def _horse_name_from_row(row: Dict[str, Any]) -> Optional[str]:
    """Try multiple possible field names for the runner name."""
    for key in ("horse", "horseName", "runner", "runnerName", "name"):
        v = row.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def fetch_skynet_picks_for_date(target_date: date_type) -> Dict[Tuple[int, int], List[Dict[str, Any]]]:
    """
    Fetch Skynet rows for a date, grouped by (meeting_id, race_number) and
    sorted by rank ascending.

    Returns:
        { (meeting_id, race_number): [
            {"tab": int, "horse": str, "rank": int},
            ...sorted by rank...
          ] }
    """
    meeting_date_param = target_date.strftime("%d-%b-%Y").lower()
    params = {"meetingDate": meeting_date_param, "apikey": SKYNET_API_KEY}

    data = _pf_get_json(SKYNET_PRICES_URL, params)
    out: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}

    if data is None:
        print(f"[CAS-FILL] No upstream data for {target_date}")
        return out

    if isinstance(data, dict) and isinstance(data.get("data"), list):
        rows = data["data"]
    else:
        rows = data

    if not isinstance(rows, list):
        return out

    for row in rows:
        if not isinstance(row, dict):
            continue

        meeting_id = _to_int(row.get("meetingId"))
        race_no = _to_int(row.get("raceNumber"))
        tab_no = _to_int(row.get("tabNumber"))
        rank = _to_int(row.get("rank"))
        horse = _horse_name_from_row(row)

        if not (meeting_id and race_no and tab_no and rank and horse):
            continue

        key = (meeting_id, race_no)
        out.setdefault(key, []).append({"tab": tab_no, "horse": horse, "rank": rank})

    # Sort each race's runners by rank ascending
    for key in out:
        out[key].sort(key=lambda r: r["rank"])

    print(f"[CAS-FILL] Fetched {sum(len(v) for v in out.values())} runners "
          f"across {len(out)} races on {target_date}")
    return out


def _build_top3_payload(runners: List[Dict[str, Any]]) -> Optional[str]:
    """
    From a sorted (by rank) list of runners, build a Top3 payload in the
    canonical text format used by the daily Gemini cron and parsed by
    both the iOS and Flutter apps:

        AI Best: #<tab> **<horse>** — <reason>
        Danger: #<tab> **<horse>** — <reason>
        Value: #<tab> **<horse>** — <reason>
    """
    if len(runners) < 3:
        return None

    return (
        f"AI Best: #{runners[0]['tab']} **{runners[0]['horse']}** — {_REASON_AI_BEST}\n"
        f"Danger: #{runners[1]['tab']} **{runners[1]['horse']}** — {_REASON_DANGER}\n"
        f"Value: #{runners[2]['tab']} **{runners[2]['horse']}** — {_REASON_VALUE}"
    )


def fill_cas_for_date(
    target_date: date_type,
    force: bool = False,
) -> Dict[str, int]:
    """
    Fill CAS with synthesised Top3 picks for every race on `target_date`
    that doesn't already have one.

    Returns a summary dict with counters.
    """
    picks = fetch_skynet_picks_for_date(target_date)
    if not picks:
        return {"races_total": 0, "written": 0, "skipped_existing": 0, "skipped_incomplete": 0}

    iso = target_date.isoformat()
    written = 0
    skipped_existing = 0
    skipped_incomplete = 0

    with httpx.Client(timeout=httpx.Timeout(20.0, connect=10.0)) as client:
        for (meeting_id, race_number), runners in picks.items():
            payload = _build_top3_payload(runners)
            if payload is None:
                skipped_incomplete += 1
                continue

            # Step 1: check if CAS already has an entry for this race
            if not force:
                try:
                    check = client.get(
                        f"{CAS_BASE_URL}/canned",
                        params={
                            "date": iso,
                            "pf_meeting_id": meeting_id,
                            "race_number": race_number,
                            "prompt_type": "top3",
                        },
                    )
                    if check.status_code == 200:
                        skipped_existing += 1
                        continue
                except Exception as e:
                    print(f"[CAS-FILL] CAS check error for "
                          f"({meeting_id}, R{race_number}): {e}")
                    # Fall through and try to write anyway

            # Step 2: write the synthesised entry
            try:
                resp = client.post(
                    f"{CAS_BASE_URL}/canned",
                    params={"force": "true"} if force else None,
                    json={
                        "date": iso,
                        "pf_meeting_id": meeting_id,
                        "race_number": race_number,
                        "prompt_type": "top3",
                        "prompt_text": "Top 3 (fallback)",
                        "raw_response": payload,
                    },
                )
                if 200 <= resp.status_code < 300:
                    written += 1
                else:
                    print(f"[CAS-FILL] CAS write {resp.status_code} for "
                          f"({meeting_id}, R{race_number}): {resp.text[:150]}")
            except Exception as e:
                print(f"[CAS-FILL] CAS write error for "
                      f"({meeting_id}, R{race_number}): {e}")

    return {
        "races_total": len(picks),
        "written": written,
        "skipped_existing": skipped_existing,
        "skipped_incomplete": skipped_incomplete,
    }
