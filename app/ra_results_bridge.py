from __future__ import annotations

from dataclasses import is_dataclass, replace
from datetime import date
from typing import Iterable, List, Sequence, TypeVar, Any

from .ra_results_client import RAResultsClient, RAResultRow

T = TypeVar("T")  # your tip row type


def _normalize_track(name: str | None) -> str:
    """
    Make track names comparable even if RA vs PF differ slightly.
    Strategy: strip, lowercase, collapse spaces, then take first word.

    E.g. "Rosehill Gardens" / "Rosehill Gdns" / "Rosehill" -> "rosehill"
    """
    if not name:
        return ""
    import re

    base = re.sub(r"\s+", " ", name).strip().lower()
    return base.split(" ")[0] if base else ""


def _index_ra_rows(rows: Sequence[RAResultRow]) -> dict[tuple, RAResultRow]:
    """
    Index RA results by (state, norm_track, race_no, tab_number).
    """
    index: dict[tuple, RAResultRow] = {}
    for r in rows:
        key = (r.state, _normalize_track(r.track), r.race_no, r.tab_number)
        index[key] = r
    return index


def _get_attr(obj: Any, *names: str) -> Any:
    """
    Try a sequence of attribute names and return the first non-None value.
    Gracefully handles missing attrs.
    """
    for n in names:
        if hasattr(obj, n):
            val = getattr(obj, n)
            if val is not None:
                return val
    return None


def attach_ra_results_for_day(
    day: date,
    tips: Sequence[T],
    client: RAResultsClient | None = None,
) -> List[T]:
    """
    Attach RA results to each tip row for the given day.

    Matching key: (state, norm_track, race_no, tab_number).

    We only overwrite finishing_pos / starting_price if they are
    currently None/0 so we don't break older data that may already
    have results filled.
    """
    if client is None:
        client = RAResultsClient()

    ra_rows = client.fetch_results_for_date(day)
    index = _index_ra_rows(ra_rows)

    enriched: List[T] = []

    for tip in tips:
        # If this tip already has a finish filled, leave it alone.
        existing_pos = _get_attr(tip, "finishing_pos", "finish_pos", "placing")
        if isinstance(existing_pos, int) and existing_pos > 0:
            enriched.append(tip)
            continue

        state = _get_attr(tip, "state", "meeting_state", "track_state")
        track = _get_attr(tip, "track_name", "track", "meeting_track")
        race_no = _get_attr(tip, "race_number", "race_no", "raceNo")
        tab_no = _get_attr(tip, "tab_number", "tabNo", "tab", "number")

        try:
            rn = int(race_no) if race_no is not None else None
        except Exception:
            rn = None

        try:
            tn = int(tab_no) if tab_no is not None else None
        except Exception:
            tn = None

        if not state or rn is None or tn is None:
            enriched.append(tip)
            continue

        key = (str(state), _normalize_track(str(track)), rn, tn)
        ra = index.get(key)
        if not ra:
            enriched.append(tip)
            continue

        # Now we have a matching RA result -> fill finishing_pos + starting_price
        if is_dataclass(tip):
            tip = replace(
                tip,
                finishing_pos=ra.finishing_pos,
                starting_price=ra.starting_price,
            )
        else:
            # ORM / Pydantic / plain object
            if hasattr(tip, "finishing_pos"):
                setattr(tip, "finishing_pos", ra.finishing_pos)
            if hasattr(tip, "starting_price"):
                setattr(tip, "starting_price", ra.starting_price)

        enriched.append(tip)

    return enriched
