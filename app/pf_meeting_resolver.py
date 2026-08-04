# app/pf_meeting_resolver.py
#
# Resolves a meeting's PunctingForm meeting id from (date, track_name, state).
#
# Why this exists
# ---------------
# The apps match a service-provided tip meeting to their own selected meeting
# by `pf_meeting_id` (the iOS app keys entirely off `track.id`, which is the PF
# meetingId from /v2/form/meetingslist). When tips are published without a
# pf_meeting_id — e.g. because RA Crawler's /races now returns meetingId=null
# and SkyNet returns meetingId=0 — every meeting in the /tips payload carries
# pf_meeting_id=null and the app silently discards all tips.
#
# The authoritative (date, track, state) -> meetingId map is PF's own
# meetingslist endpoint (the same one the app uses). This module fetches and
# caches that map so we can backfill pf_meeting_id at read time (in /tips) and
# at generation time (daily_generator) without any client release.
#
# Failure is always soft: if PF is unreachable or the key is rejected, the
# resolver returns an empty map and callers keep their existing behaviour
# (pf_meeting_id stays null). This can never make things worse than they are.

from __future__ import annotations

import os
import re
import time
from datetime import date as date_type
from typing import Dict, Optional, Tuple

import httpx

# PF form API key. Defaults to the shared racing-db/PF form key so production
# works even if the env var isn't set; override with PF_API_KEY in the
# environment. (This is the /v2/form key — the SkyNet /api/skynet key is a
# different key and is rejected here.)
PF_API_KEY = os.getenv("PF_API_KEY", "c867b2f9-d740-4cce-b772-801708c8191d")
PF_MEETINGS_URL = os.getenv(
    "PF_MEETINGS_URL",
    "https://api.puntingform.com.au/v2/form/meetingslist",
)

# Cache: date -> (fetched_at_epoch, {(norm_track, state): meeting_id})
_CACHE_TTL_SECONDS = 15 * 60
_cache: Dict[date_type, Tuple[float, Dict[Tuple[str, str], int]]] = {}


def _normalize_track_name(name: Optional[str]) -> str:
    """Lowercase, strip, collapse internal whitespace. Safe on None."""
    if not name:
        return ""
    return re.sub(r"\s+", " ", name).strip().lower()


def _pf_meeting_date_param(d: date_type) -> str:
    """PF expects e.g. '4 Aug 2026' (no leading zero on day)."""
    return f"{d.day} {d.strftime('%b')} {d.year}"


def resolve_pf_meeting_ids(
    target_date: date_type,
    use_cache: bool = True,
) -> Dict[Tuple[str, str], int]:
    """
    Return { (normalized_track_name, state_upper): pf_meeting_id } for the date.

    Soft-fails to {} on any error so callers never break.
    """
    if use_cache and target_date in _cache:
        fetched_at, cached = _cache[target_date]
        if (time.time() - fetched_at) < _CACHE_TTL_SECONDS:
            return cached

    out: Dict[Tuple[str, str], int] = {}
    params = {
        "apiKey": PF_API_KEY,
        "meetingDate": _pf_meeting_date_param(target_date),
    }

    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(PF_MEETINGS_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:  # noqa: BLE001 — soft fail by design
        print(f"[PFRESOLVE] failed for {target_date}: {e}")
        # Cache the empty result briefly so we don't hammer PF on every request.
        _cache[target_date] = (time.time(), out)
        return out

    meetings = data.get("payLoad") if isinstance(data, dict) else data
    if not isinstance(meetings, list):
        _cache[target_date] = (time.time(), out)
        return out

    for m in meetings:
        if not isinstance(m, dict):
            continue
        track = m.get("track") if isinstance(m.get("track"), dict) else {}
        name = track.get("name") or m.get("venue")
        state = (track.get("state") or m.get("state") or "").strip().upper()
        raw_mid = m.get("meetingId")
        norm = _normalize_track_name(name)
        if not norm or raw_mid is None:
            continue
        try:
            mid = int(raw_mid)
        except (TypeError, ValueError):
            continue
        if mid <= 0:
            continue
        out[(norm, state)] = mid

    _cache[target_date] = (time.time(), out)
    print(f"[PFRESOLVE] resolved {len(out)} meeting id(s) for {target_date}")
    return out


def lookup_pf_meeting_id(
    resolver_map: Dict[Tuple[str, str], int],
    track_name: Optional[str],
    state: Optional[str],
) -> Optional[int]:
    """
    Look up a pf_meeting_id from a resolver_map produced by
    resolve_pf_meeting_ids(). Tries (name, state) first, then falls back to a
    name-only match (unique track name on the day) to tolerate state mismatches.
    """
    norm = _normalize_track_name(track_name)
    if not norm:
        return None

    st = (state or "").strip().upper()
    if (norm, st) in resolver_map:
        return resolver_map[(norm, st)]

    # Name-only fallback: accept only if exactly one meeting has this track name.
    name_matches = {mid for (n, _s), mid in resolver_map.items() if n == norm}
    if len(name_matches) == 1:
        return next(iter(name_matches))

    return None


def resolve_one(
    target_date: date_type,
    track_name: Optional[str],
    state: Optional[str],
) -> Optional[int]:
    """Convenience: resolve a single meeting's pf_meeting_id."""
    return lookup_pf_meeting_id(
        resolve_pf_meeting_ids(target_date),
        track_name,
        state,
    )
