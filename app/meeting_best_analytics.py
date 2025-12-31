# app/meeting_best_analytics.py
"""
Meeting Best Analytics: Track performance of consensus tips.

A "Meeting Best" is when our AI_BEST tip matches Skynet's #1 ranked horse.
These consensus picks are tracked against actual race results.

Data sources:
- Tips: Local database (AI_BEST tips)
- Skynet: PuntingForm API (rank=1 horses)
- Results: RA Crawler API
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy.orm import Session

from . import models


# ============================================================================
# CACHING
# ============================================================================
_CACHE_TTL_SECONDS = 300  # 5 minutes

_skynet_cache: Dict[date, Tuple[float, Dict]] = {}
_results_cache: Dict[date, Tuple[float, Dict]] = {}


def _is_cache_valid(timestamp: float) -> bool:
    return (time.time() - timestamp) < _CACHE_TTL_SECONDS


def clear_meeting_best_cache():
    global _skynet_cache, _results_cache
    _skynet_cache.clear()
    _results_cache.clear()
    print("[MEETING_BEST] Cache cleared")


# ============================================================================
# DATA CLASSES
# ============================================================================
@dataclass
class MeetingBestTip:
    """A consensus tip where AI_BEST matches Skynet #1"""
    meeting_date: date
    state: str
    track_name: str
    race_number: int
    tab_number: int
    horse_name: str
    skynet_price: Optional[float]
    # Result data (filled after matching)
    finish_pos: Optional[int] = None
    is_scratched: bool = False
    starting_price: Optional[float] = None


@dataclass
class MeetingBestBucket:
    """Accumulator for Meeting Best statistics"""
    label: str
    tips: int = 0
    wins: int = 0
    seconds: int = 0
    thirds: int = 0

    @property
    def win_rate(self) -> float:
        return (self.wins / self.tips * 100) if self.tips > 0 else 0.0

    @property
    def place_rate(self) -> float:
        places = self.wins + self.seconds + self.thirds
        return (places / self.tips * 100) if self.tips > 0 else 0.0

    @property
    def podium(self) -> int:
        return self.wins + self.seconds + self.thirds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "tips": self.tips,
            "wins": self.wins,
            "seconds": self.seconds,
            "thirds": self.thirds,
            "podium": self.podium,
            "win_rate": round(self.win_rate, 1),
            "place_rate": round(self.place_rate, 1),
        }


# ============================================================================
# DATA FETCHING
# ============================================================================
def _format_skynet_date(d: date) -> str:
    """Convert date to Skynet API format: '22-dec-2025'"""
    return d.strftime("%d-%b-%Y").lower()


def _fetch_ai_best_from_db(db: Session, d: date) -> Dict[Tuple[str, int, int], Dict[str, Any]]:
    """
    Fetch AI_BEST tips from local database.
    Returns dict keyed by (state, race_number, tab_number).
    """
    tips = {}
    meetings = db.query(models.Meeting).filter(models.Meeting.date == d).all()

    for meeting in meetings:
        state = (meeting.state or "").upper()
        track_name = meeting.track_name or ""
        pf_meeting_id = meeting.pf_meeting_id

        for race in meeting.races:
            race_number = race.race_number

            for tip in race.tips:
                if tip.tip_type != "AI_BEST":
                    continue

                key = (state, race_number, tip.tab_number)
                tips[key] = {
                    "state": state,
                    "track_name": track_name,
                    "race_number": race_number,
                    "tab_number": tip.tab_number,
                    "horse_name": tip.horse_name or "",
                    "pf_meeting_id": pf_meeting_id,
                }

    return tips


def _fetch_skynet_rank1(d: date, use_cache: bool = True) -> Dict[Tuple[str, int, int], Dict[str, Any]]:
    """
    Fetch Skynet #1 ranked horses.
    Returns dict keyed by (state, race_number, tab_number).

    Note: Skynet doesn't have state directly, so we match by track name.
    """
    global _skynet_cache

    if use_cache and d in _skynet_cache:
        timestamp, cached = _skynet_cache[d]
        if _is_cache_valid(timestamp):
            return cached

    api_key = os.getenv("SKYNET_API_KEY", "1eb003d7-00a7-4233-944c-88e6c7fbf246")
    date_str = _format_skynet_date(d)
    url = f"https://puntx.puntingform.com.au/api/skynet/getskynetprices"

    results = {}
    max_retries = 2  # Fewer retries to avoid long hangs

    print(f"[MEETING_BEST] Fetching Skynet for {d} ({date_str})")

    for attempt in range(max_retries):
        try:
            with httpx.Client(timeout=15.0) as client:  # Shorter timeout
                resp = client.get(url, params={
                    "meetingDate": date_str,
                    "apikey": api_key,
                })
                resp.raise_for_status()
                data = resp.json()

            # Track name to state mapping (build from data)
            track_to_state = _build_track_state_map()

            for item in data:
                rank = item.get("rank")
                if rank != 1:
                    continue

                track = item.get("track", "")
                race_no = item.get("raceNumber")
                tab_number = item.get("tabNumber")

                if not track or race_no is None or tab_number is None:
                    continue

                # Map track to state
                state = track_to_state.get(track.lower(), "")
                if not state:
                    # Try to infer from track name patterns
                    state = _infer_state_from_track(track)

                key = (state, int(race_no), int(tab_number))
                results[key] = {
                    "track": track,
                    "race_number": int(race_no),
                    "tab_number": int(tab_number),
                    "horse_name": item.get("horse", ""),
                    "price": item.get("price"),
                    "state": state,
                }

            _skynet_cache[d] = (time.time(), results)
            return results

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[MEETING_BEST] Retry {attempt + 1}/{max_retries} for Skynet {d}: {e}")
                time.sleep(1)  # Short sleep between retries
            else:
                print(f"[MEETING_BEST] Failed fetching Skynet for {d}: {e}")

    # Cache empty results to avoid re-fetching failed dates
    _skynet_cache[d] = (time.time(), results)
    return results


def _build_track_state_map() -> Dict[str, str]:
    """Map track names to states."""
    return {
        # NSW
        "warwick farm": "NSW", "randwick": "NSW", "royal randwick": "NSW",
        "rosehill": "NSW", "rosehill gardens": "NSW", "canterbury": "NSW",
        "newcastle": "NSW", "kembla grange": "NSW", "hawkesbury": "NSW",
        "gosford": "NSW", "wyong": "NSW", "scone": "NSW", "tamworth": "NSW",
        "dubbo": "NSW", "mudgee": "NSW", "bathurst": "NSW", "goulburn": "NSW",
        "wagga": "NSW", "albury": "NSW", "canberra": "NSW", "queanbeyan": "NSW",
        "moruya": "NSW", "nowra": "NSW", "port macquarie": "NSW",
        "taree": "NSW", "grafton": "NSW", "lismore": "NSW", "ballina": "NSW",
        "coffs harbour": "NSW", "muswellbrook": "NSW",
        # VIC
        "flemington": "VIC", "caulfield": "VIC", "moonee valley": "VIC",
        "sandown": "VIC", "sandown lakeside": "VIC", "sandown hillside": "VIC",
        "cranbourne": "VIC", "pakenham": "VIC", "mornington": "VIC",
        "geelong": "VIC", "ballarat": "VIC", "bendigo": "VIC", "kilmore": "VIC",
        "kyneton": "VIC", "seymour": "VIC", "wangaratta": "VIC", "wodonga": "VIC",
        "echuca": "VIC", "swan hill": "VIC", "mildura": "VIC", "horsham": "VIC",
        "hamilton": "VIC", "warrnambool": "VIC", "stony creek": "VIC",
        "sale": "VIC", "traralgon": "VIC", "bairnsdale": "VIC",
        # QLD
        "doomben": "QLD", "eagle farm": "QLD", "sunshine coast": "QLD",
        "gold coast": "QLD", "ipswich": "QLD", "toowoomba": "QLD",
        "rockhampton": "QLD", "mackay": "QLD", "townsville": "QLD",
        "cairns": "QLD", "beaudesert": "QLD",
        # SA
        "morphettville": "SA", "morphettville parks": "SA", "murray bridge": "SA",
        "gawler": "SA", "balaklava": "SA", "strathalbyn": "SA", "oakbank": "SA",
        "mount gambier": "SA", "port lincoln": "SA",
        # WA
        "ascot": "WA", "belmont": "WA", "pinjarra": "WA", "bunbury": "WA",
        "northam": "WA", "geraldton": "WA", "kalgoorlie": "WA", "albany": "WA",
        # TAS
        "hobart": "TAS", "launceston": "TAS", "devonport": "TAS",
        # NT
        "darwin": "NT", "alice springs": "NT",
        # ACT
        "canberra": "ACT",
    }


def _infer_state_from_track(track: str) -> str:
    """Try to infer state from track name if not in map."""
    track_lower = track.lower()
    track_map = _build_track_state_map()

    # Direct match
    if track_lower in track_map:
        return track_map[track_lower]

    # Partial match
    for known_track, state in track_map.items():
        if known_track in track_lower or track_lower in known_track:
            return state

    return ""


def _fetch_results_for_date(d: date, use_cache: bool = True) -> Dict[Tuple[str, int, int], Dict[str, Any]]:
    """Fetch results keyed by (state, race_no, horse_number)."""
    global _results_cache

    if use_cache and d in _results_cache:
        timestamp, cached = _results_cache[d]
        if _is_cache_valid(timestamp):
            return cached

    base_url = os.getenv("RA_CRAWLER_BASE_URL", "https://ra-crawler.onrender.com")
    url = f"{base_url.rstrip('/')}/results"

    results = {}
    max_retries = 2

    for attempt in range(max_retries):
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(url, params={"date": d.isoformat()})
                resp.raise_for_status()
                data = resp.json()

            for item in data:
                state = (item.get("state") or "").upper()
                race_no = item.get("race_no")
                horse_number = item.get("horse_number")

                if not state or race_no is None or horse_number is None:
                    continue

                key = (state, int(race_no), int(horse_number))
                results[key] = {
                    "finishing_pos": item.get("finishing_pos"),
                    "is_scratched": bool(item.get("is_scratched")),
                    "starting_price": item.get("starting_price"),
                }

            _results_cache[d] = (time.time(), results)
            return results

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"[MEETING_BEST] Retry {attempt + 1}/{max_retries} for results {d}: {e}")
                time.sleep(2 ** attempt)
            else:
                print(f"[MEETING_BEST] Failed fetching results for {d}: {e}")

    return results


# ============================================================================
# MAIN COMPUTATION
# ============================================================================
def compute_meeting_best_trends(
    db: Session,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Compute Meeting Best performance trends.

    A Meeting Best is where AI_BEST tab_number matches Skynet rank=1 tab_number
    for the same race.
    """
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=30)  # 30 days default for faster load

    print(f"[MEETING_BEST] Computing trends from {date_from} to {date_to}")

    # Buckets for analysis
    by_state: Dict[str, MeetingBestBucket] = {}
    by_price: Dict[str, MeetingBestBucket] = {}
    by_track_type: Dict[str, MeetingBestBucket] = {}

    total_meeting_best = 0
    total_ai_best = 0
    matched_with_results = 0
    scratched = 0
    no_result = 0

    # Sample matches for display
    sample_matches: List[Dict[str, Any]] = []

    day = date_from
    while day <= date_to:
        # Fetch AI_BEST tips from database first (fast)
        ai_best_tips = _fetch_ai_best_from_db(db, day)

        # Skip days with no tips - no need to call external APIs
        if not ai_best_tips:
            day += timedelta(days=1)
            continue

        # Only fetch external APIs if we have tips
        skynet_rank1 = _fetch_skynet_rank1(day)
        results = _fetch_results_for_date(day)

        total_ai_best += len(ai_best_tips)

        # Find Meeting Best matches
        for key, tip in ai_best_tips.items():
            state, race_number, tab_number = key

            # Check if Skynet has same tab_number as #1 for this race
            # Try to match by state+race+tab
            skynet_match = skynet_rank1.get(key)

            if skynet_match is None:
                # Try matching just by race_number and tab_number across all states
                # (Skynet might not have state info)
                for sk_key, sk_val in skynet_rank1.items():
                    sk_state, sk_race, sk_tab = sk_key
                    if sk_race == race_number and sk_tab == tab_number:
                        # Check if track names match
                        if _tracks_match(tip.get("track_name", ""), sk_val.get("track", "")):
                            skynet_match = sk_val
                            break

            if skynet_match is None:
                continue

            # Found a Meeting Best!
            total_meeting_best += 1

            # Get result
            result = results.get(key)

            if result is None:
                no_result += 1
                continue

            matched_with_results += 1

            if result.get("is_scratched"):
                scratched += 1
                continue

            finish_pos = result.get("finishing_pos")
            sp = result.get("starting_price")

            # Determine bucket keys
            state_key = state or "Unknown"
            price_key = _get_price_bucket(sp)
            track_type_key = _get_track_type(tip.get("track_name", ""))

            # Update buckets
            def update_bucket(buckets: Dict[str, MeetingBestBucket], bkey: str, pos: Optional[int]):
                if bkey not in buckets:
                    buckets[bkey] = MeetingBestBucket(label=bkey)
                bucket = buckets[bkey]
                bucket.tips += 1
                if pos == 1:
                    bucket.wins += 1
                elif pos == 2:
                    bucket.seconds += 1
                elif pos == 3:
                    bucket.thirds += 1

            update_bucket(by_state, state_key, finish_pos)
            update_bucket(by_price, price_key, finish_pos)
            update_bucket(by_track_type, track_type_key, finish_pos)

            # Collect sample matches (first 20)
            if len(sample_matches) < 20:
                sample_matches.append({
                    "date": day.isoformat(),
                    "track": tip.get("track_name", ""),
                    "state": state,
                    "race": race_number,
                    "tab": tab_number,
                    "horse": tip.get("horse_name", ""),
                    "skynet_price": skynet_match.get("price"),
                    "finish_pos": finish_pos,
                    "sp": sp,
                    "is_winner": finish_pos == 1,
                })

        if total_meeting_best > 0 or len(ai_best_tips) > 0:
            print(f"[MEETING_BEST] {day}: {len(ai_best_tips)} AI_BEST, {len(skynet_rank1)} Skynet #1s")

        day += timedelta(days=1)

    # Calculate overall stats
    processed = sum(b.tips for b in by_state.values())
    wins = sum(b.wins for b in by_state.values())
    seconds = sum(b.seconds for b in by_state.values())
    thirds = sum(b.thirds for b in by_state.values())
    podium = wins + seconds + thirds

    print(f"[MEETING_BEST] Total: {total_meeting_best} Meeting Best found from {total_ai_best} AI_BEST tips")
    print(f"[MEETING_BEST] Processed: {processed} (scratched: {scratched}, no_result: {no_result})")
    print(f"[MEETING_BEST] Results: {wins}W / {podium}P from {processed} tips")

    def to_sorted_list(buckets: Dict[str, MeetingBestBucket]) -> List[Dict]:
        items = [b for b in buckets.values() if b.tips >= 3]
        items.sort(key=lambda x: x.win_rate, reverse=True)
        return [b.to_dict() for b in items]

    return {
        "has_data": processed > 0,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),

        "summary": {
            "total_ai_best": total_ai_best,
            "meeting_best_found": total_meeting_best,
            "match_rate": round(total_meeting_best / total_ai_best * 100, 1) if total_ai_best > 0 else 0,
            "processed": processed,
            "scratched": scratched,
            "no_result": no_result,
        },

        "overall": {
            "tips": processed,
            "wins": wins,
            "seconds": seconds,
            "thirds": thirds,
            "podium": podium,
            "win_rate": round(wins / processed * 100, 1) if processed > 0 else 0,
            "place_rate": round(podium / processed * 100, 1) if processed > 0 else 0,
        },

        "by_state": to_sorted_list(by_state),
        "by_price": to_sorted_list(by_price),
        "by_track_type": to_sorted_list(by_track_type),

        "sample_matches": sample_matches,
    }


def _tracks_match(track1: str, track2: str) -> bool:
    """Check if two track names refer to the same track."""
    t1 = track1.lower().strip()
    t2 = track2.lower().strip()

    if t1 == t2:
        return True

    # Handle common variations
    if t1 in t2 or t2 in t1:
        return True

    # Handle "Rosehill" vs "Rosehill Gardens"
    variations = [
        ("rosehill", "rosehill gardens"),
        ("sandown", "sandown lakeside"),
        ("sandown", "sandown hillside"),
        ("morphettville", "morphettville parks"),
        ("randwick", "royal randwick"),
    ]

    for v1, v2 in variations:
        if (v1 in t1 and v2 in t2) or (v2 in t1 and v1 in t2):
            return True
        if (t1 == v1 and v2 in t2) or (t2 == v1 and v2 in t1):
            return True

    return False


def _get_price_bucket(sp: Optional[float]) -> str:
    """Categorize starting price into buckets"""
    if sp is None:
        return "Unknown"
    if sp < 2.0:
        return "$1.01-$1.99"
    elif sp < 3.0:
        return "$2.00-$2.99"
    elif sp < 4.0:
        return "$3.00-$3.99"
    elif sp < 6.0:
        return "$4.00-$5.99"
    elif sp < 10.0:
        return "$6.00-$9.99"
    else:
        return "$10.00+"


def _get_track_type(track_name: str) -> str:
    """Determine track type."""
    metro_tracks = {
        "flemington", "caulfield", "moonee valley", "sandown",
        "randwick", "royal randwick", "rosehill", "warwick farm", "canterbury",
        "doomben", "eagle farm",
        "morphettville", "morphettville parks",
        "ascot", "belmont",
    }
    track_lower = track_name.lower()
    for metro in metro_tracks:
        if metro in track_lower:
            return "Metro"
    return "Provincial/Country"
