# app/trends_analytics.py
"""
Trend analytics built fresh from two data sources:
- Tips: https://tips-results-service.onrender.com/tips?date=YYYY-MM-DD
- Results: https://ra-crawler.onrender.com/results?date=YYYY-MM-DD

Matching key: (date, state, race_number, tab_number/horse_number)
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy.orm import Session

from . import models


@dataclass
class TrendBucket:
    """Accumulator for a single trend bucket (e.g., '1200-1400m')"""
    label: str
    tips: int = 0
    wins: int = 0
    seconds: int = 0
    thirds: int = 0

    @property
    def win_strike_rate(self) -> float:
        return (self.wins / self.tips * 100) if self.tips > 0 else 0.0

    @property
    def place_strike_rate(self) -> float:
        """Top 3 finish rate"""
        places = self.wins + self.seconds + self.thirds
        return (places / self.tips * 100) if self.tips > 0 else 0.0

    @property
    def podium_total(self) -> int:
        """Total 1st + 2nd + 3rd finishes"""
        return self.wins + self.seconds + self.thirds

    def to_dict(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "tips": self.tips,
            "wins": self.wins,
            "seconds": self.seconds,
            "thirds": self.thirds,
            "podium": self.podium_total,
            "win_strike_rate": round(self.win_strike_rate, 1),
            "place_strike_rate": round(self.place_strike_rate, 1),
        }


@dataclass
class FlatTip:
    """Flattened tip with all context needed for trend analysis"""
    meeting_date: date
    state: str
    track_name: str
    race_number: int
    tab_number: int
    horse_name: str
    tip_type: str
    distance_m: Optional[int]
    class_text: Optional[str]
    race_name: Optional[str]


@dataclass
class FlatResult:
    """Flattened result from RA Crawler"""
    meeting_date: date
    state: str
    track: str
    race_no: int
    horse_number: int
    horse_name: str
    finishing_pos: Optional[int]
    is_scratched: bool
    starting_price: Optional[float]


def _fetch_tips_for_date(d: date) -> List[FlatTip]:
    """
    Fetch tips from Tips Service API for a single date.
    GET /tips?date=YYYY-MM-DD
    """
    base_url = os.getenv("TIPS_SERVICE_BASE_URL", "https://tips-results-service.onrender.com")
    url = f"{base_url.rstrip('/')}/tips"

    tips: List[FlatTip] = []

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, params={"date": d.isoformat()})
            resp.raise_for_status()
            data = resp.json()

            # Data structure: list of {meeting: {...}, races: [{race: {...}, tips: [...]}]}
            for meeting_obj in data:
                meeting = meeting_obj.get("meeting", {})
                meeting_date = d
                state = (meeting.get("state") or "").upper()
                track_name = meeting.get("track_name") or ""

                for race_obj in meeting_obj.get("races", []):
                    race = race_obj.get("race", {})
                    race_number = race.get("race_number")
                    distance_m = race.get("distance_m")
                    class_text = race.get("class_text")
                    race_name = race.get("name")

                    if race_number is None:
                        continue

                    for tip in race_obj.get("tips", []):
                        tab_number = tip.get("tab_number")
                        if tab_number is None:
                            continue

                        tips.append(FlatTip(
                            meeting_date=meeting_date,
                            state=state,
                            track_name=track_name,
                            race_number=int(race_number),
                            tab_number=int(tab_number),
                            horse_name=tip.get("horse_name") or f"#{tab_number}",
                            tip_type=tip.get("tip_type") or "UNKNOWN",
                            distance_m=distance_m,
                            class_text=class_text,
                            race_name=race_name,
                        ))
    except Exception as e:
        print(f"[TRENDS] Error fetching tips for {d}: {e}")

    return tips


def _fetch_results_for_date(d: date) -> List[FlatResult]:
    """
    Fetch results from RA Crawler API for a single date.
    GET /results?date=YYYY-MM-DD
    """
    base_url = os.getenv("RA_CRAWLER_BASE_URL", "https://ra-crawler.onrender.com")
    url = f"{base_url.rstrip('/')}/results"

    results: List[FlatResult] = []

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, params={"date": d.isoformat()})
            resp.raise_for_status()
            data = resp.json()

            # Data structure: list of result objects
            for item in data:
                state = (item.get("state") or "").upper()
                track = item.get("track") or ""
                race_no = item.get("race_no")
                horse_number = item.get("horse_number")

                if not state or race_no is None or horse_number is None:
                    continue

                results.append(FlatResult(
                    meeting_date=d,
                    state=state,
                    track=track,
                    race_no=int(race_no),
                    horse_number=int(horse_number),
                    horse_name=item.get("horse_name") or f"#{horse_number}",
                    finishing_pos=item.get("finishing_pos"),
                    is_scratched=bool(item.get("is_scratched")),
                    starting_price=item.get("starting_price"),
                ))
    except Exception as e:
        print(f"[TRENDS] Error fetching results for {d}: {e}")

    return results


def _build_results_index(results: List[FlatResult]) -> Dict[Tuple[date, str, int, int], FlatResult]:
    """
    Build lookup index for results.
    Key: (date, STATE, race_no, horse_number)

    If there are multiple results for same key (shouldn't happen), keep the first.
    """
    index: Dict[Tuple[date, str, int, int], FlatResult] = {}

    for r in results:
        key = (r.meeting_date, r.state, r.race_no, r.horse_number)
        if key not in index:
            index[key] = r

    return index


def _get_distance_bucket(distance_m: Optional[int]) -> str:
    """Categorize distance into buckets"""
    if distance_m is None:
        return "Unknown"
    if distance_m < 1000:
        return "Sprint (<1000m)"
    elif distance_m < 1200:
        return "1000-1199m"
    elif distance_m < 1400:
        return "1200-1399m"
    elif distance_m < 1600:
        return "1400-1599m"
    elif distance_m < 1800:
        return "1600-1799m"
    elif distance_m < 2000:
        return "1800-1999m"
    elif distance_m < 2400:
        return "2000-2399m"
    else:
        return "Stayer (2400m+)"


def _get_price_bucket(sp: Optional[float]) -> str:
    """Categorize starting price into buckets"""
    if sp is None:
        return "Unknown"
    if sp < 2.0:
        return "$1.01-$1.99 (Hot Fav)"
    elif sp < 3.0:
        return "$2.00-$2.99 (Fav)"
    elif sp < 4.0:
        return "$3.00-$3.99"
    elif sp < 6.0:
        return "$4.00-$5.99"
    elif sp < 10.0:
        return "$6.00-$9.99"
    elif sp < 15.0:
        return "$10.00-$14.99"
    elif sp < 21.0:
        return "$15.00-$20.99"
    else:
        return "$21.00+ (Roughie)"


def _get_track_type(track_name: str) -> str:
    """Determine track type based on track name."""
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

    provincial_indicators = ["park", "gardens", "lakeside"]
    for ind in provincial_indicators:
        if ind in track_lower:
            return "Provincial"

    return "Provincial/Country"


def _get_class_bucket(class_text: Optional[str], race_name: Optional[str]) -> str:
    """Categorize race class"""
    import re
    text = ((class_text or "") + " " + (race_name or "")).lower()

    if "maiden" in text:
        return "Maiden"
    elif "benchmark" in text or "bm" in text:
        match = re.search(r'(?:benchmark|bm)\s*(\d+)', text)
        if match:
            bm = int(match.group(1))
            if bm <= 58:
                return "BM58 & below"
            elif bm <= 68:
                return "BM64-68"
            elif bm <= 78:
                return "BM72-78"
            else:
                return "BM82+"
        return "Benchmark"
    elif "class" in text:
        match = re.search(r'class\s*(\d+)', text)
        if match:
            return f"Class {match.group(1)}"
        return "Class race"
    elif "group" in text or "g1" in text or "g2" in text or "g3" in text:
        return "Group/Stakes"
    elif "listed" in text:
        return "Listed"
    elif "restricted" in text:
        return "Restricted"
    elif "handicap" in text:
        return "Handicap"
    elif "open" in text:
        return "Open"
    else:
        return "Other"


def compute_trends(
    db: Session,  # Not used anymore but keeping for API compatibility
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Compute comprehensive trend analysis across all dimensions.

    Fetches data directly from:
    - Tips API: /tips?date=YYYY-MM-DD
    - RA Crawler: /results?date=YYYY-MM-DD

    Matches tips to results using: (date, state, race_number, tab_number)
    """
    # Default to last 60 days
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=60)

    print(f"[TRENDS] Computing trends from {date_from} to {date_to}")

    # 1) Fetch all tips and results for the date range
    all_tips: List[FlatTip] = []
    all_results: List[FlatResult] = []

    day = date_from
    while day <= date_to:
        tips = _fetch_tips_for_date(day)
        results = _fetch_results_for_date(day)

        print(f"[TRENDS] {day}: {len(tips)} tips, {len(results)} results")

        all_tips.extend(tips)
        all_results.extend(results)
        day += timedelta(days=1)

    print(f"[TRENDS] Total: {len(all_tips)} tips, {len(all_results)} results")

    if not all_tips:
        return {"error": "No tips found", "has_data": False}

    # 2) Build results index for fast lookup
    results_index = _build_results_index(all_results)
    print(f"[TRENDS] Results index has {len(results_index)} entries")

    # Count how many tips have matching results
    tips_with_match = sum(1 for t in all_tips if (t.meeting_date, t.state, t.race_number, t.tab_number) in results_index)
    print(f"[TRENDS] Tips with matching results: {tips_with_match} / {len(all_tips)}")

    # Check for duplicate tip keys (same horse tipped multiple times in same race)
    tip_keys = [(t.meeting_date, t.state, t.race_number, t.tab_number) for t in all_tips]
    unique_tip_keys = set(tip_keys)
    if len(tip_keys) != len(unique_tip_keys):
        dupes = len(tip_keys) - len(unique_tip_keys)
        print(f"[TRENDS] Note: {dupes} tips share same horse (different tip_types for same runner)")

    # Full tip keys including tip_type
    full_tip_keys = [(t.meeting_date, t.state, t.race_number, t.tab_number, t.tip_type) for t in all_tips]
    unique_full_keys = set(full_tip_keys)
    print(f"[TRENDS] Unique tip combinations: {len(unique_full_keys)}")

    # Debug: show sample keys from both
    sample_tip_keys = []
    for t in all_tips[:5]:
        sample_tip_keys.append((t.meeting_date, t.state, t.race_number, t.tab_number))
    print(f"[TRENDS] Sample tip keys: {sample_tip_keys}")

    sample_result_keys = list(results_index.keys())[:5]
    print(f"[TRENDS] Sample result keys: {sample_result_keys}")

    # 3) Initialize buckets for each dimension
    by_distance: Dict[str, TrendBucket] = {}
    by_price: Dict[str, TrendBucket] = {}
    by_track_type: Dict[str, TrendBucket] = {}
    by_race_number: Dict[str, TrendBucket] = {}
    by_class: Dict[str, TrendBucket] = {}
    by_state: Dict[str, TrendBucket] = {}
    by_tip_type: Dict[str, TrendBucket] = {}
    by_track: Dict[str, TrendBucket] = {}

    total_tips = 0
    matched_tips = 0
    scratched_tips = 0

    # 4) Process each tip
    for tip in all_tips:
        # Build lookup key: (date, state, race_number, tab_number)
        lookup_key = (tip.meeting_date, tip.state, tip.race_number, tip.tab_number)

        # Find matching result
        result = results_index.get(lookup_key)

        # Determine outcome
        finish_pos: Optional[int] = None
        sp: Optional[float] = None
        is_scratched = False

        if result:
            matched_tips += 1
            is_scratched = result.is_scratched
            if not is_scratched:
                finish_pos = result.finishing_pos
                sp = result.starting_price

        # Skip scratched runners
        if is_scratched:
            scratched_tips += 1
            continue

        total_tips += 1

        # Determine bucket keys
        distance_key = _get_distance_bucket(tip.distance_m)
        price_key = _get_price_bucket(sp)
        track_type_key = _get_track_type(tip.track_name)
        race_num_key = f"Race {tip.race_number}"
        class_key = _get_class_bucket(tip.class_text, tip.race_name)
        state_key = tip.state or "Unknown"
        tip_type_key = tip.tip_type
        track_key = f"{tip.track_name} ({tip.state})"

        # Helper to update a bucket
        def update_bucket(buckets: Dict[str, TrendBucket], key: str, pos: Optional[int]):
            if key not in buckets:
                buckets[key] = TrendBucket(label=key)
            bucket = buckets[key]
            bucket.tips += 1

            if pos is not None:
                if pos == 1:
                    bucket.wins += 1
                elif pos == 2:
                    bucket.seconds += 1
                elif pos == 3:
                    bucket.thirds += 1

        # Update all dimension buckets
        update_bucket(by_distance, distance_key, finish_pos)
        update_bucket(by_price, price_key, finish_pos)
        update_bucket(by_track_type, track_type_key, finish_pos)
        update_bucket(by_race_number, race_num_key, finish_pos)
        update_bucket(by_class, class_key, finish_pos)
        update_bucket(by_state, state_key, finish_pos)
        update_bucket(by_tip_type, tip_type_key, finish_pos)
        update_bucket(by_track, track_key, finish_pos)

    print(f"[TRENDS] Processed: {total_tips} tips (matched: {matched_tips}, scratched: {scratched_tips})")

    if total_tips == 0:
        return {"error": "No tips found after filtering", "has_data": False}

    # 5) Sort and convert to output format
    def sort_buckets(buckets: Dict[str, TrendBucket], sort_key: str = "win_strike_rate") -> List[Dict]:
        items = list(buckets.values())
        items = [b for b in items if b.tips >= 5]
        if sort_key == "win_strike_rate":
            items.sort(key=lambda x: x.win_strike_rate, reverse=True)
        elif sort_key == "place_strike_rate":
            items.sort(key=lambda x: x.place_strike_rate, reverse=True)
        elif sort_key == "tips":
            items.sort(key=lambda x: x.tips, reverse=True)
        elif sort_key == "label":
            items.sort(key=lambda x: x.label)
        return [b.to_dict() for b in items]

    def sort_race_numbers(buckets: Dict[str, TrendBucket]) -> List[Dict]:
        items = list(buckets.values())
        items = [b for b in items if b.tips >= 5]
        items.sort(key=lambda x: int(x.label.replace("Race ", "")) if x.label.startswith("Race ") else 99)
        return [b.to_dict() for b in items]

    # Calculate overall stats
    overall_tips = sum(b.tips for b in by_tip_type.values())
    overall_wins = sum(b.wins for b in by_tip_type.values())
    overall_seconds = sum(b.seconds for b in by_tip_type.values())
    overall_thirds = sum(b.thirds for b in by_tip_type.values())
    overall_podium = overall_wins + overall_seconds + overall_thirds

    return {
        "has_data": True,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,

        "overall": {
            "tips": overall_tips,
            "wins": overall_wins,
            "seconds": overall_seconds,
            "thirds": overall_thirds,
            "podium": overall_podium,
            "win_strike_rate": round(overall_wins / overall_tips * 100, 1) if overall_tips > 0 else 0,
            "place_strike_rate": round(overall_podium / overall_tips * 100, 1) if overall_tips > 0 else 0,
        },

        "by_distance": sort_buckets(by_distance, "win_strike_rate"),
        "by_price": sort_buckets(by_price, "label"),
        "by_track_type": sort_buckets(by_track_type, "win_strike_rate"),
        "by_race_number": sort_race_numbers(by_race_number),
        "by_class": sort_buckets(by_class, "win_strike_rate"),
        "by_state": sort_buckets(by_state, "win_strike_rate"),
        "by_tip_type": sort_buckets(by_tip_type, "win_strike_rate"),
        "by_track": sort_buckets(by_track, "win_strike_rate"),

        "insights": _generate_insights(
            by_distance, by_price, by_track_type,
            by_race_number, by_class, by_state, by_tip_type
        ),
    }


def _generate_insights(
    by_distance: Dict[str, TrendBucket],
    by_price: Dict[str, TrendBucket],
    by_track_type: Dict[str, TrendBucket],
    by_race_number: Dict[str, TrendBucket],
    by_class: Dict[str, TrendBucket],
    by_state: Dict[str, TrendBucket],
    by_tip_type: Dict[str, TrendBucket],
) -> List[Dict[str, Any]]:
    """Generate actionable insights from the trend data"""
    insights = []

    def best_in_category(buckets: Dict[str, TrendBucket], category: str, min_tips: int = 10):
        items = [(k, v) for k, v in buckets.items() if v.tips >= min_tips]
        if not items:
            return None
        best = max(items, key=lambda x: x[1].win_strike_rate)
        worst = min(items, key=lambda x: x[1].win_strike_rate)
        return {
            "category": category,
            "best": {
                "label": best[0],
                "win_sr": round(best[1].win_strike_rate, 1),
                "place_sr": round(best[1].place_strike_rate, 1),
                "tips": best[1].tips,
                "wins": best[1].wins,
            },
            "worst": {
                "label": worst[0],
                "win_sr": round(worst[1].win_strike_rate, 1),
                "place_sr": round(worst[1].place_strike_rate, 1),
                "tips": worst[1].tips,
                "wins": worst[1].wins,
            },
        }

    categories = [
        (by_distance, "Distance"),
        (by_price, "Price Range"),
        (by_track_type, "Track Type"),
        (by_race_number, "Race Number"),
        (by_class, "Race Class"),
        (by_state, "State"),
        (by_tip_type, "Tip Type"),
    ]

    for buckets, name in categories:
        insight = best_in_category(buckets, name)
        if insight:
            insights.append(insight)

    return insights
