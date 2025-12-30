# app/trends_analytics.py
"""
Deep analytics for identifying winning trends across various dimensions:
- Distance ranges
- Price points (SP)
- Track types (Metro/Provincial/Country)
- Race numbers
- Race classes (Maiden, etc.)
- States
- Tip types

Uses RA Crawler results as primary source (same as Overview page).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from . import models
from .ra_results_client import RAResultsClient
from .daily_generator import _tracks_match


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


def _get_price_bucket(sp: Optional[Decimal]) -> str:
    """Categorize starting price into buckets"""
    if sp is None:
        return "Unknown"
    price = float(sp)
    if price < 2.0:
        return "$1.01-$1.99 (Hot Fav)"
    elif price < 3.0:
        return "$2.00-$2.99 (Fav)"
    elif price < 4.0:
        return "$3.00-$3.99"
    elif price < 6.0:
        return "$4.00-$5.99"
    elif price < 10.0:
        return "$6.00-$9.99"
    elif price < 15.0:
        return "$10.00-$14.99"
    elif price < 21.0:
        return "$15.00-$20.99"
    else:
        return "$21.00+ (Roughie)"


def _get_track_type(track_name: str) -> str:
    """
    Determine track type based on track name.
    """
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


def _build_ra_results_index(
    date_from: date,
    date_to: date,
) -> Dict[Tuple[date, str, int, int], List[Any]]:
    """
    Build index over RA Crawler results for the date window.
    Key: (meeting_date, STATE, race_no, tab_number) -> list[RAResultRow]

    Same approach as routes_ui_overview.py uses.
    """
    runner_index: Dict[Tuple[date, str, int, int], List[Any]] = {}

    client = RAResultsClient()
    try:
        day = date_from
        while day <= date_to:
            try:
                rows = client.fetch_results_for_date(day)
                print(f"[TRENDS] RA rows for {day}: {len(rows)}")
            except Exception as e:
                print(f"[TRENDS] error fetching RA rows for {day}: {e}")
                rows = []

            for r in rows:
                k_runner = (
                    r.meeting_date,
                    (r.state or "").upper(),
                    r.race_no,
                    r.tab_number,
                )
                runner_index.setdefault(k_runner, []).append(r)

            day += timedelta(days=1)
    finally:
        client.close()

    total_runner = sum(len(v) for v in runner_index.values())
    print(f"[TRENDS] RA runner_index total rows = {total_runner}")
    return runner_index


def compute_trends(
    db: Session,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Compute comprehensive trend analysis across all dimensions.
    Uses RA Crawler results as primary source (same as Overview page).
    """
    # Default to last 60 days
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=60)

    # 1) Build RA results index for the window (same as Overview)
    ra_index = _build_ra_results_index(date_from, date_to)

    # 2) Fetch all tips in that window
    rows = (
        db.query(models.Tip, models.Race, models.Meeting)
        .join(models.Race, models.Tip.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .filter(models.Meeting.date >= date_from)
        .filter(models.Meeting.date <= date_to)
        .all()
    )

    if not rows:
        return {"error": "No tips found", "has_data": False}

    print(f"[TRENDS] Tips in window {date_from} -> {date_to}: {len(rows)}")

    # Initialize buckets for each dimension
    by_distance: Dict[str, TrendBucket] = {}
    by_price: Dict[str, TrendBucket] = {}
    by_track_type: Dict[str, TrendBucket] = {}
    by_race_number: Dict[str, TrendBucket] = {}
    by_class: Dict[str, TrendBucket] = {}
    by_state: Dict[str, TrendBucket] = {}
    by_tip_type: Dict[str, TrendBucket] = {}
    by_track: Dict[str, TrendBucket] = {}

    tips_with_results = 0
    total_tips_counted = 0

    # Process each tip (count ALL tips like Overview does)
    for tip, race, meeting in rows:
        # Look up RA result for this tip (same matching as Overview)
        ra_key = (
            meeting.date,
            (meeting.state or "").upper(),
            race.race_number,
            tip.tab_number,
        )
        candidates = ra_index.get(ra_key) or []

        # Find matching RA row (with fuzzy track matching if multiple)
        ra_row = None
        if candidates:
            mt_track = meeting.track_name or ""
            if len(candidates) == 1:
                ra_row = candidates[0]
            else:
                for cand in candidates:
                    try:
                        if _tracks_match(mt_track, getattr(cand, "track", "") or ""):
                            ra_row = cand
                            break
                    except Exception:
                        pass
                if ra_row is None:
                    ra_row = candidates[0]

        # Determine finish position and SP from RA results (if available)
        finish_pos: Optional[int] = None
        sp: Any = None
        is_scratched = False

        if ra_row is not None:
            is_scratched = getattr(ra_row, "is_scratched", False)
            if not is_scratched:
                finish_pos = getattr(ra_row, "finishing_pos", None)
                sp = getattr(ra_row, "starting_price", None)
                if finish_pos is not None:
                    tips_with_results += 1

        # Skip scratched runners (don't count them at all)
        if is_scratched:
            continue

        total_tips_counted += 1

        # Determine bucket keys
        distance_key = _get_distance_bucket(race.distance_m)
        price_key = _get_price_bucket(Decimal(str(sp)) if sp else None)
        track_type_key = _get_track_type(meeting.track_name)
        race_num_key = f"Race {race.race_number}"
        class_key = _get_class_bucket(race.class_text, race.name)
        state_key = meeting.state or "Unknown"
        tip_type_key = tip.tip_type
        track_key = f"{meeting.track_name} ({meeting.state})"

        # Helper to update a bucket - counts all tips, only records positions for resolved tips
        def update_bucket(buckets: Dict[str, TrendBucket], key: str, pos: Optional[int]):
            if key not in buckets:
                buckets[key] = TrendBucket(label=key)
            bucket = buckets[key]
            bucket.tips += 1

            # Only record position if we have a result
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

    print(f"[TRENDS] Total tips counted: {total_tips_counted}")
    print(f"[TRENDS] Tips with RA results: {tips_with_results}")

    if total_tips_counted == 0:
        return {"error": "No tips found", "has_data": False}

    # Sort and convert to output format
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
    total_tips = sum(b.tips for b in by_tip_type.values())
    total_wins = sum(b.wins for b in by_tip_type.values())
    total_seconds = sum(b.seconds for b in by_tip_type.values())
    total_thirds = sum(b.thirds for b in by_tip_type.values())
    total_podium = total_wins + total_seconds + total_thirds

    return {
        "has_data": True,
        "date_from": date_from.isoformat() if date_from else None,
        "date_to": date_to.isoformat() if date_to else None,

        "overall": {
            "tips": total_tips,
            "wins": total_wins,
            "seconds": total_seconds,
            "thirds": total_thirds,
            "podium": total_podium,
            "win_strike_rate": round(total_wins / total_tips * 100, 1) if total_tips > 0 else 0,
            "place_strike_rate": round(total_podium / total_tips * 100, 1) if total_tips > 0 else 0,
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
