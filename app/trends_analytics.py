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
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
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


def _safe_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


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
    This is a simplified heuristic - ideally we'd have this data from PF/RA.
    """
    metro_tracks = {
        "flemington", "caulfield", "moonee valley", "sandown",
        "randwick", "royal randwick", "rosehill", "warwick farm", "canterbury",
        "doomben", "eagle farm",
        "morphettville", "morphettville parks",
        "ascot", "belmont",
    }

    track_lower = track_name.lower()

    # Check for metro tracks
    for metro in metro_tracks:
        if metro in track_lower:
            return "Metro"

    # Provincial indicators
    provincial_indicators = ["park", "gardens", "lakeside"]
    for ind in provincial_indicators:
        if ind in track_lower:
            return "Provincial"

    # Default to Provincial for known larger tracks, Country for others
    # This is imperfect but gives us something to work with
    return "Provincial/Country"


def _get_class_bucket(class_text: Optional[str], race_name: Optional[str]) -> str:
    """Categorize race class"""
    text = ((class_text or "") + " " + (race_name or "")).lower()

    if "maiden" in text:
        return "Maiden"
    elif "benchmark" in text or "bm" in text:
        # Try to extract BM number
        import re
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
        import re
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
    db: Session,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Compute comprehensive trend analysis across all dimensions.
    Returns data suitable for dashboard visualization.
    """

    # Build base query with all necessary joins
    q = (
        db.query(
            models.Tip,
            models.TipOutcome,
            models.Race,
            models.Meeting,
            models.RaceResult,
        )
        .join(models.Race, models.Tip.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .outerjoin(
            models.TipOutcome,
            models.TipOutcome.tip_id == models.Tip.id,
        )
        .outerjoin(
            models.RaceResult,
            (models.RaceResult.race_id == models.Race.id) &
            (models.RaceResult.tab_number == models.Tip.tab_number),
        )
    )

    if date_from:
        q = q.filter(models.Meeting.date >= date_from)
    if date_to:
        q = q.filter(models.Meeting.date <= date_to)

    rows = q.all()

    if not rows:
        return {"error": "No data found", "has_data": False}

    # Initialize buckets for each dimension
    by_distance: Dict[str, TrendBucket] = {}
    by_price: Dict[str, TrendBucket] = {}
    by_track_type: Dict[str, TrendBucket] = {}
    by_race_number: Dict[str, TrendBucket] = {}
    by_class: Dict[str, TrendBucket] = {}
    by_state: Dict[str, TrendBucket] = {}
    by_tip_type: Dict[str, TrendBucket] = {}
    by_track: Dict[str, TrendBucket] = {}

    # Process each tip
    for tip, outcome, race, meeting, race_result in rows:
        # Get finish position
        finish_pos = None
        sp = None

        if outcome:
            finish_pos = outcome.finish_position
            sp = outcome.starting_price
        elif race_result:
            finish_pos = race_result.finish_position
            sp = race_result.starting_price

        # Skip scratched/pending
        if outcome and outcome.outcome_status in ("SCRATCHED", "NO_RESULT", "PENDING"):
            continue

        # Determine bucket keys
        distance_key = _get_distance_bucket(race.distance_m)
        price_key = _get_price_bucket(sp) if sp else "Unknown"
        track_type_key = _get_track_type(meeting.track_name)
        race_num_key = f"Race {race.race_number}"
        class_key = _get_class_bucket(race.class_text, race.name)
        state_key = meeting.state or "Unknown"
        tip_type_key = tip.tip_type
        track_key = f"{meeting.track_name} ({meeting.state})"

        # Helper to update a bucket
        def update_bucket(buckets: Dict[str, TrendBucket], key: str):
            if key not in buckets:
                buckets[key] = TrendBucket(label=key)
            bucket = buckets[key]
            bucket.tips += 1

            if finish_pos == 1:
                bucket.wins += 1
            elif finish_pos == 2:
                bucket.seconds += 1
            elif finish_pos == 3:
                bucket.thirds += 1

        # Update all dimension buckets
        update_bucket(by_distance, distance_key)
        update_bucket(by_price, price_key)
        update_bucket(by_track_type, track_type_key)
        update_bucket(by_race_number, race_num_key)
        update_bucket(by_class, class_key)
        update_bucket(by_state, state_key)
        update_bucket(by_tip_type, tip_type_key)
        update_bucket(by_track, track_key)

    # Sort and convert to output format
    def sort_buckets(buckets: Dict[str, TrendBucket], sort_key: str = "win_strike_rate") -> List[Dict]:
        items = list(buckets.values())
        # Filter out buckets with very few tips (noise)
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

    # Sort race numbers properly
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
        "by_price": sort_buckets(by_price, "label"),  # Sort by price range
        "by_track_type": sort_buckets(by_track_type, "win_strike_rate"),
        "by_race_number": sort_race_numbers(by_race_number),
        "by_class": sort_buckets(by_class, "win_strike_rate"),
        "by_state": sort_buckets(by_state, "win_strike_rate"),
        "by_tip_type": sort_buckets(by_tip_type, "win_strike_rate"),
        "by_track": sort_buckets(by_track, "win_strike_rate"),

        # Insights - highlight best/worst performers
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

    # Generate insights for each category
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
