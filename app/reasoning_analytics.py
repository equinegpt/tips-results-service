# app/reasoning_analytics.py
"""
Phase 2 Analytics: Analyze which reasoning phrases correlate with winning tips.

Extracts key phrases from tip reasoning and tracks their win/place rates.
"""
from __future__ import annotations

import os
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx


@dataclass
class ReasoningBucket:
    """Tracks performance for tips containing a specific phrase/keyword"""
    phrase: str
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
            "phrase": self.phrase,
            "tips": self.tips,
            "wins": self.wins,
            "seconds": self.seconds,
            "thirds": self.thirds,
            "podium": self.podium,
            "win_rate": round(self.win_rate, 1),
            "place_rate": round(self.place_rate, 1),
        }


# Key phrases to look for in reasoning
# These are the betting-relevant factors mentioned in tips
REASONING_PHRASES = {
    # Sectional time indicators
    "last600": ["last600", "last 600", "last 600m"],
    "last200": ["last200", "last 200", "last 200m"],
    "sectionals": ["sectional", "sectionals"],
    "fast_sectionals": ["fast sectional", "strong sectional", "standout sectional", "top sectional"],

    # Mapping/positioning
    "maps_forward": ["maps forward", "forward map", "on-pace", "on pace"],
    "maps_midfield": ["maps midfield", "midfield", "mid-field"],
    "leader": ["leader", "leading", "lead"],
    "swooper": ["swoop", "swooper", "swooping", "backmarker"],

    # Barrier mentions
    "barrier_suits": ["barrier suits", "barrier advantage", "ideal barrier", "favourable barrier", "barrier 1", "inside barrier"],
    "wide_barrier": ["wide barrier", "barrier 6", "barrier 7", "barrier 8"],

    # Form indicators
    "consistent": ["consistent", "consistently"],
    "proven": ["proven"],
    "improving": ["improving", "improvement"],

    # Track/distance
    "track_specialist": ["track specialist", "proven at track", "at track"],
    "distance_proven": ["proven at distance", "distance specialist", "at distance", "over distance"],

    # Conditions
    "wet_track": ["heavy", "soft track", "wet"],
    "good_track": ["good track"],

    # Trainer/jockey
    "trainer_form": ["trainer strike", "trainer form", "top trainer"],
    "jockey_form": ["jockey strike", "jockey form", "jockey in form", "jockey a2e"],

    # Speed indicators
    "early_speed": ["early speed", "early pace"],
    "finishing_speed": ["finishing speed", "finish speed", "turn-of-foot", "fast finish"],

    # Class indicators
    "class_adjusted": ["class-adjusted", "class adjusted", "class benchmark"],

    # Specific metrics mentioned
    "lengths_faster": ["lengths faster", "lengths vs avg", "lengths better"],
    "pfai_score": ["pfai", "pf ai"],
}


def _extract_phrases(reasoning: str) -> Set[str]:
    """Extract which key phrases are present in a reasoning string"""
    if not reasoning:
        return set()

    reasoning_lower = reasoning.lower()
    found = set()

    for phrase_key, variants in REASONING_PHRASES.items():
        for variant in variants:
            if variant in reasoning_lower:
                found.add(phrase_key)
                break

    return found


def _fetch_tips_for_date(d: date) -> List[Dict[str, Any]]:
    """Fetch tips with full details including reasoning"""
    base_url = os.getenv("TIPS_SERVICE_BASE_URL", "https://tips-results-service.onrender.com")
    url = f"{base_url.rstrip('/')}/tips"

    tips = []

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, params={"date": d.isoformat()})
            resp.raise_for_status()
            data = resp.json()

            for meeting_obj in data:
                meeting = meeting_obj.get("meeting", {})
                state = (meeting.get("state") or "").upper()
                track_name = meeting.get("track_name") or ""

                for race_obj in meeting_obj.get("races", []):
                    race = race_obj.get("race", {})
                    race_number = race.get("race_number")

                    if race_number is None:
                        continue

                    for tip in race_obj.get("tips", []):
                        tab_number = tip.get("tab_number")
                        if tab_number is None:
                            continue

                        tips.append({
                            "date": d,
                            "state": state,
                            "track_name": track_name,
                            "race_number": int(race_number),
                            "tab_number": int(tab_number),
                            "horse_name": tip.get("horse_name") or "",
                            "tip_type": tip.get("tip_type") or "UNKNOWN",
                            "reasoning": tip.get("reasoning") or "",
                        })
    except Exception as e:
        print(f"[REASONING] Error fetching tips for {d}: {e}")

    return tips


def _fetch_results_for_date(d: date) -> Dict[Tuple[str, int, int], Dict[str, Any]]:
    """Fetch results and return as lookup dict keyed by (state, race_no, horse_number)"""
    base_url = os.getenv("RA_CRAWLER_BASE_URL", "https://ra-crawler.onrender.com")
    url = f"{base_url.rstrip('/')}/results"

    results = {}

    try:
        with httpx.Client(timeout=30.0) as client:
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
    except Exception as e:
        print(f"[REASONING] Error fetching results for {d}: {e}")

    return results


def compute_reasoning_trends(
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> Dict[str, Any]:
    """
    Analyze which reasoning phrases correlate with better results.
    """
    if date_to is None:
        date_to = date.today()
    if date_from is None:
        date_from = date_to - timedelta(days=30)

    print(f"[REASONING] Analyzing reasoning trends from {date_from} to {date_to}")

    # Initialize buckets for each phrase
    phrase_buckets: Dict[str, ReasoningBucket] = {
        key: ReasoningBucket(phrase=key) for key in REASONING_PHRASES.keys()
    }

    # Also track by tip_type + phrase
    type_phrase_buckets: Dict[str, Dict[str, ReasoningBucket]] = {
        "AI_BEST": {key: ReasoningBucket(phrase=key) for key in REASONING_PHRASES.keys()},
        "DANGER": {key: ReasoningBucket(phrase=key) for key in REASONING_PHRASES.keys()},
        "VALUE": {key: ReasoningBucket(phrase=key) for key in REASONING_PHRASES.keys()},
    }

    total_tips = 0
    matched_tips = 0

    # Process each day
    day = date_from
    while day <= date_to:
        tips = _fetch_tips_for_date(day)
        results = _fetch_results_for_date(day)

        print(f"[REASONING] {day}: {len(tips)} tips, {len(results)} results")

        for tip in tips:
            # Look up result
            key = (tip["state"], tip["race_number"], tip["tab_number"])
            result = results.get(key)

            if not result or result.get("is_scratched"):
                continue

            total_tips += 1
            finish_pos = result.get("finishing_pos")

            if finish_pos is not None:
                matched_tips += 1

            # Extract phrases from reasoning
            phrases = _extract_phrases(tip["reasoning"])
            tip_type = tip["tip_type"]

            # Update buckets for each phrase found
            for phrase in phrases:
                bucket = phrase_buckets[phrase]
                bucket.tips += 1

                if finish_pos == 1:
                    bucket.wins += 1
                elif finish_pos == 2:
                    bucket.seconds += 1
                elif finish_pos == 3:
                    bucket.thirds += 1

                # Also update type-specific bucket
                if tip_type in type_phrase_buckets:
                    type_bucket = type_phrase_buckets[tip_type][phrase]
                    type_bucket.tips += 1
                    if finish_pos == 1:
                        type_bucket.wins += 1
                    elif finish_pos == 2:
                        type_bucket.seconds += 1
                    elif finish_pos == 3:
                        type_bucket.thirds += 1

        day += timedelta(days=1)

    print(f"[REASONING] Total: {total_tips} tips, {matched_tips} with results")

    # Convert to output format - filter to phrases with enough data
    def to_sorted_list(buckets: Dict[str, ReasoningBucket], min_tips: int = 20) -> List[Dict]:
        items = [b for b in buckets.values() if b.tips >= min_tips]
        items.sort(key=lambda x: x.win_rate, reverse=True)
        return [b.to_dict() for b in items]

    # Find winning and losing phrases
    all_phrases = [b for b in phrase_buckets.values() if b.tips >= 20]
    all_phrases.sort(key=lambda x: x.win_rate, reverse=True)

    winning_phrases = all_phrases[:10] if len(all_phrases) >= 10 else all_phrases
    losing_phrases = all_phrases[-10:] if len(all_phrases) >= 10 else []
    losing_phrases = list(reversed(losing_phrases))  # Worst first

    return {
        "has_data": total_tips > 0,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "total_tips": total_tips,
        "matched_tips": matched_tips,

        # Overall phrase performance
        "all_phrases": to_sorted_list(phrase_buckets),

        # Top/bottom performers
        "winning_phrases": [p.to_dict() for p in winning_phrases],
        "losing_phrases": [p.to_dict() for p in losing_phrases],

        # By tip type
        "ai_best_phrases": to_sorted_list(type_phrase_buckets.get("AI_BEST", {})),
        "danger_phrases": to_sorted_list(type_phrase_buckets.get("DANGER", {})),
        "value_phrases": to_sorted_list(type_phrase_buckets.get("VALUE", {})),

        # Actionable insights
        "insights": _generate_reasoning_insights(phrase_buckets, type_phrase_buckets),
    }


def _generate_reasoning_insights(
    phrase_buckets: Dict[str, ReasoningBucket],
    type_phrase_buckets: Dict[str, Dict[str, ReasoningBucket]],
) -> List[Dict[str, Any]]:
    """Generate actionable insights about which reasoning factors matter most"""
    insights = []

    # Find phrases with significantly better than average win rate
    all_valid = [b for b in phrase_buckets.values() if b.tips >= 20]
    if not all_valid:
        return insights

    avg_win_rate = sum(b.wins for b in all_valid) / sum(b.tips for b in all_valid) * 100
    avg_place_rate = sum(b.podium for b in all_valid) / sum(b.tips for b in all_valid) * 100

    # Phrases that beat the average significantly
    outperformers = [b for b in all_valid if b.win_rate > avg_win_rate * 1.2]
    outperformers.sort(key=lambda x: x.win_rate, reverse=True)

    if outperformers:
        insights.append({
            "type": "outperformers",
            "title": "Reasoning factors that outperform average",
            "description": f"Average win rate is {avg_win_rate:.1f}%. These phrases beat it by 20%+",
            "phrases": [{"phrase": b.phrase, "win_rate": round(b.win_rate, 1), "tips": b.tips} for b in outperformers[:5]],
        })

    # Phrases that underperform
    underperformers = [b for b in all_valid if b.win_rate < avg_win_rate * 0.8]
    underperformers.sort(key=lambda x: x.win_rate)

    if underperformers:
        insights.append({
            "type": "underperformers",
            "title": "Reasoning factors that underperform",
            "description": f"These phrases have win rates 20%+ below average ({avg_win_rate:.1f}%)",
            "phrases": [{"phrase": b.phrase, "win_rate": round(b.win_rate, 1), "tips": b.tips} for b in underperformers[:5]],
        })

    # Best phrases for each tip type
    for tip_type, buckets in type_phrase_buckets.items():
        valid = [b for b in buckets.values() if b.tips >= 10]
        if valid:
            best = max(valid, key=lambda x: x.win_rate)
            worst = min(valid, key=lambda x: x.win_rate)
            insights.append({
                "type": f"best_for_{tip_type.lower()}",
                "title": f"Best reasoning for {tip_type} tips",
                "best": {"phrase": best.phrase, "win_rate": round(best.win_rate, 1), "tips": best.tips},
                "worst": {"phrase": worst.phrase, "win_rate": round(worst.win_rate, 1), "tips": worst.tips},
            })

    return insights
