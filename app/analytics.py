# app/analytics.py
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from . import models, schemas
from .config import settings


@dataclass
class _Agg:
    tips: int = 0
    wins: int = 0
    places: int = 0
    total_staked: Decimal = Decimal("0")   # only where we know SP
    net_profit: Decimal = Decimal("0")     # based on win SP only


def _aggregate_rows(
    rows,
    provider: str,
    stake_per_tip: float,
) -> Dict[str, _Agg]:
    """
    Shared aggregation logic for both day and range stats.
    """
    aggs: Dict[str, _Agg] = defaultdict(_Agg)
    stake_unit = Decimal(str(stake_per_tip))

    for tip, outcome, meeting in rows:
        agg = aggs[tip.tip_type]
        agg.tips += 1

        if outcome is None:
            # RA results not run yet for this tip
            continue

        status = (outcome.outcome_status or "").upper()
        if status == "WIN":
            agg.wins += 1
        elif status == "PLACE":
            agg.places += 1

        # For ROI, we only use WIN/LOSE and only when we have a starting_price.
        if status not in ("WIN", "LOSE"):
            # SCRATCHED / NO_RESULT / PENDING: no bet, no stake.
            continue

        if outcome.starting_price is None:
            # No SP available: we skip this from ROI calculation entirely.
            continue

        try:
            units = Decimal(str(float(tip.stake_units or 1.0)))
        except Exception:
            units = Decimal("1")

        stake = stake_unit * units
        price = Decimal(str(float(outcome.starting_price)))

        agg.total_staked += stake
        if status == "WIN":
            agg.net_profit += stake * (price - Decimal("1"))
        else:  # LOSE
            agg.net_profit -= stake

    return aggs


def _aggs_to_tip_type_stats(
    aggs: Dict[str, _Agg]
) -> List[schemas.TipTypeStats]:
    stats_out: List[schemas.TipTypeStats] = []

    for tip_type, agg in aggs.items():
        tips = agg.tips
        wins = agg.wins
        places = agg.places
        total_staked = float(agg.total_staked)
        net_profit = float(agg.net_profit)

        win_sr = wins / tips if tips else 0.0
        place_sr = places / tips if tips else 0.0
        roi = (net_profit / total_staked) if total_staked > 0 else 0.0

        stats_out.append(
            schemas.TipTypeStats(
                tip_type=tip_type,  # type: ignore[arg-type]
                tips=tips,
                wins=wins,
                places=places,
                win_strike_rate=win_sr,
                place_strike_rate=place_sr,
                total_staked=total_staked,
                net_profit=net_profit,
                roi=roi,
            )
        )

    # Sort by tip_type just for stable output (AI_BEST, DANGER, VALUE...)
    stats_out.sort(key=lambda s: s.tip_type)
    return stats_out


def _abandoned_meeting_ids(db: Session, target_date: date) -> set[str]:
    """
    Return meeting IDs for meetings on `target_date` that have NO race results
    at all. Treated as abandoned (only valid when date is in the past).
    """
    from datetime import datetime
    from zoneinfo import ZoneInfo

    today = datetime.now(ZoneInfo("Australia/Melbourne")).date()
    if target_date >= today:
        return set()  # Don't mark today/future meetings as abandoned

    # Find meetings on target_date
    meetings = db.query(models.Meeting.id).filter(models.Meeting.date == target_date).all()
    meeting_ids = {m[0] for m in meetings}

    if not meeting_ids:
        return set()

    # Find meetings that have AT LEAST ONE race result with finish_position set
    meetings_with_results = (
        db.query(models.Meeting.id)
        .join(models.Race, models.Race.meeting_id == models.Meeting.id)
        .join(models.RaceResult, models.RaceResult.race_id == models.Race.id)
        .filter(models.Meeting.date == target_date)
        .filter(models.RaceResult.finish_position.isnot(None))
        .distinct()
        .all()
    )
    with_results_ids = {m[0] for m in meetings_with_results}

    # Abandoned = on date but with no results
    return meeting_ids - with_results_ids


def compute_day_stats(
    db: Session,
    target_date: date,
    provider: str = "RA",
    stake_per_tip: float = 10.0,
    track_name: Optional[str] = None,
    state: Optional[str] = None,
) -> schemas.DayStatsOut:
    """
    Compute per-tip-type stats for a given date.

    Optionally filter to a specific meeting by track_name / state.
    Excludes abandoned meetings (past date with zero results).
    """
    abandoned = _abandoned_meeting_ids(db, target_date)

    q = (
        db.query(models.Tip, models.TipOutcome, models.Meeting)
        .join(models.TipRun, models.Tip.tip_run_id == models.TipRun.id)
        .join(models.Race, models.Tip.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .outerjoin(
            models.TipOutcome,
            (models.TipOutcome.tip_id == models.Tip.id)
            & (models.TipOutcome.provider == provider),
        )
        .filter(models.Meeting.date == target_date)
        .filter(models.TipRun.source == settings.tips_default_source)
    )

    if abandoned:
        q = q.filter(~models.Meeting.id.in_(abandoned))
    if track_name:
        q = q.filter(models.Meeting.track_name == track_name)
    if state:
        q = q.filter(models.Meeting.state == state)

    rows = q.all()

    if not rows:
        return schemas.DayStatsOut(
            date=target_date,
            provider=provider,
            stake_per_tip=stake_per_tip,
            stats=[],
        )

    aggs = _aggregate_rows(rows, provider=provider, stake_per_tip=stake_per_tip)
    stats_out = _aggs_to_tip_type_stats(aggs)

    return schemas.DayStatsOut(
        date=target_date,
        provider=provider,
        stake_per_tip=stake_per_tip,
        stats=stats_out,
    )


def compute_range_stats(
    db: Session,
    date_from: date,
    date_to: date,
    provider: str = "RA",
    stake_per_tip: float = 10.0,
    track_name: Optional[str] = None,
    state: Optional[str] = None,
) -> schemas.RangeStatsOut:
    """
    Compute per-tip-type stats across a date range [date_from, date_to].

    Optional filters:
      - track_name: limit to one track (e.g. only Flemington)
      - state: limit to a state (e.g. only VIC meetings)
    """
    # Exclude abandoned meetings across the range
    from datetime import timedelta
    abandoned: set[str] = set()
    cur = date_from
    while cur <= date_to:
        abandoned |= _abandoned_meeting_ids(db, cur)
        cur += timedelta(days=1)

    q = (
        db.query(models.Tip, models.TipOutcome, models.Meeting)
        .join(models.TipRun, models.Tip.tip_run_id == models.TipRun.id)
        .join(models.Race, models.Tip.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .outerjoin(
            models.TipOutcome,
            (models.TipOutcome.tip_id == models.Tip.id)
            & (models.TipOutcome.provider == provider),
        )
        .filter(models.Meeting.date >= date_from)
        .filter(models.Meeting.date <= date_to)
        .filter(models.TipRun.source == settings.tips_default_source)
    )

    if abandoned:
        q = q.filter(~models.Meeting.id.in_(abandoned))
    if track_name:
        q = q.filter(models.Meeting.track_name == track_name)
    if state:
        q = q.filter(models.Meeting.state == state)

    rows = q.all()

    if not rows:
        return schemas.RangeStatsOut(
            date_from=date_from,
            date_to=date_to,
            provider=provider,
            stake_per_tip=stake_per_tip,
            stats=[],
        )

    aggs = _aggregate_rows(rows, provider=provider, stake_per_tip=stake_per_tip)
    stats_out = _aggs_to_tip_type_stats(aggs)

    return schemas.RangeStatsOut(
        date_from=date_from,
        date_to=date_to,
        provider=provider,
        stake_per_tip=stake_per_tip,
        stats=stats_out,
    )
