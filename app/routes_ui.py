# app/routes_ui.py
from datetime import date as date_type
from typing import Any, Optional, Dict, List, Tuple

from fastapi import APIRouter, Depends, Query
from fastapi.responses import HTMLResponse
from starlette.requests import Request
from sqlalchemy.orm import Session

from .database import get_db
from . import models
from .daily_generator import today_mel
from .ui_helpers import (
    templates,
    display_horse_name,
    display_reason,
    format_pretty_date,
)
from .ra_results_client import RAResultsClient  # 👈 NEW

router = APIRouter()


def _classify_outcome(pos_fin: Optional[int]) -> str:
    """
    Simple WIN/PLACE/LOSE classifier based on finishing position.
    Mirrors pf_results._classify_outcome.
    """
    if pos_fin is None or pos_fin <= 0:
        return "UNKNOWN"
    if pos_fin == 1:
        return "WIN"
    if pos_fin in (2, 3):
        return "PLACE"
    return "LOSE"


@router.get("/ui/day", response_class=HTMLResponse)
def ui_day(
    request: Request,
    meeting_date: date_type = Query(default_factory=today_mel, alias="date"),
    db: Session = Depends(get_db),
):
    """
    HTML view of tips for a given date.

    - Uses TipOutcome when present (PF import).
    - Falls back to RaceResult by (track_name, state, race_number, tab_number)
      so RA-only days still populate PLACING / RESULT / SP.
    - Builds per-meeting and per-day summaries:
        • tips, wins, strike rate
        • turnover, return, P&L
        • quinellas:
            any two of (AI_BEST, DANGER, VALUE) fill 1st & 2nd (any order)
        • trifectas:
            AI_BEST, DANGER & VALUE fill 1st/2nd/3rd in any order
    """
    STAKE_PER_UNIT = 10.0  # $10 per stake unit

    # -----------------------
    # 1) All tip runs for this date
    # -----------------------
    q = db.query(models.TipRun).join(models.Meeting)
    q = q.filter(models.Meeting.date == meeting_date)
    tip_runs = q.all()
    print(f"[UI] tip_runs for {meeting_date}: {len(tip_runs)}")

    # -----------------------
    # 2) Build RaceResult index for this date
    #    key = (track_name, state, race_number, tab_number)
    #    normalised: strip spaces, uppercase state.
    #    Prefer PF provider over others.
    # -----------------------
    race_results_index: dict[tuple[str, str, int, int], Any] = {}

    rr_rows = (
        db.query(models.RaceResult, models.Race, models.Meeting)
        .join(models.Race, models.RaceResult.race_id == models.Race.id)
        .join(models.Meeting, models.Race.meeting_id == models.Meeting.id)
        .filter(models.Meeting.date == meeting_date)
        .all()
    )
    print(f"[UI] raw RaceResult rows for {meeting_date}: {len(rr_rows)}")

    for rr, race, meeting in rr_rows:
        track = (meeting.track_name or "").strip()
        state = (meeting.state or "").strip().upper()
        key = (track, state, race.race_number, rr.tab_number)

        existing = race_results_index.get(key)
        if existing is None:
            race_results_index[key] = rr
        else:
            # Prefer PF over RA/other providers
            if getattr(existing, "provider", None) != "PF" and rr.provider == "PF":
                race_results_index[key] = rr

    print(f"[UI] race_results_index size (PF/DB) = {len(race_results_index)}")
    for k, v in list(race_results_index.items())[:10]:
        print(
            f"[UI] RR index sample {k} -> "
            f"pos={v.finish_position}, sp={v.starting_price}, provider={v.provider}"
        )

    # -----------------------
    # 2b) Supplement index with RA Crawler /results for this date
    #     (fallback for RA-only days; does NOT override PF rows)
    # -----------------------
    ra_rows = []
    try:
        ra_client = RAResultsClient()
        try:
            ra_rows = ra_client.fetch_results_for_date(meeting_date)
            print(f"[UI] RAResultsClient rows for {meeting_date}: {len(ra_rows)}")
        finally:
            ra_client.close()
    except Exception as e:
        print(f"[UI] error fetching RA results for {meeting_date}: {e}")
        ra_rows = []

    class _RAStub:
        __slots__ = ("finish_position", "starting_price", "provider")

        def __init__(self, finish_position, starting_price, provider: str):
            self.finish_position = finish_position
            self.starting_price = starting_price
            self.provider = provider

    for ra in ra_rows:
        track = (ra.track or "").strip()
        state = (ra.state or "").strip().upper()
        key = (track, state, ra.race_no, ra.tab_number)

        # Never override an existing result (especially PF)
        if key in race_results_index:
            continue

        race_results_index[key] = _RAStub(
            finish_position=ra.finishing_pos,
            starting_price=ra.starting_price,
            provider="RA",
        )

    print(f"[UI] race_results_index size after RA fallback = {len(race_results_index)}")
    for k, v in list(race_results_index.items())[:10]:
        print(
            f"[UI] RR+RA index sample {k} -> "
            f"pos={v.finish_position}, sp={v.starting_price}, provider={v.provider}"
        )

    # -----------------------
    # 3) Pull any TipOutcome rows for these tips
    # -----------------------
    tip_ids: list[str] = [t.id for tr in tip_runs for t in tr.tips]
    outcomes_by_tip_id: dict[str, dict[str, Any]] = {}
    print(f"[UI] total tips for {meeting_date}: {len(tip_ids)}")

    if tip_ids:
        rows = (
            db.query(models.TipOutcome, models.RaceResult)
            .outerjoin(
                models.RaceResult,
                models.TipOutcome.race_result_id == models.RaceResult.id,
            )
            .filter(models.TipOutcome.tip_id.in_(tip_ids))
            .all()
        )
        print(f"[UI] TipOutcome rows fetched: {len(rows)}")

        for outcome, rr in rows:
            placing = outcome.finish_position or (rr.finish_position if rr else None)
            sp_src = outcome.starting_price or (rr.starting_price if rr else None)
            sp_val = float(sp_src) if sp_src is not None else None

            outcomes_by_tip_id[outcome.tip_id] = {
                "placing": placing,
                "result": outcome.outcome_status,
                "sp": sp_val,
            }

    # -----------------------
    # 4) Build meeting → races → tips structure for template
    #    and compute meeting + day summaries.
    # -----------------------
    meetings_data: list[dict[str, Any]] = []

    day_totals = {
        "tips": 0,
        "wins": 0,
        "turnover": 0.0,
        "return": 0.0,
        "quinellas": 0,
        "trifectas": 0,
    }

    for tr in tip_runs:
        meeting = tr.meeting
        races_block: list[dict[str, Any]] = []

        mt_track = (meeting.track_name or "").strip()
        mt_state = (meeting.state or "").strip().upper()

        # Per-meeting accumulators
        mt_stats = {
            "tips": 0,
            "wins": 0,
            "turnover": 0.0,
            "return": 0.0,
            "quinellas": 0,
            "trifectas": 0,
        }

        # Only include races that actually have tips for this TipRun
        for race in sorted(meeting.races, key=lambda r: r.race_number):
            race_tips = [t for t in tr.tips if t.race_id == race.id]
            if not race_tips:
                continue

            tips_rows: list[dict[str, Any]] = []

            # For quinella & trifecta detection
            ai_best_placing: Optional[int] = None
            danger_placing: Optional[int] = None
            value_placing: Optional[int] = None

            for tip in sorted(race_tips, key=lambda t: (t.tip_type, t.tab_number)):
                horse = display_horse_name(tip)
                reasoning = display_reason(tip)

                # Start with TipOutcome if present
                outcome = outcomes_by_tip_id.get(tip.id, {})
                placing: Optional[int] = outcome.get("placing")
                result = outcome.get("result")
                sp = outcome.get("sp")

                # Fallback: look up by metadata key in RaceResult index
                rr_key = (mt_track, mt_state, race.race_number, tip.tab_number)
                rr = race_results_index.get(rr_key)
                debug_hit = rr is not None

                if rr is not None:
                    if placing is None:
                        placing = rr.finish_position
                    if sp is None and rr.starting_price is not None:
                        sp = float(rr.starting_price)
                    if result is None:
                        result = _classify_outcome(rr.finish_position)

                # --- meeting-level stats (tips, wins, turnover, return) ---
                units = float(getattr(tip, "stake_units", 1.0) or 1.0)
                stake_dollars = units * STAKE_PER_UNIT

                mt_stats["tips"] += 1
                mt_stats["turnover"] += stake_dollars

                # Winner = placing == 1
                if placing == 1:
                    mt_stats["wins"] += 1
                    if sp is not None:
                        mt_stats["return"] += stake_dollars * float(sp)

                # Track placings for quinella / trifecta
                if tip.tip_type == "AI_BEST":
                    ai_best_placing = placing
                elif tip.tip_type == "DANGER":
                    danger_placing = placing
                elif tip.tip_type == "VALUE":
                    value_placing = placing

                print(
                    f"[UI] tip {mt_track} {mt_state} R{race.race_number} "
                    f"TAB #{tip.tab_number}: "
                    f"TO={'Y' if tip.id in outcomes_by_tip_id else 'N'}, "
                    f"RR={'Y' if debug_hit else 'N'}, "
                    f"placing={placing}, result={result}, sp={sp}"
                )

                tips_rows.append(
                    {
                        "id": tip.id,
                        "tip_type": tip.tip_type,
                        "tab_number": tip.tab_number,
                        "horse": horse,
                        "reason": reasoning,
                        "stake": float(units),
                        "placing": placing,   # ← maps finish_position
                        "result": result,     # ← WIN/PLACE/LOSE
                        "sp": sp,             # ← starting_price
                    }
                )

            # Quinella: any two of AI_BEST / DANGER / VALUE run 1st & 2nd (any order)
            placings_for_quinella: List[int] = []
            if ai_best_placing is not None:
                placings_for_quinella.append(ai_best_placing)
            if danger_placing is not None:
                placings_for_quinella.append(danger_placing)
            if value_placing is not None:
                placings_for_quinella.append(value_placing)

            if 1 in placings_for_quinella and 2 in placings_for_quinella:
                mt_stats["quinellas"] += 1

            # Trifecta: AI_BEST, DANGER & VALUE fill 1st/2nd/3rd in any order
            if (
                ai_best_placing is not None
                and danger_placing is not None
                and value_placing is not None
            ):
                trifecta_positions = {
                    ai_best_placing,
                    danger_placing,
                    value_placing,
                }
                # Require exactly 1,2,3 as a set
                if trifecta_positions == {1, 2, 3}:
                    mt_stats["trifectas"] += 1

            races_block.append(
                {
                    "race_number": race.race_number,
                    "race_name": race.name,
                    "distance_m": race.distance_m,
                    "tips": tips_rows,
                }
            )

        if not races_block:
            continue

        # Finalise meeting summary
        mt_tips = mt_stats["tips"]
        mt_wins = mt_stats["wins"]
        mt_turnover = mt_stats["turnover"]
        mt_return = mt_stats["return"]
        mt_quin = mt_stats["quinellas"]
        mt_trif = mt_stats["trifectas"]

        mt_strike = (mt_wins * 100.0 / mt_tips) if mt_tips > 0 else 0.0
        mt_pnl = mt_return - mt_turnover

        meeting_summary = {
            "tips": mt_tips,
            "wins": mt_wins,
            "strike_rate_pct": mt_strike,
            "turnover": mt_turnover,
            "return": mt_return,
            "pnl": mt_pnl,
            "quinellas": mt_quin,
            "trifectas": mt_trif,
        }

        print(
            f"[UI] MEETING {meeting.track_name} {meeting.state}: "
            f"tips={mt_tips}, wins={mt_wins}, strike%={mt_strike:.1f}, "
            f"turnover={mt_turnover:.0f}, return={mt_return:.0f}, pnl={mt_pnl:.0f}, "
            f"quinellas={mt_quin}, trifectas={mt_trif}"
        )

        # Roll up into day totals
        day_totals["tips"] += mt_tips
        day_totals["wins"] += mt_wins
        day_totals["turnover"] += mt_turnover
        day_totals["return"] += mt_return
        day_totals["quinellas"] += mt_quin
        day_totals["trifectas"] += mt_trif

        meetings_data.append(
            {
                "meeting": meeting,
                "tip_run": tr,
                "races": races_block,
                "summary": meeting_summary,  # used by template as Meeting summary
            }
        )

    # Sort meetings by state then track
    meetings_data.sort(key=lambda m: (m["meeting"].state, m["meeting"].track_name))

    # -----------------------
    # 5) Day-level summary
    # -----------------------
    if day_totals["tips"] > 0:
        day_strike = day_totals["wins"] * 100.0 / day_totals["tips"]
    else:
        day_strike = 0.0

    day_turnover = day_totals["turnover"]
    day_return = day_totals["return"]
    day_pnl = day_return - day_turnover

    day_summary = {
        "tips": day_totals["tips"],
        "wins": day_totals["wins"],
        "strike_rate_pct": day_strike,
        "turnover": day_turnover,
        "return": day_return,
        "pnl": day_pnl,
        "quinellas": day_totals["quinellas"],
        "trifectas": day_totals["trifectas"],
    }

    print(
        f"[UI] DAY {meeting_date}: tips={day_summary['tips']}, "
        f"wins={day_summary['wins']}, strike%={day_strike:.1f}, "
        f"turnover={day_turnover:.0f}, return={day_return:.0f}, pnl={day_pnl:.0f}, "
        f"quinellas={day_summary['quinellas']}, "
        f"trifectas={day_summary['trifectas']}"
    )

    return templates.TemplateResponse(
        "day.html",
        {
            "request": request,
            "date": meeting_date,
            "display_date": format_pretty_date(meeting_date),
            "meetings": meetings_data,
            "day_summary": day_summary,
        },
    )
