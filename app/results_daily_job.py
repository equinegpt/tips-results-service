# app/results_daily_job.py
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session

from app.ra_results_client import fetch_results_for_date, RAResultRow
from app.database import SessionLocal
from app.models import (
    Meeting,
    Race,
    Tip,
    RaceResult,
    TipOutcome,
)


def _normalise_track(name: str) -> str:
    """Simple normalisation to improve matching."""
    return name.strip().lower()


def _determine_outcome_status(
    finishing_pos: int | None,
    is_scratched: bool,
) -> str:
    """
    Map RA finishing position + scratch flag to our TipOutcome.outcome_status.
    """
    if is_scratched:
        return "SCRATCHED"
    if finishing_pos is None:
        return "NO_RESULT"

    if finishing_pos == 1:
        return "WIN"
    if finishing_pos in (2, 3):
        return "PLACE"
    return "LOSE"


def _upsert_results_for_date(db: Session, d: date) -> None:
    """
    For a given date:
      - fetch RA results via ra-crawler
      - upsert RaceResult rows
      - upsert TipOutcome rows for any matching Tip
    """
    print(f"[results_daily_job] Fetching RA results for {d}")
    ra_rows: list[RAResultRow] = fetch_results_for_date(d)
    print(f"[results_daily_job] {d}: received {len(ra_rows)} rows from RA crawler")

    if not ra_rows:
        return

    # Preload meetings for that date into a lookup by (state, track_normalised)
    meetings: list[Meeting] = (
        db.query(Meeting).filter(Meeting.date == d).all()
    )
    meeting_index: dict[tuple[str, str], Meeting] = {}
    for m in meetings:
        key = (m.state.upper(), _normalise_track(m.track_name))
        meeting_index[key] = m

    print(
        f"[results_daily_job] {d}: loaded {len(meeting_index)} meeting(s) for that date from DB"
    )

    # Preload races per meeting to avoid N+1 queries
    races_by_meeting_and_no: dict[tuple[str, int], Race] = {}
    if meetings:
        meeting_ids = [m.id for m in meetings]
        races: list[Race] = (
            db.query(Race)
            .filter(Race.meeting_id.in_(meeting_ids))
            .all()
        )
        for r in races:
            key = (r.meeting_id, r.race_number)
            races_by_meeting_and_no[key] = r

        print(
            f"[results_daily_job] {d}: loaded {len(races)} race(s) for those meetings"
        )

    # Process each RA result row
    for rr in ra_rows:
        # 1) Find Meeting
        m_key = (rr.state.upper(), _normalise_track(rr.track))
        meeting = meeting_index.get(m_key)
        if meeting is None:
            # We may not have meetings for all RA tracks (e.g. no tips at that meeting)
            print(
                f"[results_daily_job] {d}: no Meeting match for "
                f"{rr.state}/{rr.track} – skipping row (race_no={rr.race_no}, "
                f"TAB={rr.horse_number})"
            )
            continue

        # 2) Find Race
        r_key = (meeting.id, rr.race_no)
        race = races_by_meeting_and_no.get(r_key)
        if race is None:
            print(
                f"[results_daily_job] {d}: no Race match for meeting={meeting.id}, "
                f"race_no={rr.race_no} – skipping row (TAB={rr.horse_number})"
            )
            continue

        # 3) Upsert RaceResult
        existing_rr: RaceResult | None = (
            db.query(RaceResult)
            .filter(
                RaceResult.provider == "RA",
                RaceResult.race_id == race.id,
                RaceResult.tab_number == rr.horse_number,
            )
            .one_or_none()
        )

        sp_decimal: Decimal | None = None
        if rr.starting_price is not None:
            try:
                sp_decimal = Decimal(str(rr.starting_price))
            except Exception:
                sp_decimal = None

        status = _determine_outcome_status(
            finishing_pos=rr.finishing_pos,
            is_scratched=rr.is_scratched,
        )

        if existing_rr is None:
            existing_rr = RaceResult(
                provider="RA",
                race_id=race.id,
                tab_number=rr.horse_number,
                horse_name=rr.horse_name,
                finish_position=rr.finishing_pos,
                status=status,
                margin_text=(
                    f"{rr.margin_lens:.2f}L" if rr.margin_lens is not None else None
                ),
                starting_price=sp_decimal,
            )
            db.add(existing_rr)
        else:
            existing_rr.horse_name = rr.horse_name
            existing_rr.finish_position = rr.finishing_pos
            existing_rr.status = status
            existing_rr.margin_text = (
                f"{rr.margin_lens:.2f}L" if rr.margin_lens is not None else None
            )
            existing_rr.starting_price = sp_decimal

        # 4) Upsert TipOutcome for any Tips on this race + TAB
        tips_for_runner: list[Tip] = (
            db.query(Tip)
            .filter(
                Tip.race_id == race.id,
                Tip.tab_number == rr.horse_number,
            )
            .all()
        )

        if not tips_for_runner:
            continue

        for tip in tips_for_runner:
            outcome: TipOutcome | None = (
                db.query(TipOutcome)
                .filter(
                    TipOutcome.tip_id == tip.id,
                    TipOutcome.provider == "RA",
                )
                .one_or_none()
            )

            if outcome is None:
                outcome = TipOutcome(
                    tip_id=tip.id,
                    provider="RA",
                    race_result_id=existing_rr.id,
                    finish_position=rr.finishing_pos,
                    outcome_status=status,
                    starting_price=sp_decimal,
                )
                db.add(outcome)
            else:
                outcome.race_result_id = existing_rr.id
                outcome.finish_position = rr.finishing_pos
                outcome.outcome_status = status
                outcome.starting_price = sp_decimal


def main() -> int:
    # For now: do yesterday + today. For testing, you can swap to a fixed date.
    today = date.today()
    yesterday = today - timedelta(days=1)

    with SessionLocal() as db:
        for d in (yesterday, today):
            try:
                _upsert_results_for_date(db, d)
                db.commit()
            except Exception as exc:
                db.rollback()
                print(f"[results_daily_job] ERROR processing {d}: {exc}")

    print("[results_daily_job] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
