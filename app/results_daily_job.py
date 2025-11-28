# results_daily_job.py
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models import Meeting, Race, Tip, RaceResult, TipOutcome
from ra_results_client import RAResultsClient


# ---------- Helpers ----------

def _today_melb() -> date:
    return datetime.now(ZoneInfo("Australia/Melbourne")).date()


def _get_field(row: Dict[str, Any], *names: str, default: Any = None) -> Any:
    """
    Convenience helper: try several key names (snake/camel) on a row.
    """
    for n in names:
        if n in row:
            return row[n]
    return default


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


# ---------- Core logic ----------

def _upsert_race_result(
    session: Session,
    race_id: str,
    row: Dict[str, Any],
) -> RaceResult:
    """
    Take one RA result row (from RA-crawler) and upsert into RaceResult.
    We key by (provider='RA', race_id, tab_number).
    """

    tab_number = _get_field(row, "tab_number", "tabNumber")
    if tab_number is None:
        raise ValueError(f"Missing tab_number in RA result row: {row!r}")

    horse_name = _get_field(row, "horse_name", "horseName", default="Unknown")
    finish_pos = _get_field(row, "finishing_pos", "finishPosition")
    is_scratched = bool(_get_field(row, "is_scratched", "scratched", default=False))
    margin = _get_field(row, "margin_lens", "margin")
    sp_raw = _get_field(row, "starting_price", "startingPrice")

    rr: RaceResult | None = (
        session.query(RaceResult)
        .filter(
            RaceResult.provider == "RA",
            RaceResult.race_id == race_id,
            RaceResult.tab_number == int(tab_number),
        )
        .one_or_none()
    )

    if rr is None:
        rr = RaceResult(
            provider="RA",
            race_id=race_id,
            tab_number=int(tab_number),
            horse_name=str(horse_name),
        )
        session.add(rr)

    # Update fields
    rr.horse_name = str(horse_name)

    if finish_pos is not None:
        try:
            rr.finish_position = int(finish_pos)
        except Exception:
            rr.finish_position = None
    else:
        rr.finish_position = None

    rr.status = "SCRATCHED" if is_scratched else "RUN"

    rr.margin_text = str(margin) if margin not in (None, "") else None
    rr.starting_price = _to_decimal(sp_raw)

    return rr


def _compute_outcome_from_result(rr: Optional[RaceResult]) -> Dict[str, Any]:
    """
    Given a RaceResult (or None), compute TipOutcome columns:
      - outcome_status
      - finish_position
      - starting_price
      - race_result_id
    """
    if rr is None:
        return {
            "outcome_status": "NO_RESULT",
            "finish_position": None,
            "starting_price": None,
            "race_result_id": None,
        }

    if rr.status == "SCRATCHED":
        return {
            "outcome_status": "SCRATCHED",
            "finish_position": None,
            "starting_price": rr.starting_price,
            "race_result_id": rr.id,
        }

    pos = rr.finish_position
    if pos == 1:
        status = "WIN"
    elif pos in (2, 3):
        status = "PLACE"
    elif pos is None:
        status = "NO_RESULT"
    else:
        status = "LOSE"

    return {
        "outcome_status": status,
        "finish_position": pos,
        "starting_price": rr.starting_price,
        "race_result_id": rr.id,
    }


def _process_day(session: Session, target_date: date, client: RAResultsClient) -> None:
    print(f"[results_daily_job] Fetching RA results for {target_date.isoformat()}")

    rows = client.fetch_results_for_date(target_date)
    print(f"[results_daily_job] {target_date}: received {len(rows)} row(s) from RA crawler")

    if not rows:
        return

    # 1) Build a map (meetingId, raceNo) -> Race.id in our DB
    #    so we can attach RA results to our Race rows.
    meetings = (
        session.query(Meeting)
        .filter(Meeting.date == target_date)
        .all()
    )

    meeting_by_pf_id: dict[int, Meeting] = {}
    for m in meetings:
        if m.pf_meeting_id is not None:
            meeting_by_pf_id[int(m.pf_meeting_id)] = m

    races = (
        session.query(Race)
        .filter(Race.meeting_id.in_([m.id for m in meetings]) if meetings else False)
        .all()
    )

    race_index: dict[tuple[int, int], Race] = {}  # (pf_meeting_id, race_number) -> Race
    pf_id_by_meeting: dict[str, int] = {
        m.id: int(m.pf_meeting_id) for m in meetings if m.pf_meeting_id is not None
    }
    for r in races:
        pf_mid = pf_id_by_meeting.get(r.meeting_id)
        if pf_mid is None:
            continue
        race_index[(pf_mid, int(r.race_number))] = r

    # 2) Upsert RaceResult rows from RA payload
    race_results_by_race_tab: dict[tuple[str, int], RaceResult] = {}

    for row in rows:
        # meetingId should be coming from RA-crawler /results (joined via race_program).
        meeting_id_raw = _get_field(row, "meetingId", "meeting_id")
        race_no = _get_field(row, "race_no", "raceNo")
        if meeting_id_raw is None or race_no is None:
            # Can't attach to a race; skip
            continue

        try:
            pf_meeting_id = int(meeting_id_raw)
            race_no_int = int(race_no)
        except Exception:
            continue

        race = race_index.get((pf_meeting_id, race_no_int))
        if not race:
            # Our Tips DB might not have this race (e.g. non-TAB / not in tips); skip.
            continue

        rr = _upsert_race_result(session, race.id, row)
        race_results_by_race_tab[(race.id, rr.tab_number)] = rr

    # 3) For all tips on that date, compute TipOutcome based on RaceResult
    tips = (
        session.query(Tip)
        .join(Race, Tip.race_id == Race.id)
        .join(Meeting, Race.meeting_id == Meeting.id)
        .filter(Meeting.date == target_date)
        .all()
    )

    updated_outcomes = 0
    for tip in tips:
        key = (tip.race_id, tip.tab_number)
        rr = race_results_by_race_tab.get(key)

        outcome = _compute_outcome_from_result(rr)

        tobj: TipOutcome | None = session.get(TipOutcome, tip.id)
        if tobj is None:
            tobj = TipOutcome(tip_id=tip.id)
            session.add(tobj)

        tobj.provider = "RA"
        tobj.race_result_id = outcome["race_result_id"]
        tobj.finish_position = outcome["finish_position"]
        tobj.outcome_status = outcome["outcome_status"]
        tobj.starting_price = outcome["starting_price"]

        updated_outcomes += 1

    print(
        f"[results_daily_job] {target_date}: "
        f"race_results_upserted={len(race_results_by_race_tab)}, "
        f"tip_outcomes_updated={updated_outcomes}"
    )


def run_daily() -> None:
    """
    Entry point for scripts/results_daily.sh.

    Strategy:
      - Run for today (Melbourne) and yesterday.
        (Covers late results and soft retries.)
    """
    today = _today_melb()
    days = [today - timedelta(days=1), today]

    client = RAResultsClient()
    db: Session = SessionLocal()

    try:
        for d in days:
            _process_day(db, d, client)
        db.commit()
    finally:
        db.close()


if __name__ == "__main__":
    run_daily()
