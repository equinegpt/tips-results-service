# app/results_daily_job.py
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, Tuple

from zoneinfo import ZoneInfo
from sqlalchemy.orm import Session

from .database import SessionLocal
from .models import Meeting, Race, Tip, RaceResult, TipOutcome
from .ra_results_client import RAResultsClient, RAResultRow


# ---------- helpers ----------


def _today_melb() -> date:
    return datetime.now(ZoneInfo("Australia/Melbourne")).date()


def _canonical_track_name(raw: str) -> str:
    """
    Canonicalise track names so RA (crawler) and PF (Meetings in this DB)
    line up.

    This mirrors the logic we used in the RA crawler's meeting_id backfill.
    """
    import re

    if not raw:
        return ""

    s = raw.strip().lower()
    s = re.sub(r"[-,/]", " ", s)  # normalise separators
    s = re.sub(r"\s+", " ", s)    # collapse spaces

    # Strip sponsors / fluff
    sponsors = [
        "sportsbet",
        "ladbrokes",
        "bet365",
        "picklebet",
        "thomas farms",
        "aquis park",
        "aquis",
        "tabtouch",
        "tab ",
    ]
    for sp in sponsors:
        s = s.replace(sp, "")

    junk_words = [
        "rc",
        "racecourse",
        "raceway",
        "race club",
        "race club inc",
        "race club incorporated",
        "park",
        "gh",
    ]
    for jw in junk_words:
        s = s.replace(f" {jw} ", " ")
        if s.endswith(f" {jw}"):
            s = s[: -len(f" {jw}")]
        if s.startswith(f"{jw} "):
            s = s[len(f"{jw} "):]

    s = " ".join(s.split())

    # Southside variants → plain track
    if s.startswith("southside cranbourne"):
        return "cranbourne"
    if s.startswith("southside pakenham"):
        return "pakenham"

    # Yarra / Yarra Valley
    if "yarra valley" in s:
        return "yarra glen"

    # Port Lincoln sponsor variants
    if "port lincoln" in s:
        return "port lincoln"

    # Mt/Mount normalisation
    if s.startswith("mt "):
        s = "mount " + s[len("mt "):]

    # Darwin / Fannie Bay quirks:
    # RA may use "Darwin" in program; PF calls it "Fannie Bay".
    if s == "darwin":
        return "fannie bay"

    return s


def _status_from_result(rr: RAResultRow) -> str:
    if rr.is_scratched:
        return "SCRATCHED"
    if rr.finishing_pos is None:
        return "NO_RESULT"
    return "RUN"


def _tip_outcome_status(rr: RAResultRow) -> str:
    if rr.is_scratched:
        return "SCRATCHED"
    if rr.finishing_pos is None:
        return "NO_RESULT"
    if rr.finishing_pos == 1:
        return "WIN"
    if rr.finishing_pos in (2, 3):
        return "PLACE"
    return "LOSE"


# ---------- core wiring ----------


def _build_meeting_race_maps(
    db: Session, d: date
) -> Tuple[Dict[tuple[str, str], Meeting], Dict[tuple[str, int], Race]]:
    """
    For a given date, build:

      meetings[(STATE, canon_track)] -> Meeting
      races[(meeting_id, race_number)] -> Race
    """
    meetings = db.query(Meeting).filter(Meeting.date == d).all()

    meeting_map: Dict[tuple[str, str], Meeting] = {}
    for m in meetings:
        key = (m.state.upper(), _canonical_track_name(m.track_name))
        meeting_map[key] = m

    if not meetings:
        return meeting_map, {}

    races = (
        db.query(Race)
        .filter(Race.meeting_id.in_([m.id for m in meetings]))
        .all()
    )
    race_map: Dict[tuple[str, int], Race] = {}
    for r in races:
        race_map[(r.meeting_id, r.race_number)] = r

    return meeting_map, race_map


def _apply_results_for_date(
    db: Session,
    d: date,
    ra_rows: Iterable[RAResultRow],
) -> None:
    meeting_map, race_map = _build_meeting_race_maps(db, d)

    upserted_results = 0
    updated_outcomes = 0

    for rr in ra_rows:
        m_key = (rr.state.upper(), _canonical_track_name(rr.track))
        meeting = meeting_map.get(m_key)
        if not meeting:
            # You'll see these in Render logs if our canon mapping misses something,
            # or if we simply never stored a Meeting row for that track/date.
            print(
                f"[results_daily_job] WARN no Meeting match for {d} "
                f"{rr.state} '{rr.track}' (canon='{m_key[1]}')"
            )
            continue

        race = race_map.get((meeting.id, rr.race_no))
        if not race:
            print(
                f"[results_daily_job] WARN no Race match for {d} {rr.state} "
                f"{rr.track} R{rr.race_no}"
            )
            continue

        # ----- Upsert RaceResult -----
        result = (
            db.query(RaceResult)
            .filter(
                RaceResult.provider == "RA",
                RaceResult.race_id == race.id,
                RaceResult.tab_number == rr.tab_number,
            )
            .one_or_none()
        )

        if result is None:
            result = RaceResult(
                provider="RA",
                race_id=race.id,
                tab_number=rr.tab_number,
                horse_name=rr.horse_name,
            )
            db.add(result)
        else:
            result.horse_name = rr.horse_name

        result.finish_position = rr.finishing_pos
        result.status = _status_from_result(rr)
        result.margin_text = (
            f"{rr.margin_lens:.2f}L" if rr.margin_lens is not None else None
        )
        result.starting_price = rr.starting_price
        upserted_results += 1

        # ----- Wire into TipOutcome for any Tips on this runner -----
        tips = (
            db.query(Tip)
            .filter(
                Tip.race_id == race.id,
                Tip.tab_number == rr.tab_number,
            )
            .all()
        )

        for tip in tips:
            # PK is tip_id; use Session.get to avoid legacy warning
            outcome = db.get(TipOutcome, tip.id)
            if outcome is None:
                outcome = TipOutcome(
                    tip_id=tip.id,
                    provider="RA",
                    race_result=result,
                )
                db.add(outcome)
            else:
                outcome.provider = "RA"
                outcome.race_result = result

            outcome.finish_position = rr.finishing_pos
            outcome.outcome_status = _tip_outcome_status(rr)
            outcome.starting_price = rr.starting_price
            updated_outcomes += 1

    print(
        f"[results_daily_job] Applied results for {d}: "
        f"RaceResult upserts={upserted_results}, TipOutcome updates={updated_outcomes}"
    )


# ---------- runners ----------


def run_results_for_window(date_from: date, date_to: date) -> None:
    """
    Apply RA results for all days in [date_from, date_to] inclusive.

    Safe to run multiple times – RaceResult upsert is idempotent and
    TipOutcome gets updated in place.
    """
    if date_to < date_from:
        print(
            f"[results_daily_job] WARN date_to ({date_to}) < date_from ({date_from}); "
            f"nothing to do."
        )
        return

    client = RAResultsClient()
    db: Session = SessionLocal()

    try:
        d = date_from
        while d <= date_to:
            print(f"[results_daily_job] Fetching RA results for {d}")
            rows = client.fetch_results_for_date(d)
            print(
                f"[results_daily_job] {d}: received {len(rows)} row(s) "
                f"from RA crawler"
            )
            if rows:
                _apply_results_for_date(db, d, rows)
                db.commit()
            d += timedelta(days=1)
    finally:
        db.close()
        client.close()

    print(f"[results_daily_job] Done window {date_from} → {date_to}")


def run_results_daily() -> None:
    """
    Default behaviour for cron (scripts/results_daily.sh):
    process yesterday + today (Melbourne time) so late results
    are still picked up.
    """
    today = _today_melb()
    date_from = today - timedelta(days=1)
    date_to = today
    run_results_for_window(date_from, date_to)


# ---------- CLI entrypoint ----------


def main() -> int:
    """
    Usage:

      # normal daily behaviour (yesterday + today; used by cron)
      python -m app.results_daily_job

      # manual backfill for a window (inclusive)
      python -m app.results_daily_job 2025-11-21 2025-11-27
    """
    if len(sys.argv) == 3:
        date_from = date.fromisoformat(sys.argv[1])
        date_to = date.fromisoformat(sys.argv[2])
        run_results_for_window(date_from, date_to)
    else:
        run_results_daily()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
