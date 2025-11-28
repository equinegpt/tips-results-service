# app/results_daily_job.py
from __future__ import annotations

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
    s = re.sub(r"[-,/]", " ", s)          # normalise separators
    s = re.sub(r"\s+", " ", s)            # collapse spaces

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
        s = "mount " + s[len("mt ") :]

    # Darwin / Fannie Bay quirks:
    # RA uses "Darwin" in program; PF calls it "Fannie Bay".
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

def _build_meeting_race_maps(db: Session, d: date) -> Tuple[
    Dict[tuple[str, str], Meeting],
    Dict[tuple[str, int], Race],
]:
    """
    For a given date, build:

      meetings[(STATE, canon_track)] -> Meeting
      races[(meeting_id, race_number)] -> Race
    """
    meetings = (
        db.query(Meeting)
        .filter(Meeting.date == d)
        .all()
    )

    meeting_map: Dict[tuple[str, str], Meeting] = {}
    for m in meetings:
        key = (m.state.upper(), _canonical_track_name(m.track_name))
        meeting_map[key] = m

    races = (
        db.query(Race)
        .filter(Race.meeting_id.in_([m.id for m in meetings]))
        .all()
    )
    race_map: Dict[tuple[str, int], Race] = {}
    for r in races:
        race_map[(r.meeting_id, r.race_number)] = r

    return meeting_map, race_map


def _apply_results_for_date(db: Session, d: date, ra_rows: Iterable[RAResultRow]) -> None:
    meeting_map, race_map = _build_meeting_race_maps(db, d)

    upserted_results = 0
    updated_outcomes = 0

    for rr in ra_rows:
        m_key = (rr.state.upper(), _canonical_track_name(rr.track))
        meeting = meeting_map.get(m_key)
        if not meeting:
            # You'll see these in Render logs if our canon mapping misses something.
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
            # Primary key is tip_id
            outcome = db.query(TipOutcome).get(tip.id)
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


def run() -> None:
    """
    Entry point for cron (and scripts/results_daily.sh).

    We process yesterday + today (Melbourne time) so late results
    are still picked up.
    """
    today = _today_melb()
    dates = [today - timedelta(days=1), today]

    client = RAResultsClient()
    try:
        for d in dates:
            print(f"[results_daily_job] Fetching RA results for {d}")
            rows = client.fetch_results_for_date(d)
            print(
                f"[results_daily_job] {d}: received {len(rows)} row(s) "
                f"from RA crawler"
            )
            if not rows:
                continue

            db: Session = SessionLocal()
            try:
                _apply_results_for_date(db, d, rows)
                db.commit()
            finally:
                db.close()
    finally:
        client.close()

    print("[results_daily_job] Done.")


if __name__ == "__main__":
    run()
