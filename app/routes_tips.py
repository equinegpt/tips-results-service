from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date as date_type
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy.orm import Session

from .database import get_db
from . import schemas, models, daily_generator
from .clients import ireel_client, gemini_client
from .config import settings

router = APIRouter()


def _meeting_has_tips(
    db: Session,
    meeting_date: date_type,
    track_name: str,
    state: str,
    pf_meeting_id: Optional[int] = None,
    source: Optional[str] = None,
) -> bool:
    """
    Check if tips already exist for a meeting.

    If source is provided, only checks for TipRuns from that source
    (e.g. "iReel" or "Gemini"). This allows parallel tip generation
    from multiple providers for the same meeting.
    """
    # First try to find the meeting
    meeting: models.Meeting | None = None

    # Try pf_meeting_id first if available
    if pf_meeting_id is not None:
        meeting = (
            db.query(models.Meeting)
            .filter(models.Meeting.pf_meeting_id == pf_meeting_id)
            .first()
        )

    # Fallback to natural key
    if meeting is None:
        meeting = (
            db.query(models.Meeting)
            .filter(
                models.Meeting.date == meeting_date,
                models.Meeting.track_name == track_name,
                models.Meeting.state == state,
            )
            .first()
        )

    if meeting is None:
        return False

    # Check if TipRun exists for this meeting (optionally filtered by source)
    q = db.query(models.TipRun).filter(models.TipRun.meeting_id == meeting.id)
    if source:
        q = q.filter(models.TipRun.source == source)

    return q.count() > 0


@router.post("/tips/batch", response_model=schemas.MeetingTipsOut)
def create_tips_batch(
    payload: schemas.TipsBatchIn,
    db: Session = Depends(get_db),
):
    """
    Store a batch of tips for a single meeting.

    - Upserts Meeting
    - Creates a TipRun
    - Upserts Races
    - Creates Tips for each race
    - Returns MeetingTipsOut
    """
    m = payload.meeting

    # --- Upsert Meeting (robust against existing rows) ---
    meeting: models.Meeting | None = None

    # 1) Try to match on pf_meeting_id if we have one
    if m.pf_meeting_id is not None:
        meeting = (
            db.query(models.Meeting)
            .filter(models.Meeting.pf_meeting_id == m.pf_meeting_id)
            .first()
        )

    # 2) Fallback: natural key (date, track_name, state)
    if meeting is None:
        meeting = (
            db.query(models.Meeting)
            .filter(
                models.Meeting.date == m.date,
                models.Meeting.track_name == m.track_name,
                models.Meeting.state == m.state,
            )
            .first()
        )

    if meeting is None:
        # Brand new meeting
        meeting = models.Meeting(
            date=m.date,
            track_name=m.track_name,
            state=m.state,
            country=m.country,
            pf_meeting_id=m.pf_meeting_id,
            ra_meetcode=m.ra_meetcode,
        )
        db.add(meeting)
        db.flush()
    else:
        # Update existing row in place
        meeting.country = m.country
        meeting.ra_meetcode = m.ra_meetcode
        if m.pf_meeting_id is not None and meeting.pf_meeting_id != m.pf_meeting_id:
            meeting.pf_meeting_id = m.pf_meeting_id

    # --- Create TipRun ---
    tr = payload.tip_run
    tip_run = models.TipRun(
        source=tr.source,
        model_version=tr.model_version,
        meta=tr.meta or {},
        meeting_id=meeting.id,   # link TipRun → Meeting
    )
    db.add(tip_run)
    db.flush()

    # --- Upsert Races + create Tips ---
    race_with_tips_out: list[schemas.RaceWithTipsOut] = []

    for race_in in payload.races:
        r = race_in.race

        race = (
            db.query(models.Race)
            .filter(
                models.Race.meeting_id == meeting.id,
                models.Race.race_number == r.race_number,
            )
            .first()
        )

        if not race:
            race = models.Race(
                meeting_id=meeting.id,
                race_number=r.race_number,
                name=r.name,
                distance_m=r.distance_m,
                class_text=r.class_text,
                scheduled_start=r.scheduled_start,
            )
            db.add(race)
            db.flush()
        else:
            # Light "upsert" update
            if r.name is not None:
                race.name = r.name
            if r.distance_m is not None:
                race.distance_m = r.distance_m
            if r.class_text is not None:
                race.class_text = r.class_text
            if r.scheduled_start is not None:
                race.scheduled_start = r.scheduled_start

        tip_out_list: list[schemas.TipOut] = []

        for t in race_in.tips:
            tip = models.Tip(
                race_id=race.id,
                tip_run_id=tip_run.id,
                tip_type=t.tip_type,
                tab_number=t.tab_number,
                horse_name=t.horse_name,
                reasoning=t.reasoning,
                stake_units=t.stake_units,
            )
            db.add(tip)
            db.flush()

            tip_out = schemas.TipOut.model_validate(tip)
            tip_out_list.append(tip_out)

        race_out = schemas.RaceOut.model_validate(race)
        race_with_tips_out.append(
            schemas.RaceWithTipsOut(
                race=race_out,
                tips=tip_out_list,
            )
        )

    db.commit()

    meeting_out = schemas.MeetingOut.model_validate(meeting)

    return schemas.MeetingTipsOut(
        meeting=meeting_out,
        tip_run_id=tip_run.id,
        races=race_with_tips_out,
    )


@router.post("/generate-tips", response_model=schemas.MeetingTipsOut)
def generate_tips(
    payload: schemas.GenerateTipsIn,
    db: Session = Depends(get_db),
):
    """
    Internal: for a single meeting, call iReel once per race to generate
    AI Best + Danger + Value then persist via the same logic as /tips/batch.
    """
    tip_run_in = payload.tip_run
    meeting = payload.meeting
    races_ctx = payload.races

    project_id = getattr(tip_run_in, "project_id", None)

    races_entries: list[dict[str, Any]] = []

    for race_ctx in races_ctx:
        race_in = race_ctx.race
        scratchings = race_ctx.scratchings or []
        track_condition = race_ctx.track_condition

        try:
            tip_dicts = ireel_client.generate_race_tips(
                meeting=meeting,
                race=race_in,
                scratchings=scratchings,
                track_condition=track_condition,
                project_id=project_id,
            )
        except Exception as e:
            print(
                f"[iReel] error for {meeting.track_name} "
                f"R{race_in.race_number}: {e}"
            )
            tip_dicts = []

        if not tip_dicts:
            continue

        races_entries.append(
            {
                "race": race_in,
                "tips": tip_dicts,
            }
        )

    if not races_entries:
        raise HTTPException(
            status_code=502,
            detail="No tips generated for this meeting.",
        )

    tips_batch = schemas.TipsBatchIn(
        meeting=meeting,
        tip_run=tip_run_in,
        races=races_entries,
    )

    return create_tips_batch(tips_batch, db=db)


@router.post("/cron/generate-daily-tips", response_model=schemas.CronGenerateTipsOut)
def cron_generate_daily_tips(
    date_str: str = Query(..., alias="date"),
    project_id: str = Query(...),
    skip_tracks: list[str] = Query(default_factory=list, alias="skip_track"),
    only_pf_meeting_id: int | None = Query(default=None, alias="only_pf_meeting_id"),
    db: Session = Depends(get_db),
):
    """
    Cron-style endpoint.

    Uses daily_generator.build_generate_tips_payloads_for_date(), which
    now filters to Metro ("M") and Provincial ("P") meetings only.
    Country ("C") cards are excluded here by design, and can be run
    explicitly via /cron/generate-meeting-tips.
    """
    target_date = date_type.fromisoformat(date_str)
    print(f"[CRON] Generating tips for {target_date} (project_id={project_id})")

    payloads = daily_generator.build_generate_tips_payloads_for_date(
    target_date=target_date,
    project_id=project_id,
    force_all_meetings=False,   # daily
    )
    print(f"[CRON] daily_generator returned {len(payloads)} meetings")

    # Optional: narrow to a single PF meeting (still respects M/P filtering
    # done in daily_generator).
    if only_pf_meeting_id is not None:
        payloads = [
            p for p in payloads
            if getattr(p.meeting, "pf_meeting_id", None) == only_pf_meeting_id
        ]
        print(
            f"[CRON] after only_pf_meeting_id={only_pf_meeting_id}, "
            f"{len(payloads)} meetings remain"
        )

    # Optionally skip some tracks by name
    if skip_tracks:
        payloads = [
            p for p in payloads
            if getattr(p.meeting, "track_name", None) not in skip_tracks
        ]
        print(f"[CRON] after skip_tracks={skip_tracks!r}, {len(payloads)} meetings remain")

    meetings_processed = 0
    tip_runs_created = 0
    races_with_tips = 0
    meetings_skipped = 0
    errors: list[dict[str, Any]] = []

    for payload in payloads:
        meeting = payload.meeting
        tip_run_in = payload.tip_run

        # Check if iReel tips already exist for this meeting - skip if so
        if _meeting_has_tips(
            db=db,
            meeting_date=meeting.date,
            track_name=meeting.track_name,
            state=meeting.state,
            pf_meeting_id=getattr(meeting, "pf_meeting_id", None),
            source="iReel",
        ):
            print(
                f"[CRON] SKIPPING {meeting.track_name} ({meeting.state}) on {meeting.date} "
                f"- iReel tips already exist"
            )
            meetings_skipped += 1
            continue

        races_entries: list[dict[str, Any]] = []

        for race_ctx in payload.races:
            race_in = race_ctx.race
            scratchings = race_ctx.scratchings or []
            track_condition = race_ctx.track_condition

            print(
                f"[CRON] calling iReel for "
                f"{getattr(meeting, 'track_name', meeting)} "
                f"R{getattr(race_in, 'race_number', '?')}, "
                f"scratchings={scratchings}, cond={track_condition!r}"
            )

            try:
                tip_dicts = ireel_client.generate_race_tips(
                    meeting=meeting,
                    race=race_in,
                    scratchings=scratchings,
                    track_condition=track_condition,
                    project_id=tip_run_in.project_id,
                )
            except Exception as e:
                print(
                    f"[CRON] iReel error for "
                    f"{getattr(meeting, 'track_name', meeting)} "
                    f"R{getattr(race_in, 'race_number', '?')}: {e}"
                )
                tip_dicts = []
            finally:
                # Play nice with iReel rate limits
                time.sleep(1.0)

            if not tip_dicts:
                continue

            races_entries.append({"race": race_in, "tips": tip_dicts})

        if not races_entries:
            continue

        tips_batch = schemas.TipsBatchIn(
            meeting=meeting,
            tip_run=tip_run_in,
            races=races_entries,
        )

        try:
            mt_out = create_tips_batch(tips_batch, db=db)
        except Exception as e:
            print(
                f"[CRON] create_tips_batch failed for "
                f"{meeting.track_name} ({meeting.state}) on {meeting.date}: {e}"
            )
            db.rollback()
            errors.append(
                {
                    "track_name": meeting.track_name,
                    "state": meeting.state,
                    "date": meeting.date.isoformat(),
                    "error": repr(e),
                }
            )
            continue

        meetings_processed += 1
        tip_runs_created += 1
        races_with_tips += len(mt_out.races)

    return schemas.CronGenerateTipsOut(
        ok=True,
        date=target_date.isoformat(),
        project_id=project_id,
        meetings_processed=meetings_processed,
        tip_runs_created=tip_runs_created,
        races_with_tips=races_with_tips,
        meetings_skipped=meetings_skipped,
    )


@router.post("/cron/generate-meeting-tips", response_model=schemas.MeetingTipsOut)
def cron_generate_meeting_tips(
    date_str: str = Query(..., alias="date"),
    pf_meeting_id: int = Query(..., alias="pf_meeting_id"),
    project_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    Cron-style endpoint to generate tips for a *single* PF meeting id
    on a given date.

    Example:
      POST /cron/generate-meeting-tips?date=2025-11-23&pf_meeting_id=235501&project_id=...

    Use this when you want to explicitly run a Country ("C") card or
    a specific meeting that isn't included in the default M/P sweep.
    """
    target_date = date_type.fromisoformat(date_str)
    print(
        f"[CRON] Generating tips for single meeting "
        f"pf_meeting_id={pf_meeting_id} on {target_date} "
        f"(project_id={project_id})"
    )

    # Build all payloads for the date, then pick the one with this pf_meeting_id.
    # Note: daily_generator currently filters to M/P inside; if you later
    # adjust it to take a "track types" flag, you can call the "all tracks"
    # variant from here to include Country as well.
    payloads = daily_generator.build_generate_tips_payloads_for_date(
    target_date=target_date,
    project_id=project_id,
    force_all_meetings=True,    # manual /cron override
    )

    print(f"[CRON] daily_generator returned {len(payloads)} meetings for {target_date}")

    payload = next(
        (p for p in payloads if getattr(p.meeting, "pf_meeting_id", None) == pf_meeting_id),
        None,
    )

    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"No meeting with pf_meeting_id={pf_meeting_id} on {target_date}",
        )

    meeting = payload.meeting

    # Skip if iReel tips already exist for this meeting
    if _meeting_has_tips(
        db=db,
        meeting_date=meeting.date,
        track_name=meeting.track_name,
        state=meeting.state,
        pf_meeting_id=getattr(meeting, "pf_meeting_id", None),
        source="iReel",
    ):
        print(
            f"[CRON] (single) SKIPPING {meeting.track_name} ({meeting.state}) "
            f"on {meeting.date} - iReel tips already exist"
        )
        raise HTTPException(
            status_code=409,
            detail=f"iReel tips already exist for {meeting.track_name} ({meeting.state}) on {meeting.date}",
        )

    tip_run_in = payload.tip_run
    races_entries: list[dict[str, Any]] = []

    for race_ctx in payload.races:
        race_in = race_ctx.race
        scratchings = race_ctx.scratchings or []
        track_condition = race_ctx.track_condition

        print(
            f"[CRON] (single) calling iReel for "
            f"{getattr(meeting, 'track_name', meeting)} "
            f"R{getattr(race_in, 'race_number', '?')}, "
            f"scratchings={scratchings}, cond={track_condition!r}"
        )

        try:
            tip_dicts = ireel_client.generate_race_tips(
                meeting=meeting,
                race=race_in,
                scratchings=scratchings,
                track_condition=track_condition,
                project_id=tip_run_in.project_id,
            )
        except Exception as e:
            print(
                f"[CRON] (single) iReel error for "
                f"{getattr(meeting, 'track_name', meeting)} "
                f"R{getattr(race_in, 'race_number', '?')}: {e}"
            )
            tip_dicts = []
        finally:
            # Play nice with iReel rate limits
            time.sleep(1.0)

        if not tip_dicts:
            continue

        races_entries.append({"race": race_in, "tips": tip_dicts})

    if not races_entries:
        raise HTTPException(
            status_code=502,
            detail=f"No tips generated for pf_meeting_id={pf_meeting_id} on {target_date}",
        )

    tips_batch = schemas.TipsBatchIn(
        meeting=meeting,
        tip_run=tip_run_in,
        races=races_entries,
    )

    # Re-use the existing batch path so everything is stored exactly the same
    return create_tips_batch(tips_batch, db=db)


@router.post("/cron/generate-meeting-tips-gemini", response_model=schemas.MeetingTipsOut)
def cron_generate_meeting_tips_gemini(
    date_str: str = Query(..., alias="date"),
    pf_meeting_id: int = Query(..., alias="pf_meeting_id"),
    db: Session = Depends(get_db),
):
    """
    Generate Gemini tips for a single meeting via the Stablfy API.

    Runs in parallel with iReel — stores tips with source="Gemini".
    Uses the same M/P/C meeting filtering and scratchings/conditions
    injection as the iReel endpoint.
    """
    target_date = date_type.fromisoformat(date_str)
    print(
        f"[GEMINI] Generating tips for pf_meeting_id={pf_meeting_id} "
        f"on {target_date}"
    )

    # Build payloads (same as iReel — includes scratchings + conditions)
    payloads = daily_generator.build_generate_tips_payloads_for_date(
        target_date=target_date,
        project_id="gemini",  # not used by Gemini, but required by schema
        force_all_meetings=True,
    )

    payload = next(
        (p for p in payloads if getattr(p.meeting, "pf_meeting_id", None) == pf_meeting_id),
        None,
    )

    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"No meeting with pf_meeting_id={pf_meeting_id} on {target_date}",
        )

    meeting = payload.meeting

    # Skip if Gemini tips already exist (allows iReel tips to coexist)
    if _meeting_has_tips(
        db=db,
        meeting_date=meeting.date,
        track_name=meeting.track_name,
        state=meeting.state,
        pf_meeting_id=getattr(meeting, "pf_meeting_id", None),
        source="Gemini",
    ):
        print(
            f"[GEMINI] SKIPPING {meeting.track_name} ({meeting.state}) "
            f"on {meeting.date} - Gemini tips already exist"
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"Gemini tips already exist for {meeting.track_name} "
                f"({meeting.state}) on {meeting.date}"
            ),
        )

    # Generate tips via Gemini — parallel conversations (up to 5 at a time)
    MAX_PARALLEL = 5
    races_entries: list[dict[str, Any]] = []

    def _generate_one(race_ctx):
        race_in = race_ctx.race
        scratchings = race_ctx.scratchings or []
        track_condition = race_ctx.track_condition
        race_num = getattr(race_in, "race_number", "?")

        print(
            f"[GEMINI] Generating tips for {meeting.track_name} "
            f"R{race_num}, scratchings={scratchings}, cond={track_condition!r}"
        )

        try:
            tip_dicts = gemini_client.generate_race_tips(
                meeting=meeting,
                race=race_in,
                scratchings=scratchings,
                track_condition=track_condition,
            )
        except Exception as e:
            print(f"[GEMINI] Error for {meeting.track_name} R{race_num}: {e}")
            tip_dicts = []

        return race_in, tip_dicts

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
        futures = {
            executor.submit(_generate_one, rc): rc for rc in payload.races
        }
        for future in as_completed(futures):
            race_in, tip_dicts = future.result()
            if tip_dicts:
                races_entries.append({"race": race_in, "tips": tip_dicts})

    if not races_entries:
        raise HTTPException(
            status_code=502,
            detail=(
                f"No Gemini tips generated for pf_meeting_id={pf_meeting_id} "
                f"on {target_date}"
            ),
        )

    # Store with source="Gemini" so they sit alongside iReel tips
    gemini_tip_run = schemas.TipRunIn(
        source="Gemini",
        model_version="gemini-stablfy-v1",
        meta={
            "generated_by": "cron_generate_meeting_tips_gemini",
            "pf_meeting_id": pf_meeting_id,
        },
    )

    tips_batch = schemas.TipsBatchIn(
        meeting=meeting,
        tip_run=gemini_tip_run,
        races=races_entries,
    )

    return create_tips_batch(tips_batch, db=db)


# ──────────────────────────────────────────────────────────────────────
# Gemini coverage sweep — fills race-level holes after generation
# ──────────────────────────────────────────────────────────────────────

@router.post("/cron/sweep-gemini-tips")
def cron_sweep_gemini_tips(
    date_str: str = Query(..., alias="date"),
    force_all: bool = Query(
        False,
        description=(
            "If true, sweep every meeting in scope (incl. Country). "
            "Default: same M/P scope as the daily generator."
        ),
    ),
    max_concurrent: int = Query(3, ge=1, le=10),
    db: Session = Depends(get_db),
):
    """
    Sweep all in-scope races for the date and fill any race-level holes
    in Gemini coverage.

    Source of truth = daily_generator's payload list (RA Crawler races
    + scratchings + conditions, M/P filter). Does NOT depend on iReel
    coverage as a reference.

    For each race with no Gemini tips, calls gemini_client per race and
    appends the tips to the meeting's existing Gemini TipRun (or creates
    a new run tagged as a sweep patch when none exists yet).

    Intended to run after the Gemini generation step in the daily cron
    pipeline.
    """
    target_date = date_type.fromisoformat(date_str)
    print(f"[SWEEP] Gemini sweep for {target_date} (force_all={force_all})")

    payloads = daily_generator.build_generate_tips_payloads_for_date(
        target_date=target_date,
        project_id="gemini",
        force_all_meetings=force_all,
    )

    races_expected = 0
    races_present = 0
    races_missing = 0
    races_filled = 0
    races_still_failing = 0
    errors: list[dict[str, Any]] = []
    per_meeting_report: list[dict[str, Any]] = []

    for payload in payloads:
        meeting_in = payload.meeting

        # Locate the meeting row (may not exist yet for fresh meetings)
        meeting_row: models.Meeting | None = None
        pf_id = getattr(meeting_in, "pf_meeting_id", None)
        if pf_id is not None:
            meeting_row = (
                db.query(models.Meeting)
                .filter(models.Meeting.pf_meeting_id == pf_id)
                .first()
            )
        if meeting_row is None:
            meeting_row = (
                db.query(models.Meeting)
                .filter(
                    models.Meeting.date == meeting_in.date,
                    models.Meeting.track_name == meeting_in.track_name,
                    models.Meeting.state == meeting_in.state,
                )
                .first()
            )

        # Race numbers that already have ≥1 Gemini tip across ANY Gemini TipRun
        present_race_numbers: set[int] = set()
        if meeting_row is not None:
            covered = (
                db.query(models.Race.race_number)
                .join(models.Tip, models.Tip.race_id == models.Race.id)
                .join(models.TipRun, models.Tip.tip_run_id == models.TipRun.id)
                .filter(
                    models.Race.meeting_id == meeting_row.id,
                    models.TipRun.source == "Gemini",
                )
                .distinct()
                .all()
            )
            present_race_numbers = {row[0] for row in covered}

        # Identify missing races for this meeting
        missing_race_ctxs = []
        for race_ctx in payload.races:
            races_expected += 1
            rn = getattr(race_ctx.race, "race_number", None)
            if rn is not None and rn in present_race_numbers:
                races_present += 1
            else:
                races_missing += 1
                missing_race_ctxs.append(race_ctx)

        if not missing_race_ctxs:
            continue

        print(
            f"[SWEEP] {meeting_in.track_name} ({meeting_in.state}): "
            f"{len(missing_race_ctxs)} race(s) missing Gemini tips"
        )

        def _generate_one(ctx):
            r = ctx.race
            try:
                tips = gemini_client.generate_race_tips(
                    meeting=meeting_in,
                    race=r,
                    scratchings=ctx.scratchings or [],
                    track_condition=ctx.track_condition,
                )
                return r, tips, None
            except Exception as exc:
                return r, [], str(exc)

        new_race_entries: list[tuple[Any, list[dict]]] = []
        meeting_errors: list[dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = [executor.submit(_generate_one, c) for c in missing_race_ctxs]
            for fut in as_completed(futures):
                race_in, tips, err = fut.result()
                rn = getattr(race_in, "race_number", "?")
                if err:
                    meeting_errors.append({
                        "track": meeting_in.track_name,
                        "state": meeting_in.state,
                        "race_number": rn,
                        "error": err,
                    })
                if tips:
                    new_race_entries.append((race_in, tips))
                else:
                    races_still_failing += 1

        if not new_race_entries:
            errors.extend(meeting_errors)
            per_meeting_report.append({
                "track": meeting_in.track_name,
                "state": meeting_in.state,
                "races_expected": len(payload.races),
                "races_present_before": len(present_race_numbers),
                "races_filled": 0,
                "races_still_failing": len(missing_race_ctxs),
            })
            continue

        # Upsert the Meeting row if it was new
        if meeting_row is None:
            meeting_row = models.Meeting(
                date=meeting_in.date,
                track_name=meeting_in.track_name,
                state=meeting_in.state,
                country=getattr(meeting_in, "country", None),
                pf_meeting_id=pf_id,
                ra_meetcode=getattr(meeting_in, "ra_meetcode", None),
            )
            db.add(meeting_row)
            db.flush()

        # Locate-or-create the canonical Gemini TipRun for this meeting
        gemini_run = (
            db.query(models.TipRun)
            .filter(
                models.TipRun.meeting_id == meeting_row.id,
                models.TipRun.source == "Gemini",
            )
            .order_by(models.TipRun.created_at.asc())
            .first()
        )
        if gemini_run is None:
            gemini_run = models.TipRun(
                source="Gemini",
                model_version="gemini-stablfy-v1",
                meeting_id=meeting_row.id,
                meta={
                    "generated_by": "cron_sweep_gemini_tips",
                    "is_sweep_patch": True,
                },
            )
            db.add(gemini_run)
            db.flush()
        else:
            meta = dict(gemini_run.meta or {})
            meta["sweep_patches"] = int(meta.get("sweep_patches", 0)) + 1
            meta["last_sweep_at"] = target_date.isoformat()
            gemini_run.meta = meta

        for race_in, tip_dicts in new_race_entries:
            race_row = (
                db.query(models.Race)
                .filter(
                    models.Race.meeting_id == meeting_row.id,
                    models.Race.race_number == race_in.race_number,
                )
                .first()
            )
            if race_row is None:
                race_row = models.Race(
                    meeting_id=meeting_row.id,
                    race_number=race_in.race_number,
                    name=getattr(race_in, "name", None),
                    distance_m=getattr(race_in, "distance_m", None),
                    class_text=getattr(race_in, "class_text", None),
                    scheduled_start=getattr(race_in, "scheduled_start", None),
                )
                db.add(race_row)
                db.flush()

            for t in tip_dicts:
                tip = models.Tip(
                    race_id=race_row.id,
                    tip_run_id=gemini_run.id,
                    tip_type=t.get("tip_type"),
                    tab_number=t.get("tab_number"),
                    horse_name=t.get("horse_name"),
                    reasoning=t.get("reasoning"),
                    stake_units=t.get("stake_units", 1.0),
                )
                db.add(tip)
            races_filled += 1

        db.commit()

        if meeting_errors:
            errors.extend(meeting_errors)
        per_meeting_report.append({
            "track": meeting_in.track_name,
            "state": meeting_in.state,
            "races_expected": len(payload.races),
            "races_present_before": len(present_race_numbers),
            "races_filled": len(new_race_entries),
            "races_still_failing": len(missing_race_ctxs) - len(new_race_entries),
        })

    return {
        "date": target_date.isoformat(),
        "force_all": force_all,
        "meetings_scanned": len(payloads),
        "races_expected": races_expected,
        "races_present": races_present,
        "races_missing": races_missing,
        "races_filled": races_filled,
        "races_still_failing": races_still_failing,
        "meetings": per_meeting_report,
        "errors": errors,
    }


# ──────────────────────────────────────────────────────────────────────
# Clone + Gemini thin-prompt endpoint
# ──────────────────────────────────────────────────────────────────────

CLONE_API_URL = "https://stablfy-social.onrender.com/api/clone"


def _fetch_clone_picks_for_meeting(
    target_date: date_type,
    pf_meeting_id: int,
) -> dict[int, list[dict]]:
    """
    Fetch clone runner data from stablfy-social and return top-3 picks
    per race for the given meeting.

    Returns {race_number: [{tab_number, horse_name, role, clone_price}]}
    """
    import httpx

    resp = httpx.get(
        CLONE_API_URL,
        params={"date": target_date.isoformat()},
        timeout=30.0,
    )
    resp.raise_for_status()
    data = resp.json()
    runners = data.get("runners", [])

    # Filter to this meeting
    meeting_runners = [
        r for r in runners
        if r.get("meeting_id") == pf_meeting_id
    ]

    # Group by race_number, sort by clone_rank, take top 3
    by_race: dict[int, list[dict]] = {}
    for r in meeting_runners:
        rn = r.get("race_number")
        if rn is not None:
            by_race.setdefault(int(rn), []).append(r)

    ROLES = ["AI Best", "Danger", "Value"]
    races: dict[int, list[dict]] = {}
    for rn, race_runners in by_race.items():
        race_runners.sort(key=lambda x: x.get("clone_rank", 999))
        top3 = race_runners[:3]
        picks = []
        for i, runner in enumerate(top3):
            picks.append({
                "tab_number": runner.get("tab_number"),
                "horse_name": runner.get("horse", "?"),
                "role": ROLES[i],
                "clone_price": runner.get("clone_price"),
                "clone_rank": runner.get("clone_rank"),
            })
        if len(picks) == 3:
            races[rn] = picks

    return races


@router.post(
    "/cron/generate-meeting-tips-clone",
    response_model=schemas.MeetingTipsOut,
)
def cron_generate_meeting_tips_clone(
    date_str: str = Query(..., alias="date"),
    pf_meeting_id: int = Query(..., alias="pf_meeting_id"),
    db: Session = Depends(get_db),
):
    """
    Generate clone-powered tips for a single meeting.

    1. Fetch top-3 clone picks per race from stablfy-social
    2. Send thin prompt to Gemini for commentary only
    3. Store tips with source="Clone"
    """
    target_date = date_type.fromisoformat(date_str)
    print(
        f"[CLONE] Generating tips for pf_meeting_id={pf_meeting_id} "
        f"on {target_date}"
    )

    # Fetch clone picks
    try:
        clone_picks_by_race = _fetch_clone_picks_for_meeting(
            target_date, pf_meeting_id
        )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to fetch clone data: {e}",
        )

    if not clone_picks_by_race:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No clone picks for pf_meeting_id={pf_meeting_id} "
                f"on {target_date}"
            ),
        )

    print(f"[CLONE] Got clone picks for {len(clone_picks_by_race)} races")

    # Build payloads (for scratchings + conditions + meeting metadata)
    payloads = daily_generator.build_generate_tips_payloads_for_date(
        target_date=target_date,
        project_id="clone",
        force_all_meetings=True,
    )

    payload = next(
        (p for p in payloads
         if getattr(p.meeting, "pf_meeting_id", None) == pf_meeting_id),
        None,
    )

    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No meeting with pf_meeting_id={pf_meeting_id} "
                f"on {target_date}"
            ),
        )

    meeting = payload.meeting

    # Skip if Clone tips already exist
    if _meeting_has_tips(
        db=db,
        meeting_date=meeting.date,
        track_name=meeting.track_name,
        state=meeting.state,
        pf_meeting_id=getattr(meeting, "pf_meeting_id", None),
        source="Clone",
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Clone tips already exist for {meeting.track_name} "
                f"({meeting.state}) on {meeting.date}"
            ),
        )

    # Generate thin-prompt commentary for each race
    races_entries: list[dict[str, Any]] = []

    for race_ctx in payload.races:
        race_in = race_ctx.race
        race_number = getattr(race_in, "race_number", None)
        scratchings = race_ctx.scratchings or []
        track_condition = race_ctx.track_condition

        clone_picks = clone_picks_by_race.get(race_number)
        if not clone_picks:
            print(
                f"[CLONE] No clone picks for {meeting.track_name} "
                f"R{race_number}, skipping"
            )
            continue

        # Filter out scratched horses from clone picks
        scratched_set = set(scratchings)
        clone_picks = [
            p for p in clone_picks
            if p["tab_number"] not in scratched_set
        ]
        if len(clone_picks) < 3:
            print(
                f"[CLONE] Clone pick scratched in {meeting.track_name} "
                f"R{race_number}, skipping"
            )
            continue

        print(
            f"[CLONE] {meeting.track_name} R{race_number}: "
            f"Best=#{clone_picks[0]['tab_number']} "
            f"{clone_picks[0]['horse_name']}, "
            f"Danger=#{clone_picks[1]['tab_number']} "
            f"{clone_picks[1]['horse_name']}, "
            f"Value=#{clone_picks[2]['tab_number']} "
            f"{clone_picks[2]['horse_name']}"
        )

        try:
            tip_dicts = gemini_client.generate_clone_race_tips(
                meeting=meeting,
                race=race_in,
                scratchings=scratchings,
                track_condition=track_condition,
                clone_picks=clone_picks,
            )
        except Exception as e:
            print(
                f"[CLONE] Error for {meeting.track_name} "
                f"R{race_number}: {e}"
            )
            tip_dicts = []

        if not tip_dicts:
            continue

        races_entries.append({"race": race_in, "tips": tip_dicts})

    if not races_entries:
        raise HTTPException(
            status_code=502,
            detail=(
                f"No Clone tips generated for "
                f"pf_meeting_id={pf_meeting_id} on {target_date}"
            ),
        )

    clone_tip_run = schemas.TipRunIn(
        source="Clone",
        model_version="clone-gemini-thin-v1",
        meta={
            "generated_by": "cron_generate_meeting_tips_clone",
            "pf_meeting_id": pf_meeting_id,
        },
    )

    tips_batch = schemas.TipsBatchIn(
        meeting=meeting,
        tip_run=clone_tip_run,
        races=races_entries,
    )

    return create_tips_batch(tips_batch, db=db)


@router.get("/tips", response_model=list[schemas.MeetingTipsOut])
def list_tips(
    response: Response,
    meeting_date: date_type = Query(..., alias="date"),
    track_name: str | None = None,
    state: str | None = None,
    source: str | None = Query(
        default=None,
        description=(
            "Filter by tip source: 'iReel', 'Gemini', or 'all'. "
            "Default: Gemini only. Pass source=all to get every provider."
        ),
    ),
    db: Session = Depends(get_db),
):
    """
    List tip runs (and tips) for a given date, optionally filtered
    by track_name, state, and/or source.

    Default source is Gemini. Pass source=all or source=iReel to override.
    """
    # Tips are time-sensitive (cron may regenerate during the day; sweep
    # may patch missing races). Forbid client-side caching so iOS
    # URLCache cannot serve a stale early-morning response after tips
    # are added later in the day.
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    q = db.query(models.TipRun).join(models.Meeting)
    q = q.filter(models.Meeting.date == meeting_date)

    if track_name:
        q = q.filter(models.Meeting.track_name == track_name)
    if state:
        q = q.filter(models.Meeting.state == state)

    # Default source is controlled by settings.tips_default_source
    # (env var TIPS_DEFAULT_SOURCE). Apps can override with ?source=iReel,
    # ?source=Gemini, or ?source=all.
    if source is not None and source.lower() != "all":
        q = q.filter(models.TipRun.source == source)
        tip_runs = q.all()
    elif source is not None and source.lower() == "all":
        tip_runs = q.all()
    else:
        q = q.filter(models.TipRun.source == settings.tips_default_source)
        tip_runs = q.all()
    results: list[schemas.MeetingTipsOut] = []

    for tr in tip_runs:
        meeting = tr.meeting
        races_with_tips: list[schemas.RaceWithTipsOut] = []

        for race in meeting.races:
            race_tips = [t for t in tr.tips if t.race_id == race.id]
            if not race_tips:
                continue

            race_out = schemas.RaceOut.model_validate(race)
            race_out.pf_meeting_id = meeting.pf_meeting_id
            tip_outs = [schemas.TipOut.model_validate(t) for t in race_tips]

            races_with_tips.append(
                schemas.RaceWithTipsOut(
                    race=race_out,
                    tips=tip_outs,
                )
            )

        if races_with_tips:
            meeting_out = schemas.MeetingOut.model_validate(meeting)
            results.append(
                schemas.MeetingTipsOut(
                    meeting=meeting_out,
                    tip_run_id=tr.id,
                    races=races_with_tips,
                )
            )

    return results


@router.put("/tips/{tip_id}", response_model=schemas.TipOut)
def edit_tip(
    tip_id: str,
    payload: schemas.TipEditIn,
    db: Session = Depends(get_db),
):
    """
    Edit an existing tip. Only provided fields will be updated.
    """
    tip = db.query(models.Tip).filter(models.Tip.id == tip_id).first()

    if tip is None:
        raise HTTPException(status_code=404, detail=f"Tip {tip_id} not found")

    # Update only provided fields
    if payload.tip_type is not None:
        tip.tip_type = payload.tip_type
    if payload.tab_number is not None:
        tip.tab_number = payload.tab_number
    if payload.horse_name is not None:
        tip.horse_name = payload.horse_name
    if payload.reasoning is not None:
        tip.reasoning = payload.reasoning
    if payload.stake_units is not None:
        tip.stake_units = payload.stake_units

    db.commit()
    db.refresh(tip)

    print(f"[TIPS] Updated tip {tip_id}: {tip.tip_type} #{tip.tab_number} {tip.horse_name}")

    return schemas.TipOut.model_validate(tip)


@router.delete("/tips/{tip_id}")
def delete_tip(
    tip_id: str,
    db: Session = Depends(get_db),
):
    """
    Delete a specific tip.
    """
    tip = db.query(models.Tip).filter(models.Tip.id == tip_id).first()

    if tip is None:
        raise HTTPException(status_code=404, detail=f"Tip {tip_id} not found")

    tip_info = f"{tip.tip_type} #{tip.tab_number} {tip.horse_name}"
    db.delete(tip)
    db.commit()

    print(f"[TIPS] Deleted tip {tip_id}: {tip_info}")

    return {"ok": True, "deleted": tip_id}
