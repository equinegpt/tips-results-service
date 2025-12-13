# app/routes_admin.py
from __future__ import annotations

import uuid
from datetime import date as date_type
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .database import get_db
from . import models
from .models import Meeting
from .clients import ireel_client

router = APIRouter()


class PfMeetingIdPatch(BaseModel):
    date: date_type
    track_name: str
    state: str
    pf_meeting_id: int


class AdminReplaceTipsText(BaseModel):
    date: date_type
    track_name: str
    state: str
    race_number: int
    text: str


class ManualTipsImport(BaseModel):
    date: date_type
    track_name: str
    state: str
    race_number: int
    text: str
    tip_run_id: str | None = None  # optional: override which TipRun to use


@router.post("/admin/backfill-pf-meeting-ids")
def admin_backfill_pf_meeting_ids(
    payload: list[PfMeetingIdPatch],
    db: Session = Depends(get_db),
):
    updated = 0
    missing: list[dict[str, Any]] = []

    for item in payload:
        meeting = (
            db.query(models.Meeting)
            .filter(
                models.Meeting.date == item.date,
                models.Meeting.track_name == item.track_name,
                models.Meeting.state == item.state,
            )
            .first()
        )

        if not meeting:
            missing.append(
                {
                    "date": item.date.isoformat(),
                    "track_name": item.track_name,
                    "state": item.state,
                    "pf_meeting_id": item.pf_meeting_id,
                    "reason": "no matching Meeting row",
                }
            )
            continue

        meeting.pf_meeting_id = item.pf_meeting_id
        updated += 1

    db.commit()

    return {
        "ok": True,
        "updated": updated,
        "missing": missing,
    }


@router.post("/admin/import-tips-text")
def admin_import_tips_text(
    payload: ManualTipsImport,
    db: Session = Depends(get_db),
):
    """
    Manually import tips for a single race from raw iReel text.

    - You provide: date, track_name, state, race_number, and the raw text
      that contains 'AI Best: ...' and 'Danger: ...'.
    - We find (or create) a TipRun for that meeting.
    - We create the Race row if it doesn't exist.
    - We parse the text with ireel_client.parse_tips_text and insert tips.
    """

    # 1) Find Meeting
    meeting = (
        db.query(models.Meeting)
        .filter(
            models.Meeting.date == payload.date,
            models.Meeting.track_name == payload.track_name,
            models.Meeting.state == payload.state,
        )
        .first()
    )
    if not meeting:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No Meeting found for date={payload.date.isoformat()} "
                f"track_name={payload.track_name!r} state={payload.state!r}"
            ),
        )

    # 2) Find (or create) TipRun to attach to
    if payload.tip_run_id:
        tip_run = (
            db.query(models.TipRun)
            .filter(
                models.TipRun.id == payload.tip_run_id,
                models.TipRun.meeting_id == meeting.id,
            )
            .first()
        )
        if not tip_run:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No TipRun {payload.tip_run_id!r} found for this meeting."
                ),
            )
    else:
        tip_run = (
            db.query(models.TipRun)
            .filter(models.TipRun.meeting_id == meeting.id)
            .order_by(models.TipRun.created_at.desc())
            .first()
        )

        if tip_run is None:
            # No auto iReel run – create a manual TipRun so we can attach these tips.
            tip_run = models.TipRun(
                id=str(uuid.uuid4()),
                meeting_id=meeting.id,
                source="manual-import",
                model_version="manual",
            )
            db.add(tip_run)
            db.commit()
            db.refresh(tip_run)

    # 3) Ensure Race row exists
    race = (
        db.query(models.Race)
        .filter(
            models.Race.meeting_id == meeting.id,
            models.Race.race_number == payload.race_number,
        )
        .first()
    )

    if race is None:
        race = models.Race(
            meeting_id=meeting.id,
            race_number=payload.race_number,
            name=None,
            distance_m=None,
            class_text=None,
            scheduled_start=None,
        )
        db.add(race)
        db.flush()

    # 4) Check if this race already has tips for this TipRun
    existing_count = (
        db.query(models.Tip)
        .filter(
            models.Tip.tip_run_id == tip_run.id,
            models.Tip.race_id == race.id,
        )
        .count()
    )
    if existing_count > 0:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Race {payload.race_number} at {meeting.track_name} already has "
                f"{existing_count} tips for this TipRun; refusing to overwrite."
            ),
        )

    # 5) Parse the text using the existing iReel parser
    tip_dicts = ireel_client.parse_tips_text(payload.text or "")
    if not tip_dicts:
        raise HTTPException(
            status_code=400,
            detail="No tips could be parsed from the provided text.",
        )

    # 6) Insert tips
    inserted = 0
    for td in tip_dicts:
        tip = models.Tip(
            race_id=race.id,
            tip_run_id=tip_run.id,
            tip_type=td["tip_type"],
            tab_number=td["tab_number"],
            horse_name=td["horse_name"],
            reasoning=td.get("reasoning"),
            stake_units=td.get("stake_units", 1.0),
        )
        db.add(tip)
        inserted += 1

    db.commit()

    return {
        "ok": True,
        "meeting_id": meeting.id,
        "tip_run_id": tip_run.id,
        "race_id": race.id,
        "tips_inserted": inserted,
    }


@router.post("/admin/replace-tips-text")
def admin_replace_tips_text(
    payload: AdminReplaceTipsText,
    db: Session = Depends(get_db),
):
    # 1) Find the meeting
    meeting = (
        db.query(models.Meeting)
        .filter(
            models.Meeting.date == payload.date,
            models.Meeting.track_name == payload.track_name,
            models.Meeting.state == payload.state,
        )
        .first()
    )
    if not meeting:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No Meeting found for date={payload.date}, "
                f"track_name={payload.track_name}, state={payload.state}"
            ),
        )

    # 2) Latest TipRun for that meeting
    tip_run = (
        db.query(models.TipRun)
        .filter(models.TipRun.meeting_id == meeting.id)
        .order_by(models.TipRun.created_at.desc())
        .first()
    )
    if not tip_run:
        raise HTTPException(
            status_code=400,
            detail=f"No TipRun found for meeting_id={meeting.id}",
        )

    # 3) Race by meeting + race_number
    race = (
        db.query(models.Race)
        .filter(
            models.Race.meeting_id == meeting.id,
            models.Race.race_number == payload.race_number,
        )
        .first()
    )
    if not race:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No Race found for meeting_id={meeting.id}, "
                f"race_number={payload.race_number}"
            ),
        )

    # 4) Delete existing tips for this TipRun+Race
    deleted = (
        db.query(models.Tip)
        .filter(
            models.Tip.tip_run_id == tip_run.id,
            models.Tip.race_id == race.id,
        )
        .delete(synchronize_session=False)
    )

    # 5) Parse fresh tips from text
    tip_dicts = ireel_client.parse_tips_text(payload.text)
    if not tip_dicts:
        raise HTTPException(
            status_code=400,
            detail="parse_tips_text() did not return any tips for the given text.",
        )

    inserted = 0
    for t in tip_dicts:
        tip = models.Tip(
            race_id=race.id,
            tip_run_id=tip_run.id,
            tip_type=t["tip_type"],
            tab_number=t["tab_number"],
            horse_name=t["horse_name"],
            reasoning=t.get("reasoning"),
            stake_units=t.get("stake_units", 1.0),
        )
        db.add(tip)
        inserted += 1

    db.commit()

    return {
        "ok": True,
        "meeting_id": meeting.id,
        "tip_run_id": tip_run.id,
        "race_id": race.id,
        "deleted_old_tips": deleted,
        "inserted_new_tips": inserted,
    }
