# app/crud.py
from datetime import datetime
from sqlalchemy.orm import Session
from . import models, schemas


def get_or_create_meeting(db: Session, data: schemas.MeetingBase) -> models.Meeting:
    m = (
        db.query(models.Meeting)
        .filter(
            models.Meeting.date == data.date,
            models.Meeting.track_name == data.track_name,
            models.Meeting.state == data.state,
        )
        .first()
    )
    if m:
        changed = False
        if data.pf_meeting_id and m.pf_meeting_id != data.pf_meeting_id:
            m.pf_meeting_id = data.pf_meeting_id
            changed = True
        if data.ra_meetcode and m.ra_meetcode != data.ra_meetcode:
            m.ra_meetcode = data.ra_meetcode
            changed = True
        if changed:
            m.updated_at = datetime.utcnow()
        return m

    m = models.Meeting(
        date=data.date,
        track_name=data.track_name,
        state=data.state,
        country=data.country,
        pf_meeting_id=data.pf_meeting_id,
        ra_meetcode=data.ra_meetcode,
    )
    db.add(m)
    db.flush()
    return m


def get_or_create_race(
    db: Session, meeting: models.Meeting, data: schemas.RaceBase
) -> models.Race:
    r = (
        db.query(models.Race)
        .filter(
            models.Race.meeting_id == meeting.id,
            models.Race.race_number == data.race_number,
        )
        .first()
    )
    if r:
        return r

    r = models.Race(
        meeting_id=meeting.id,
        race_number=data.race_number,
        name=data.name,
        distance_m=data.distance_m,
        class_text=data.class_text,
        scheduled_start=data.scheduled_start,
    )
    db.add(r)
    db.flush()
    return r


def create_tip_run(
    db: Session, meeting: models.Meeting, data: schemas.TipRunIn
) -> models.TipRun:
    meta = dict(data.meta or {})
    if data.project_id:
        meta["project_id"] = data.project_id

    tr = models.TipRun(
        source=data.source,
        model_version=data.model_version,
        meeting_id=meeting.id,
        meta=meta,
    )
    db.add(tr)
    db.flush()
    return tr


def create_tips_batch(db: Session, payload: schemas.TipsBatchIn) -> models.TipRun:
    meeting = get_or_create_meeting(db, payload.meeting)
    tip_run = create_tip_run(db, meeting, payload.tip_run)

    for race_block in payload.races:
        race = get_or_create_race(db, meeting, race_block.race)
        for t in race_block.tips:
            tip = models.Tip(
                tip_run_id=tip_run.id,
                race_id=race.id,
                tip_type=t.tip_type,
                tab_number=t.tab_number,
                horse_name=t.horse_name,
                reasoning=t.reasoning,
                confidence=t.confidence,
                stake_units=t.stake_units,
            )
            db.add(tip)

    db.commit()
    db.refresh(tip_run)
    return tip_run
