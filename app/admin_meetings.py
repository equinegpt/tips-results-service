from datetime import date
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_async_session
from . import models  # assumes Meeting is in app/models.py


Meeting = models.Meeting  # alias for brevity

router = APIRouter(prefix="/admin/meetings", tags=["admin-meetings"])


class FixPFMeetingIDIn(BaseModel):
    # Either meeting_id (UUID) OR (date + track_name [+ state])
    meeting_id: str | None = None
    date: date | None = None
    track_name: str | None = None
    state: str | None = None

    pf_meeting_id: int


@router.post("/fix-pf-meeting-id")
async def fix_pf_meeting_id(
    body: FixPFMeetingIDIn,
    db: AsyncSession = Depends(get_async_session),
):
    # --- Build the query for the meeting we want to patch ---
    if body.meeting_id:
        stmt = select(Meeting).where(Meeting.id == body.meeting_id)
    else:
        if not (body.date and body.track_name):
            raise HTTPException(
                status_code=400,
                detail="Provide either meeting_id or (date + track_name [+ state])",
            )
        stmt = select(Meeting).where(
            Meeting.date == body.date,
            Meeting.track_name == body.track_name,
        )
        if body.state:
            stmt = stmt.where(Meeting.state == body.state)

    result = await db.execute(stmt)
    meetings = list(result.scalars())

    if not meetings:
        raise HTTPException(status_code=404, detail="Meeting not found")

    if len(meetings) > 1 and not body.meeting_id:
        raise HTTPException(
            status_code=409,
            detail="Multiple meetings matched – use meeting_id to disambiguate",
        )

    m = meetings[0]
    m.pf_meeting_id = body.pf_meeting_id

    await db.commit()
    await db.refresh(m)

    return {
        "ok": True,
        "meeting_id": str(m.id),
        "track_name": m.track_name,
        "state": m.state,
        "date": str(m.date),
        "pf_meeting_id": m.pf_meeting_id,
    }
