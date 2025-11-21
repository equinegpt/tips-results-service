# app/models.py
import uuid
from datetime import date, datetime
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    Numeric,
    JSON,
    UniqueConstraint,
)
from sqlalchemy.dialects.sqlite import BLOB as SQLITE_BLOB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship, Mapped, mapped_column

from .database import Base


def UUIDCol():
    """UUID column type that works on both SQLite and Postgres."""
    # If we ever switch to Postgres, PG_UUID is nice;
    # SQLite will happily store UUID as BLOB or TEXT.
    try:
        return mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    except Exception:  # fallback for SQLite if needed
        return mapped_column(SQLITE_BLOB, primary_key=True, default=lambda: uuid.uuid4().bytes)


class Meeting(Base):
    __tablename__ = "meetings"

    # We'll use TEXT UUID for portability; simpler than BLOB.
    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    date: Mapped[date] = mapped_column(Date, nullable=False)
    track_name: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String(8), nullable=False)
    country: Mapped[str] = mapped_column(String(8), nullable=False, default="AUS")

    pf_meeting_id: Mapped[int | None] = mapped_column(Integer)
    ra_meetcode: Mapped[str | None] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    races: Mapped[list["Race"]] = relationship("Race", back_populates="meeting")

    __table_args__ = (
        UniqueConstraint("date", "track_name", "state", name="uq_meeting_date_track"),
    )


class Race(Base):
    __tablename__ = "races"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    meeting_id: Mapped[str] = mapped_column(
        String, ForeignKey("meetings.id"), nullable=False
    )
    race_number: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str | None] = mapped_column(String)
    distance_m: Mapped[int | None] = mapped_column(Integer)
    class_text: Mapped[str | None] = mapped_column(String)
    scheduled_start: Mapped[datetime | None] = mapped_column(DateTime)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    meeting: Mapped["Meeting"] = relationship("Meeting", back_populates="races")
    tips: Mapped[list["Tip"]] = relationship("Tip", back_populates="race")

    __table_args__ = (
        UniqueConstraint("meeting_id", "race_number", name="uq_race_meeting_number"),
    )


class TipRun(Base):
    __tablename__ = "tip_runs"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    source: Mapped[str] = mapped_column(String, nullable=False)  # 'iReel' etc
    model_version: Mapped[str | None] = mapped_column(String)

    meeting_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("meetings.id"), nullable=True
    )

    # For traceability: store iReel project id, chat id, prompt, etc
    meta: Mapped[dict | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    meeting: Mapped["Meeting | None"] = relationship("Meeting")
    tips: Mapped[list["Tip"]] = relationship("Tip", back_populates="tip_run")


class Tip(Base):
    __tablename__ = "tips"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    tip_run_id: Mapped[str] = mapped_column(
        String, ForeignKey("tip_runs.id"), nullable=False
    )
    race_id: Mapped[str] = mapped_column(
        String, ForeignKey("races.id"), nullable=False
    )

    tip_type: Mapped[str] = mapped_column(String, nullable=False)  # 'AI_BEST', 'DANGER'
    tab_number: Mapped[int] = mapped_column(Integer, nullable=False)
    horse_name: Mapped[str] = mapped_column(String, nullable=False)

    reasoning: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[Numeric | None] = mapped_column(Numeric(4, 3))
    stake_units: Mapped[Numeric] = mapped_column(Numeric(5, 2), default=1.0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    tip_run: Mapped["TipRun"] = relationship("TipRun", back_populates="tips")
    race: Mapped["Race"] = relationship("Race", back_populates="tips")

    __table_args__ = (
        UniqueConstraint("tip_run_id", "race_id", "tip_type", name="uq_tip_unique"),
    )
class RaceResult(Base):
    """
    Official race result for a given provider (default: RA) and race.
    One row per (provider, race, tab_number).
    """
    __tablename__ = "race_results"

    id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid.uuid4())
    )

    provider: Mapped[str] = mapped_column(String, nullable=False, default="RA")
    race_id: Mapped[str] = mapped_column(
        String, ForeignKey("races.id"), nullable=False
    )

    tab_number: Mapped[int] = mapped_column(Integer, nullable=False)
    horse_name: Mapped[str] = mapped_column(String, nullable=False)

    # 1, 2, 3…  or None if scratched / no result
    finish_position: Mapped[int | None] = mapped_column(Integer)

    # FREE TEXT: RUN, SCRATCHED, LATE SCR, ABANDONED, etc
    status: Mapped[str | None] = mapped_column(String)

    # We'll just keep the raw margin text ("1.5L", "NK", etc)
    margin_text: Mapped[str | None] = mapped_column(String)

    # Starting Price (win) as a decimal: 3.60 == $3.60
    starting_price: Mapped[Numeric | None] = mapped_column(Numeric(10, 2))

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    race: Mapped["Race"] = relationship("Race")

    __table_args__ = (
        UniqueConstraint(
            "provider", "race_id", "tab_number", name="uq_result_race_provider_tab"
        ),
    )


class TipOutcome(Base):
    """
    Outcome of a specific Tip against an official RaceResult (per provider).
    One row per tip_id.
    """
    __tablename__ = "tip_outcomes"

    tip_id: Mapped[str] = mapped_column(
        String, ForeignKey("tips.id"), primary_key=True
    )

    provider: Mapped[str] = mapped_column(String, nullable=False, default="RA")

    race_result_id: Mapped[str | None] = mapped_column(
        String, ForeignKey("race_results.id"), nullable=True
    )

    # Duplicate key bits here for quick querying / future-proofing.
    finish_position: Mapped[int | None] = mapped_column(Integer)

    # WIN, PLACE, LOSE, SCRATCHED, NO_RESULT, PENDING
    outcome_status: Mapped[str] = mapped_column(
        String, nullable=False, default="PENDING"
    )

    # SP win price for that horse, if we have it
    starting_price: Mapped[Numeric | None] = mapped_column(Numeric(10, 2))

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow
    )

    tip: Mapped["Tip"] = relationship("Tip")
    race_result: Mapped["RaceResult | None"] = relationship("RaceResult")
