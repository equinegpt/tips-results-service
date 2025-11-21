# app/schemas.py
from datetime import date, datetime
from typing import Literal, Optional, List, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field


TipType = Literal["AI_BEST", "DANGER", "VALUE"]


# ---------- Meetings & Races ----------

class MeetingBase(BaseModel):
    date: date
    track_name: str
    state: str
    country: str = "AUS"
    pf_meeting_id: Optional[int] = None
    ra_meetcode: Optional[str] = None


class MeetingOut(MeetingBase):
    id: UUID

    class Config:
        from_attributes = True


class RaceBase(BaseModel):
    race_number: int
    name: Optional[str] = None
    distance_m: Optional[int] = None
    class_text: Optional[str] = None
    scheduled_start: Optional[datetime] = None


class RaceOut(RaceBase):
    id: UUID

    class Config:
        from_attributes = True

class RaceGenerationIn(BaseModel):
    race: RaceBase
    scratchings: List[int] = []
    track_condition: Optional[str] = None

# ---------- Tips input / output ----------

class TipIn(BaseModel):
    tip_type: TipType
    tab_number: int
    horse_name: str
    reasoning: Optional[str] = None
    confidence: Optional[float] = Field(default=None, ge=0, le=1)
    stake_units: float = 1.0


class RaceTipsIn(BaseModel):
    race: RaceBase
    tips: List[TipIn]


class TipRunIn(BaseModel):
    source: str = "iReel"
    model_version: Optional[str] = None
    project_id: Optional[str] = None  # iReel project id
    meta: Dict[str, Any] = Field(default_factory=dict)


class TipsBatchIn(BaseModel):
    tip_run: TipRunIn
    meeting: MeetingBase
    races: List[RaceTipsIn]

class GenerateTipsIn(BaseModel):
    tip_run: TipRunIn              # same as before: includes project_id
    meeting: MeetingBase           # date, track_name, state, pf_meeting_id...
    races: List[RaceGenerationIn]  # races with scratchings/condition, but NO tips yet

class TipOut(BaseModel):
    id: UUID
    tip_type: TipType
    tab_number: int
    horse_name: str
    reasoning: Optional[str]
    stake_units: float

    class Config:
        from_attributes = True


class RaceWithTipsOut(BaseModel):
    race: RaceOut
    tips: List[TipOut]


class MeetingTipsOut(BaseModel):
    meeting: MeetingOut
    tip_run_id: UUID
    races: List[RaceWithTipsOut]


# ---------- Stats / Analytics ----------

class TipTypeStats(BaseModel):
    tip_type: TipType
    tips: int
    wins: int
    places: int
    win_strike_rate: float
    place_strike_rate: float
    total_staked: float        # only counting bets where we know SP
    net_profit: float          # profit on WIN/LOSE using win SP only
    roi: float                 # net_profit / total_staked (0 if no stake)


class DayStatsOut(BaseModel):
    date: date
    provider: str
    stake_per_tip: float
    stats: List[TipTypeStats]


class RangeStatsOut(BaseModel):
    date_from: date
    date_to: date
    provider: str
    stake_per_tip: float
    stats: List[TipTypeStats]

class CronGenerateTipsOut(BaseModel):
    ok: bool
    date: str              # we currently return date.isoformat(), i.e. a string
    project_id: str
    meetings_processed: int
    tip_runs_created: int
    races_with_tips: int