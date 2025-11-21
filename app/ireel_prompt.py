# app/iReel_prompt.py
from __future__ import annotations

from typing import List
import re

from . import schemas


def build_race_prompt(
    meeting: schemas.MeetingIn,
    race: schemas.RaceIn,
    scratchings: list[int],
    track_condition: str | None,
) -> str:
    """
    Build the exact shape of prompt we send to iReel, per race.
    Now requests AI Best, Danger, and Value selections.
    """
    lines: list[str] = []

    # Optional meetingId (Punting Form meetingId if you ever add it)
    pf_meeting_id = getattr(meeting, "pf_meeting_id", None)
    if pf_meeting_id:
        lines.append(f"meetingId: {pf_meeting_id}")

    # Meeting basics
    lines.append(f"meeting: {meeting.track_name}")
    lines.append(f"date: {meeting.date.isoformat()}")

    # Race header
    race_header = f"raceNumber: {race.race_number}"
    if race.name:
        race_header += f" — {race.name}"
    lines.append(race_header)

    # Scratchings
    if scratchings:
        scr_list = ", ".join(str(n) for n in sorted(scratchings))
        lines.append(f"SCRATCHINGS (TAB NUMBERS): {scr_list}")
        lines.append(
            "IMPORTANT: Treat these TAB numbers as SCRATCHED for all answers and calculations."
        )

    # Track condition
    if track_condition:
        lines.append(f"TRACK CONDITION (official): {track_condition}")

    lines.append("")
    lines.append("Please reply with short mobile-length tips:")
    lines.append('• "AI Best: #<TAB> <Horse> — <one-sentence reason>"')
    lines.append('• "Danger: #<TAB> <Horse> — <one-sentence reason>"')
    lines.append('• "Value: #<TAB> <Horse> — <one-sentence reason>"')
    lines.append("No extra commentary.")

    return "\n".join(lines)


# -------- Parsing --------

# Regexes to pull AI Best, Danger, and Value out of iReel's text response.
# We keep them separate but identical in shape for simplicity.

_AI_RE = re.compile(
    r"AI\s*Best:\s*#(?P<tab>\d+)\s+(?P<name>[^–—\-\n]+)[\s–—\-]+(?P<reason>.+)",
    re.IGNORECASE | re.DOTALL,
)

_DANGER_RE = re.compile(
    r"Danger:\s*#(?P<tab>\d+)\s+(?P<name>[^–—\-\n]+)[\s–—\-]+(?P<reason>.+)",
    re.IGNORECASE | re.DOTALL,
)

_VALUE_RE = re.compile(
    r"Value:\s*#(?P<tab>\d+)\s+(?P<name>[^–—\-\n]+)[\s–—\-]+(?P<reason>.+)",
    re.IGNORECASE | re.DOTALL,
)


def _make_tip(
    kind: str,  # "AI_BEST", "DANGER", or "VALUE"
    tab: str,
    name: str,
    reason: str,
) -> schemas.TipIn:
    return schemas.TipIn(
        tip_type=kind,
        tab_number=int(tab),
        horse_name=name.strip().strip(" ."),
        reasoning=reason.strip(),
        stake_units=1.0,
    )


def parse_tips_text(text: str) -> List[schemas.TipIn]:
    """
    Parse an iReel tips string into [TipIn, ...] for:
      - AI_BEST
      - DANGER
      - VALUE

    Raises if nothing matches, so we can log and skip that race.
    """
    tips: List[schemas.TipIn] = []

    ai = _AI_RE.search(text or "")
    danger = _DANGER_RE.search(text or "")
    value = _VALUE_RE.search(text or "")

    if ai:
        tips.append(
            _make_tip(
                "AI_BEST",
                ai.group("tab"),
                ai.group("name"),
                ai.group("reason"),
            )
        )

    if danger:
        tips.append(
            _make_tip(
                "DANGER",
                danger.group("tab"),
                danger.group("name"),
                danger.group("reason"),
            )
        )

    if value:
        tips.append(
            _make_tip(
                "VALUE",
                value.group("tab"),
                value.group("name"),
                value.group("reason"),
            )
        )

    if not tips:
        raise ValueError(
            f"Could not parse AI Best / Danger / Value from iReel response: {text!r}"
        )

    return tips
