# app/ui_helpers.py
from __future__ import annotations

import re
from datetime import date as date_type, datetime
from typing import Any

from fastapi.templating import Jinja2Templates

from . import models

# Single templates instance for the whole app
templates = Jinja2Templates(directory="templates")

# ---------- Jinja filters ----------

def decode_reasoning(value: str | None) -> str:
    """
    Clean up old iReel text artefacts for display only:
    - decode \u2014 etc to punctuation
    - strip stray quotes/braces
    - collapse \n into spaces
    """
    if not value:
        return ""
    text = str(value)

    replacements = {
        r"\\u2014": "—",
        r"\\u2013": "–",
        r"\\u2019": "’",
        r"\\u002B": "+",
        r"\\n": " ",
        r"\\r": " ",
        r"\\u0027": "'",   #  ← NEW: apostrophe

    }
    for k, v in replacements.items():
        text = text.replace(k, v)

    # Trim junk from badly-serialised JSON
    text = text.strip().strip('"}').strip()
    return text


def extract_horse_label(raw: str | None) -> str:
    """
    Try to pull a clean horse name out of the stored horse_name field.

    Handles old rows like:
      "**Sensational Secret** \\u2014 Strong class"
    and newer rows where horse_name is just "Sensational Secret".
    """
    if not raw:
        return "–"

    s = str(raw).strip()
    if not s:
        return "–"

    # 1) If we have markdown-style **Name**, grab the first bold segment
    if "**" in s:
        parts = s.split("**")
        for i, p in enumerate(parts):
            if i % 2 == 1 and p.strip():
                return p.strip()

    # 2) Otherwise, split on an em dash or hyphen and take the left-hand side
    for sep in ["\u2014", "—", " - ", "-"]:
        if sep in s:
            head = s.split(sep, 1)[0].strip()
            if head:
                return head

    # 3) Fallback: just return the whole thing
    return s


def human_date(value: date_type) -> str:
    """Render '2025-11-19' as 'Wednesday 19 November 2025'."""
    if isinstance(value, datetime):
        d = value.date()
    else:
        d = value
    return d.strftime("%A %d %B %Y")


templates.env.filters["decode_reasoning"] = decode_reasoning
templates.env.filters["horse_label"] = extract_horse_label
templates.env.filters["human_date"] = human_date

# ---------- UI helpers for /ui/day ----------

def clean_text(text: str | None) -> str:
    """
    Light clean for any iReel text:
    - decode common escaped unicode / newlines
    - strip markdown **bold** markers
    - trim stray JSON-ish junk
    """
    if not text:
        return ""
    t = str(text)

    # Decode common escape junk
    t = t.replace("\\u2014", "—")
    t = t.replace("\\n", " ")
    t = t.replace("\n", " ")
    t = t.replace("\\u0027", "'")   # ← NEW: apostrophe (escaped)
    t = t.replace("\u0027", "'")   # ← NEW: apostrophe (already decoded)
    # Strip markdown bold markers
    t = t.replace("**", "")

    # Tidy up stray JSON-ish endings
    t = t.replace('\\"', '"')
    t = t.replace("'}", "")
    t = t.replace("\"}", "")

    # Collapse double spaces, trim
    t = " ".join(t.split())
    return t.strip()


def display_horse_name(tip: models.Tip) -> str:
    """
    Return a clean horse name for display.

    Handles things like:
      "**Sensational Secret** \\u2014 Strong class"
      "AI Best: #2 Sweltering — Strong last 600m..."
    """
    raw = tip.horse_name or ""

    # If the AI accidentally kept "AI Best: #2 ..." etc, strip that off
    raw = re.sub(
        r"^\s*(AI\s*Best|Best|Danger|Value)\s*:\s*[#No\.\s]*\d+\s*",
        "",
        raw,
        flags=re.IGNORECASE,
    )

    # If there's an em dash / hyphen, keep only the left side (name)
    for sep in ["\u2014", "\\u2014", "—", " - ", "-"]:
        if sep in raw:
            raw = raw.split(sep, 1)[0]
            break

    return clean_text(raw)

def display_reason(tip: models.Tip) -> str:
    """
    Return a clean reason string.

    Prefer Tip.reasoning; fall back to the part of horse_name after the dash.
    Also strips any trailing 'Danger: ...' or 'Value: ...' that got jammed in.
    """
    raw = tip.reasoning or ""

    if not raw:
        # Fallback: try to salvage text after the dash from horse_name
        raw = tip.horse_name or ""
        for sep in ["\u2014", "\\u2014", "—", " - ", "-"]:
            if sep in raw:
                raw = raw.split(sep, 1)[1]
                break

    # If another "Danger: ..." or "Value: ..." got jammed in, drop everything after it
    for marker in ("Danger:", "Value:"):
        if marker in raw:
            raw = raw.split(marker, 1)[0]

    return clean_text(raw)

def format_pretty_date(d: date_type) -> str:
    """e.g. Wednesday 19 November 2025."""
    try:
        return d.strftime("%A %d %B %Y")
    except Exception:
        return d.isoformat()


def classify_outcome_from_finish(pos_fin: int | None) -> str:
    """
    Simple WIN/PLACE/LOSE classifier (duplicate of pf_results._classify_outcome).
    """
    if pos_fin is None or pos_fin <= 0:
        return "UNKNOWN"
    if pos_fin == 1:
        return "WIN"
    if pos_fin in (2, 3):
        return "PLACE"
    return "LOSE"
