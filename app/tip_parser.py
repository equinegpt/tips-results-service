# app/tip_parser.py
"""Tip-text parsing, retained from the retired iReel client.

The external providers (iReel chat API, Gemini via the Stablfy
platform) were STRIPPED on 2026-08-04 — tips are generated in-house
(source='8F', scripts/generate_8f_tips.py). The admin manual-entry
routes still accept pasted tip text in the classic format, so the pure
parser lives on here. Any attempt to call a retired network method
(generate_race_tips, _post_chat, ...) raises 410 Gone.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

try:
    from fastapi import HTTPException
except ImportError:                      # local scripts without fastapi
    class HTTPException(Exception):      # type: ignore[no-redef]
        def __init__(self, status_code: int, detail: str = ""):
            self.status_code, self.detail = status_code, detail
            super().__init__(f"{status_code}: {detail}")

log = logging.getLogger(__name__)

_RETIRED_MSG = ("External tip providers retired 2026-08-04. Tips are "
                "generated in-house (source=8F) by "
                "scripts/generate_8f_tips.py; this endpoint is gone.")


class RetiredProvider:
    """Every attribute access raises 410 — retired generation endpoints
    fail loudly and honestly instead of half-working."""

    def __getattr__(self, name: str):
        def _gone(*a, **k):
            raise HTTPException(status_code=410, detail=_RETIRED_MSG)
        return _gone


class TipTextParser(RetiredProvider):
    """parse_tips_text survives (pure text parsing, used by the admin
    manual-entry routes); everything else inherits the 410."""

    def parse_tips_text(self, text: str) -> List[Dict[str, Any]]:
        """
        Parse tips text into a list of tip dicts.

        Each dict matches the shape expected inside TipsBatchIn.races[].tips[]:
        {
            "tip_type": "AI_BEST" | "DANGER" | "VALUE",
            "tab_number": int,
            "horse_name": str,
            "reasoning": str,
            "stake_units": float,
        }

        Robust behaviour:
        - Handles the normal case: AI Best / Danger / Value.
        - If there is no explicit "Value:" line but two separate "Danger:"
          lines are present, the second Danger is treated as VALUE.
        """
        tips: List[Dict[str, Any]] = []

        if not text:
            return tips

        cleaned = text.strip()
        cleaned = cleaned.replace("\\n", "\n")
        cleaned = cleaned.replace("\\u2014", "—")

        ai_best_re = re.compile(
            r"AI\s*Best\s*:?\s*[#No\.\s]*(\d+)\s+(.+?)\s*[—\-–]\s*(.+?)(?=(?:\n|$|Danger\s*:|Value\s*:))",
            re.IGNORECASE | re.DOTALL,
        )
        danger_re = re.compile(
            r"Danger\s*:?\s*[#No\.\s]*(\d+)\s+(.+?)\s*[—\-–]\s*(.+?)(?=(?:\n|$|Value\s*:|AI\s*Best\s*:|Danger\s*:|$))",
            re.IGNORECASE | re.DOTALL,
        )
        value_re = re.compile(
            r"Value\s*:?\s*[#No\.\s]*(\d+)\s+(.+?)\s*[—\-–]\s*(.+)",
            re.IGNORECASE | re.DOTALL,
        )

        def make_tip(kind: str, m: re.Match) -> Dict[str, Any]:
            return {
                "tip_type": kind,
                "tab_number": int(m.group(1)),
                "horse_name": self._clean_fragment(m.group(2)),
                "reasoning": self._clean_fragment(m.group(3)),
                "stake_units": 1.0,
            }

        m_best = ai_best_re.search(cleaned)
        if m_best:
            tips.append(make_tip("AI_BEST", m_best))

        danger_matches = list(danger_re.finditer(cleaned))
        if danger_matches:
            tips.append(make_tip("DANGER", danger_matches[0]))

        m_value = value_re.search(cleaned)
        if m_value:
            tips.append(make_tip("VALUE", m_value))
        else:
            if len(danger_matches) > 1:
                log.warning(
                    "[TipTextParser] No explicit 'Value:' line – treating "
                    "second Danger as VALUE. Raw text: %r", text[:300])
                tips.append(make_tip("VALUE", danger_matches[1]))

        return tips

    @staticmethod
    def _clean_fragment(s: str) -> str:
        """Normalise small regex-group fragments: strip markdown bold,
        decode common escapes, trim stray quotes/braces."""
        if s is None:
            return ""
        s = str(s)
        s = s.replace("\\u2014", "—")
        s = s.replace("—", "—")
        s = s.replace("\\u0027", "'")
        s = s.replace("\\u2019", "'")
        s = s.replace("**", "")
        s = " ".join(s.split())
        return s.strip(' "\'}')
