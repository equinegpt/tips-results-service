# app/clients.py
"""Provider singletons.

2026-08-04 STRIP: iReel (chat API) and Gemini (Stablfy platform →
Google) are RETIRED — tips are generated in-house
(scripts/generate_8f_tips.py, source='8F', served to legacy consumers
via GEMINI_ALIAS_SOURCE). The objects below keep the old import
surface alive: parse_tips_text still works (admin manual entry);
every retired network method raises 410 Gone.
"""
from __future__ import annotations

from .tip_parser import RetiredProvider, TipTextParser

ireel_client = TipTextParser()      # .parse_tips_text lives; the rest 410s
gemini_client = RetiredProvider()   # everything 410s
