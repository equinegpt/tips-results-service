"""8F Analyst — in-house tips generation (replaces Gemini + iReel).

Deterministic, explainable, backtestable. Consumes ONLY the raw
PuntingForm ingredients allowed by docs/analyst-data-contract.md —
no SkyNet, no Clone, no No Mugs, no PFAI model outputs.
"""
from .scorer import score_race, select_tips  # noqa: F401
from .reasoning import reasoning_line  # noqa: F401
