# app/ireel_client.py
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional
import re
import httpx
import logging

log = logging.getLogger(__name__)


class IreelClient:
    def __init__(self, api_key: Optional[str], api_base_url: str, assistant_id: str):
        """
        api_key: optional X-API-Key header
        api_base_url: e.g. "https://api.ireel.ai"
        assistant_id: e.g. "387665a3-6fc7-436b-a302-a7f707a7713a" (Top 3 chat)
        """
        self.api_key = api_key
        self.api_base_url = api_base_url.rstrip("/")
        self.assistant_id = assistant_id

    # ---------- HTTP core ----------

    def _build_url(self) -> str:
        if not self.api_base_url or not self.assistant_id:
            raise RuntimeError(
                "IreelClient is not configured with api_base_url or assistant_id"
            )
        return f"{self.api_base_url}/chat/{self.assistant_id}"

    def _post_chat(self, prompt: str, project_id: Optional[str]) -> str:
        """
        Low-level: send the prompt to iReel and return the response text.
        Uses the JSON shape that the .NET API expects: { "Prompt": "<text>" }.
        """
        if not prompt or not prompt.strip():
            raise ValueError("Prompt must be a non-empty string")

        url = self._build_url()

        headers: Dict[str, str] = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        params: Dict[str, str] = {}
        if project_id:
            params["projectId"] = project_id

        # IMPORTANT: API expects a top-level Prompt field
        body: Dict[str, Any] = {"Prompt": prompt}

        with httpx.Client(timeout=60.0) as client:
            try:
                resp = client.post(url, params=params, json=body, headers=headers)
                resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                # Log full response body to see WHY iReel is returning 4xx/5xx
                print(
                    "[IreelClient] HTTP error",
                    e.response.status_code,
                    "for URL",
                    e.request.url,
                )
                try:
                    print("[IreelClient] Response body:", e.response.text)
                except Exception:
                    print("[IreelClient] (no response text)")
                raise

            try:
                data = resp.json()
            except Exception:
                # Not JSON? Just return raw text.
                return resp.text

        # Try a few common response layouts, fall back to raw text.
        if isinstance(data, dict):
            # Simple "output" string
            if isinstance(data.get("output"), str):
                return data["output"]
            # Simple "message" string
            if isinstance(data.get("message"), str):
                return data["message"]
            # OpenAI-style choices
            if "choices" in data:
                try:
                    return data["choices"][0]["message"]["content"]
                except Exception:
                    pass

        return resp.text

    # ---------- Prompt building ----------

    @staticmethod
    def _get(obj: Any, key: str, default: Any = None) -> Any:
        """Helper: handle both dicts and Pydantic models."""
        if obj is None:
            return default
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    def build_prompt_for_race(
        self,
        meeting: Any,
        race: Any,
        scratchings: List[int],
        track_condition: Optional[str],
    ) -> str:
        """
        Build the text prompt iReel expects, including:

        - meetingId
        - meeting (track name + state)
        - date
        - raceNumber + race name
        - distance
        - SCRATCHINGS + TRACK CONDITION where available
        - Instructions for AI Best / Danger / Value format
        """

        meeting_date = self._get(meeting, "date")
        if isinstance(meeting_date, date):
            date_str = meeting_date.strftime("%d %b %Y")
        elif isinstance(meeting_date, str):
            date_str = meeting_date
        else:
            date_str = ""

        track_name = (
            self._get(meeting, "track_name", "")
            or self._get(meeting, "track", "")
            or ""
        )
        state = self._get(meeting, "state", "") or ""
        meeting_id = (
            self._get(meeting, "pf_meeting_id")
            or self._get(meeting, "meeting_id")
            or ""
        )

        race_number = (
            self._get(race, "race_number")
            or self._get(race, "race_no")
            or "?"
        )
        race_name = (
            self._get(race, "name")
            or self._get(race, "race_name")
            or ""
        )
        distance_m = (
            self._get(race, "distance_m")
            or self._get(race, "distance")
            or None
        )

        header_lines = [
            f"meetingId: {meeting_id}",
            f"meeting: {track_name}{f' ({state})' if state else ''}",
            f"date: {date_str}",
            f"raceNumber: {race_number} — {race_name}",
        ]
        if distance_m:
            header_lines.append(f"distance: {distance_m}m")

        lines: List[str] = header_lines
        lines.append("")  # blank line

        # Scratchings line if present
        if scratchings:
            scratch_str = ", ".join(str(x) for x in sorted(scratchings))
            lines.append(f"SCRATCHINGS (TAB NUMBERS): {scratch_str}")
            lines.append(
                "IMPORTANT: Treat these TAB numbers as SCRATCHED for all answers and calculations."
            )

        # Track condition line if present
        if track_condition:
            lines.append(f"TRACK CONDITION (official): {track_condition}")

        lines.append("")
        lines.append("Please reply with short mobile-length tips:")
        lines.append('• "AI Best: #<TAB> <Horse> — <one-sentence reason>"')
        lines.append('• "Danger: #<TAB> <Horse> — <one-sentence reason>"')
        lines.append('• "Value: #<TAB> <Horse> — <one-sentence reason>"')
        lines.append("No extra commentary.")

        return "\n".join(lines)

    # ---------- Parsing ----------

    def parse_tips_text(self, text: str) -> List[Dict[str, Any]]:
        """
        Parse iReel's text response into a list of tip dicts:

        Each dict matches the shape expected inside TipsBatchIn.races[].tips[]:
        {
            "tip_type": "AI_BEST" | "DANGER" | "VALUE",
            "tab_number": int,
            "horse_name": str,
            "reasoning": str,
            "stake_units": float,
        }
        """
        tips: List[Dict[str, Any]] = []

        if not text:
            return tips

        cleaned = text.strip()

        # Normalise escaped newlines that sometimes sneak through
        cleaned = cleaned.replace("\\n", "\n")

        # Decode escaped em-dash for matching
        cleaned = cleaned.replace("\\u2014", "—")

        # AI Best: stop the reason before a newline OR before "Danger:" or "Value:"
        ai_best_re = re.compile(
            r"AI\s*Best\s*:?\s*[#No\.\s]*(\d+)\s+(.+?)\s*[—\-–]\s*(.+?)(?=(?:\n|$|Danger\s*:|Value\s*:))",
            re.IGNORECASE | re.DOTALL,
        )

        # Danger: from label to newline / end or before "Value:"
        danger_re = re.compile(
            r"Danger\s*:?\s*[#No\.\s]*(\d+)\s+(.+?)\s*[—\-–]\s*(.+?)(?=(?:\n|$|Value\s*:))",
            re.IGNORECASE | re.DOTALL,
        )

        # Value: from label to the end
        value_re = re.compile(
            r"Value\s*:?\s*[#No\.\s]*(\d+)\s+(.+?)\s*[—\-–]\s*(.+)",
            re.IGNORECASE | re.DOTALL,
        )

        m_best = ai_best_re.search(cleaned)
        if m_best:
            tab = int(m_best.group(1))
            horse = self._clean_fragment(m_best.group(2))
            reason = self._clean_fragment(m_best.group(3))
            tips.append(
                {
                    "tip_type": "AI_BEST",
                    "tab_number": tab,
                    "horse_name": horse,
                    "reasoning": reason,
                    "stake_units": 1.0,
                }
            )

        m_danger = danger_re.search(cleaned)
        if m_danger:
            tab = int(m_danger.group(1))
            horse = self._clean_fragment(m_danger.group(2))
            reason = self._clean_fragment(m_danger.group(3))
            tips.append(
                {
                    "tip_type": "DANGER",
                    "tab_number": tab,
                    "horse_name": horse,
                    "reasoning": reason,
                    "stake_units": 1.0,
                }
            )

        m_value = value_re.search(cleaned)
        if m_value:
            tab = int(m_value.group(1))
            horse = self._clean_fragment(m_value.group(2))
            reason = self._clean_fragment(m_value.group(3))
            tips.append(
                {
                    "tip_type": "VALUE",
                    "tab_number": tab,
                    "horse_name": horse,
                    "reasoning": reason,
                    "stake_units": 1.0,
                }
            )

        return tips

    @staticmethod
    def _clean_fragment(s: str) -> str:
        """
        Normalise small text fragments coming out of regex groups:
        - strip markdown bold (**)
        - decode common escapes
        - trim stray quotes/braces
        """
        if s is None:
            return ""
        s = str(s)

        # Decode common escaped forms
        s = s.replace("\\u2014", "—")
        s = s.replace("\u2014", "—")

        # Strip markdown bold markers
        s = s.replace("**", "")

        # Collapse whitespace
        s = " ".join(s.split())

        # Trim obvious junk from ends
        return s.strip(' "\'}')

    # ---------- High-level: single race ----------

    def generate_race_tips(
        self,
        *,
        meeting: Any,
        race: Any,
        scratchings: List[int],
        track_condition: Optional[str],
        project_id: Optional[str],
    ) -> List[Dict[str, Any]]:
        """
        High-level wrapper per race:

        - Build prompt (meeting + race + scratchings + track condition)
        - Call iReel
        - Parse text into a list of tip dicts
        """
        prompt = self.build_prompt_for_race(
            meeting=meeting,
            race=race,
            scratchings=scratchings or [],
            track_condition=track_condition,
        )

        log.info(
            "[IreelClient] generate_race_tips project_id=%s meeting=%s race=%s",
            project_id,
            getattr(meeting, "track_name", meeting),
            getattr(race, "race_number", race),
        )

        raw_text = self._post_chat(prompt, project_id=project_id)
        return self.parse_tips_text(raw_text)
