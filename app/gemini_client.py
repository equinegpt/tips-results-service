# app/gemini_client.py
"""
Gemini tip generation via the Stablfy API.

Flow: login → create conversation (kind=0) → poll for response → parse tips.
"""
from __future__ import annotations

import re
import time
import logging
from typing import Any, Dict, List, Optional

import httpx

log = logging.getLogger(__name__)

POLL_INTERVAL = 3     # seconds between polls
POLL_TIMEOUT = 120    # max wait for AI response


class GeminiClient:
    def __init__(
        self,
        api_url: str,
        username: str,
        password: str,
    ):
        self.api_url = api_url.rstrip("/")
        self.username = username
        self.password = password
        self.token: Optional[str] = None
        self.refresh_token: Optional[str] = None

    # ------------------------------------------------------------------ auth
    def login(self) -> None:
        """Authenticate with the Stablfy API and store tokens."""
        if not self.username or not self.password:
            raise RuntimeError(
                "GeminiClient: STABLFY_USERNAME and STABLFY_PASSWORD env vars "
                "must be set to generate Gemini tips"
            )
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(
                f"{self.api_url}/api/common/account/login",
                json={"userName": self.username, "password": self.password},
                headers=self._base_headers(),
            )
            resp.raise_for_status()
            data = resp.json()

        self.token = data["token"]
        self.refresh_token = data.get("refreshToken")
        log.info(
            "[GeminiClient] Logged in as %s (TTL %ss)",
            data.get("firstName", "?"),
            data.get("ttl", "?"),
        )

    def _base_headers(self) -> Dict[str, str]:
        h: Dict[str, str] = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://admin.stablfy.com",
            "Referer": "https://admin.stablfy.com/",
        }
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _refresh_login(self) -> None:
        """Re-authenticate if the token has expired."""
        log.info("[GeminiClient] Token expired, re-logging in...")
        self.login()

    def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Authenticated request with auto-retry on 401 and connection errors."""
        url = f"{self.api_url}{path}"
        kwargs.setdefault("timeout", 30.0)
        kwargs.setdefault("headers", self._base_headers())

        for attempt in range(3):
            try:
                with httpx.Client() as client:
                    resp = client.request(method, url, **kwargs)
                if resp.status_code == 401:
                    self._refresh_login()
                    kwargs["headers"] = self._base_headers()
                    with httpx.Client() as client:
                        resp = client.request(method, url, **kwargs)
                return resp
            except (httpx.ConnectError, httpx.ConnectTimeout) as e:
                if attempt < 2:
                    wait = (attempt + 1) * 5
                    log.warning(
                        "[GeminiClient] Connection error, retrying in %ss: %s",
                        wait, e,
                    )
                    time.sleep(wait)
                else:
                    raise

    # ---------------------------------------------------------- conversations
    def _create_conversation(self, title: str, initial_message: str) -> Dict[str, Any]:
        resp = self._request(
            "POST",
            "/api/admin/ai/conversations",
            json={"kind": 0, "title": title, "initialMessage": initial_message},
        )
        resp.raise_for_status()
        return resp.json()

    def _get_conversation(self, conv_id: int) -> Dict[str, Any]:
        resp = self._request("GET", f"/api/admin/ai/conversations/{conv_id}")
        resp.raise_for_status()
        return resp.json()

    def _delete_conversation(self, conv_id: int) -> None:
        try:
            self._request("DELETE", f"/api/admin/ai/conversations/{conv_id}")
        except Exception:
            pass  # best-effort cleanup

    def _poll_for_response(self, conv_id: int) -> Optional[str]:
        """Poll until the AI response is ready or timeout."""
        start = time.time()
        attempt = 0
        while time.time() - start < POLL_TIMEOUT:
            attempt += 1
            conv = self._get_conversation(conv_id)
            messages = conv.get("messages", [])

            assistant_msgs = [m for m in messages if m.get("role") == 1]
            if assistant_msgs:
                last = assistant_msgs[-1]
                status = last.get("status", 0)
                if status == 2:  # succeeded
                    elapsed = time.time() - start
                    log.info(
                        "[GeminiClient] AI responded in %.1fs (%d polls)",
                        elapsed, attempt,
                    )
                    return last.get("content", "")
                elif status == 3:  # failed
                    log.warning(
                        "[GeminiClient] AI FAILED: %s",
                        last.get("content", "unknown"),
                    )
                    return None

            time.sleep(POLL_INTERVAL)

        log.warning("[GeminiClient] TIMEOUT after %ds", POLL_TIMEOUT)
        return None

    # ---------------------------------------------------------- prompt + parse
    @staticmethod
    def _get(obj: Any, key: str, default: Any = None) -> Any:
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
        """Build the message sent to Gemini for a single race."""
        from datetime import date

        meeting_date = self._get(meeting, "date")
        if isinstance(meeting_date, date):
            date_str = meeting_date.isoformat()
        else:
            date_str = str(meeting_date) if meeting_date else ""

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
        race_name = self._get(race, "name") or self._get(race, "race_name") or ""
        distance_m = (
            self._get(race, "distance_m")
            or self._get(race, "distance")
            or None
        )

        msg = (
            f"meetingId: {meeting_id} "
            f"meeting: {track_name} ({state}) "
            f"date: {date_str} "
            f"raceNumber: {race_number} — {race_name} "
        )
        if distance_m:
            msg += f"distance: {distance_m}m"
        msg += "\n\n"

        if scratchings:
            tabs_str = ", ".join(f"#{t}" for t in sorted(scratchings))
            msg += f"SCRATCHED (do NOT tip these runners): {tabs_str}\n"
        if track_condition:
            msg += f"Track condition: {track_condition}\n"
        if scratchings or track_condition:
            msg += "\n"

        msg += (
            "Analyse this race and output AI Best, Danger, Value — "
            "3 lines only, format EXACTLY:\n"
            "AI Best: #<tab> <Horse> — <reasoning>\n"
            "Danger: #<tab> <Horse> — <reasoning>\n"
            "Value: #<tab> <Horse> — <reasoning>\n\n"
            "IMPORTANT: Always include the # tab number. "
            "Factor in each horse's record at this DISTANCE, this TRACK, "
            "and on today's CONDITIONS when making your picks. "
            "Cite sectional times/lengths, barrier, speedmap, margins, "
            "strike-rates. Write like a racing pundit."
        )
        return msg

    def parse_tips_text(self, text: str) -> List[Dict[str, Any]]:
        """
        Parse Gemini's 3-line response into tip dicts compatible with
        the TRS TipsBatchIn format.
        """
        tips: List[Dict[str, Any]] = []
        if not text:
            return tips

        lines = text.strip().split("\n")
        for line in lines:
            line = line.strip().lstrip("•*- ").strip().strip('"').strip()
            if not line:
                continue
            lower = line.lower()

            if lower.startswith("ai best:"):
                parsed = self._parse_pick_line(line)
                if parsed:
                    tips.append({**parsed, "tip_type": "AI_BEST"})
            elif lower.startswith("danger:"):
                parsed = self._parse_pick_line(line)
                if parsed:
                    tips.append({**parsed, "tip_type": "DANGER"})
            elif lower.startswith("value:"):
                parsed = self._parse_pick_line(line)
                if parsed:
                    tips.append({**parsed, "tip_type": "VALUE"})

        return tips

    def _parse_pick_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a single pick line into {tab_number, horse_name, reasoning, stake_units}."""
        try:
            _, rest = line.split(":", 1)
            rest = rest.strip().replace("**", "").replace("\t", " ")

            dash_pat = re.compile(r'\s*[—–]\s*|\s+-\s+')
            parts = dash_pat.split(rest, 1)
            horse_part = parts[0].strip()
            reasoning = parts[1].strip() if len(parts) > 1 else ""

            tab = None
            horse = None

            # #N Horse Name
            m = re.match(r'^#(\d+)\s+(.+)', horse_part)
            if m:
                tab = int(m.group(1))
                horse = m.group(2).strip()
            else:
                # N. Horse Name
                m = re.match(r'^(\d+)\.\s+(.+)', horse_part)
                if m:
                    tab = int(m.group(1))
                    horse = m.group(2).strip()
                else:
                    # Horse Name (N)
                    m = re.match(r'^(.+?)\s*\(#?(\d+)\)\s*$', horse_part)
                    if m:
                        horse = m.group(1).strip()
                        tab = int(m.group(2))
                    else:
                        horse = horse_part

            if tab is None:
                log.warning("[GeminiClient] No tab number found in: %r", line)
                return None

            return {
                "tab_number": tab,
                "horse_name": horse or "?",
                "reasoning": reasoning,
                "stake_units": 1.0,
            }
        except Exception as e:
            log.warning("[GeminiClient] Failed to parse line %r: %s", line, e)
            return None

    # ---------------------------------------------------------- high-level
    def generate_race_tips(
        self,
        *,
        meeting: Any,
        race: Any,
        scratchings: List[int],
        track_condition: Optional[str],
    ) -> List[Dict[str, Any]]:
        """
        Generate Gemini tips for a single race.

        1. Build prompt
        2. Create conversation (kind=0, pre-race)
        3. Poll for response
        4. Parse 3-line output
        5. Cleanup conversation
        """
        track_name = (
            self._get(meeting, "track_name", "")
            or self._get(meeting, "track", "")
            or "?"
        )
        race_number = (
            self._get(race, "race_number")
            or self._get(race, "race_no")
            or "?"
        )

        prompt = self.build_prompt_for_race(
            meeting=meeting,
            race=race,
            scratchings=scratchings or [],
            track_condition=track_condition,
        )

        log.info(
            "[GeminiClient] Generating tips for %s R%s",
            track_name, race_number,
        )

        # Ensure we're logged in
        if not self.token:
            self.login()

        meeting_date = self._get(meeting, "date", "")
        title = f"Cron: {track_name} R{race_number} {meeting_date}"

        conv = self._create_conversation(title=title, initial_message=prompt)
        conv_id = conv.get("id")
        if not conv_id:
            log.error("[GeminiClient] Failed to create conversation")
            return []

        try:
            response_text = self._poll_for_response(conv_id)
            if not response_text:
                return []

            tips = self.parse_tips_text(response_text)
            if not tips:
                log.warning(
                    "[GeminiClient] No tips parsed from: %s",
                    response_text[:200],
                )
            return tips
        finally:
            # Clean up conversation to avoid clutter
            self._delete_conversation(conv_id)
