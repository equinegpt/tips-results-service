# app/ra_results_client.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Any

import httpx


@dataclass
class RAResultRow:
    meeting_date: date
    state: str
    track: str
    race_no: int
    tab_number: int
    horse_name: str
    finishing_pos: Optional[int]
    is_scratched: bool
    margin_lens: Optional[float]
    starting_price: Optional[float]
    trainer: Optional[str] = None
    jockey: Optional[str] = None

class RAResultsClient:
    """
    Thin client over the RA Crawler /results endpoint.

      GET {base}/results?date=YYYY-MM-DD

    This is tolerant of small schema differences
    (race_no vs raceNo, starting_price vs startingPrice, etc.)
    """

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 20.0,
    ) -> None:
        self.base_url = (
            base_url
            or os.getenv("RA_CRAWLER_BASE_URL", "https://ra-crawler.onrender.com")
        ).rstrip("/")
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    # ---------- private helpers ----------

    @staticmethod
    def _get_num(d: dict, *keys: str) -> Optional[float]:
        for k in keys:
            if k in d and d[k] is not None:
                try:
                    return float(d[k])
                except (TypeError, ValueError):
                    continue
        return None

    @staticmethod
    def _get_int(d: dict, *keys: str) -> Optional[int]:
        for k in keys:
            if k in d and d[k] is not None:
                try:
                    return int(d[k])
                except (TypeError, ValueError):
                    continue
        return None

    # ---------- public API ----------

    def fetch_results_for_date(self, d: date) -> List[RAResultRow]:
        url = f"{self.base_url}/results"
        resp = self._client.get(url, params={"date": d.isoformat()})
        resp.raise_for_status()
        data: Any = resp.json()

        # Our RA API returns a bare list; if we ever wrap later,
        # this keeps things robust.
        if isinstance(data, dict):
            items = data.get("results") or data.get("rows") or data.get("items") or []
        else:
            items = data

        rows: List[RAResultRow] = []

        for item in items:
            state = (item.get("state") or "").upper()
            track = item.get("track") or ""
            if not state or not track:
                continue

            race_no = self._get_int(item, "race_no", "raceNo", "race_number")
            tab_number = self._get_int(item, "horse_number", "horseNumber", "tabNumber")
            if race_no is None or tab_number is None:
                continue

            finishing_pos = self._get_int(item, "finishing_pos", "finishingPos")
            is_scratched = bool(
                item.get("is_scratched")
                or item.get("isScratched")
                or False
            )

            margin_lens = self._get_num(item, "margin_lens", "marginLens")
            starting_price = self._get_num(
                item, "starting_price", "startingPrice", "sp"
            )

            horse_name = (
                item.get("horse_name")
                or item.get("horseName")
                or f"#{tab_number}"
            )

            trainer = item.get("trainer") or item.get("trainerName") or None
            jockey = item.get("jockey") or item.get("jockeyName") or None

            rows.append(
                RAResultRow(
                    meeting_date=d,
                    state=state,
                    track=track,
                    race_no=race_no,
                    tab_number=tab_number,
                    horse_name=horse_name,
                    finishing_pos=finishing_pos,
                    is_scratched=is_scratched,
                    margin_lens=margin_lens,
                    starting_price=starting_price,
                    trainer=trainer,
                    jockey=jockey,
                )
            )

        return rows
