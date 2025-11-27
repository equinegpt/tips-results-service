# app/ra_results_client.py
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import List, Optional

import requests


RA_CRAWLER_BASE_URL = os.getenv(
    "RA_CRAWLER_BASE_URL",
    "https://ra-crawler.onrender.com",
)


@dataclass
class RAResultRow:
    meeting_date: date
    state: str
    track: str
    race_no: int
    horse_number: int
    horse_name: str
    finishing_pos: Optional[int]
    is_scratched: bool
    margin_lens: Optional[float]
    starting_price: Optional[float]

    @classmethod
    def from_json(cls, payload: dict) -> "RAResultRow":
        return cls(
            meeting_date=date.fromisoformat(payload["meeting_date"]),
            state=payload["state"],
            track=payload["track"],
            race_no=payload["race_no"],
            horse_number=payload["horse_number"],
            horse_name=payload["horse_name"],
            finishing_pos=payload.get("finishing_pos"),
            is_scratched=payload.get("is_scratched", False),
            margin_lens=payload.get("margin_lens"),
            starting_price=payload.get("starting_price"),
        )


def fetch_results_for_date(meeting_date: date) -> List[RAResultRow]:
    url = f"{RA_CRAWLER_BASE_URL}/results"
    params = {"date": meeting_date.isoformat()}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return [RAResultRow.from_json(row) for row in data]
