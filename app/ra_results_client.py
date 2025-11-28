# ra_results_client.py
from __future__ import annotations

import os
from datetime import date
from typing import Any, Dict, List

import httpx

DEFAULT_BASE_URL = "https://ra-crawler.onrender.com"


class RAResultsClient:
    """
    Thin HTTP client for the RA-crawler /results endpoint.

    It expects either:
      - a bare JSON list: [ { ...row... }, ... ]
      - or a dict with 'results': [ { ...row... }, ... ]
    """

    def __init__(self, base_url: str | None = None, timeout: float = 10.0) -> None:
        self.base_url = base_url or os.getenv("RA_CRAWLER_BASE_URL", DEFAULT_BASE_URL)
        self.timeout = timeout

    def fetch_results_for_date(self, d: date) -> List[Dict[str, Any]]:
        url = self.base_url.rstrip("/") + "/results"
        params = {"date": d.isoformat()}

        with httpx.Client(timeout=self.timeout) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        # Handle a couple of reasonable shapes
        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            if isinstance(data.get("results"), list):
                return data["results"]
            if isinstance(data.get("items"), list):
                return data["items"]

        raise ValueError(f"Unexpected /results payload shape: {type(data)}")
