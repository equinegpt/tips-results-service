# app/clients.py
from __future__ import annotations

from .config import settings
from .ireel_client import IreelClient

ireel_client = IreelClient(
    api_key=settings.ireel_api_key,
    api_base_url=settings.ireel_api_base_url,
    assistant_id=settings.ireel_assistant_id or "",
)
