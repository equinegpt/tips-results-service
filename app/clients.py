# app/clients.py
from __future__ import annotations

from .config import settings
from .ireel_client import IreelClient
from .gemini_client import GeminiClient

ireel_client = IreelClient(
    api_key=settings.ireel_api_key,
    api_base_url=settings.ireel_api_base_url,
    assistant_id=settings.ireel_assistant_id or "",
)

gemini_client = GeminiClient(
    api_url=settings.stablfy_api_url,
    username=settings.stablfy_username,
    password=settings.stablfy_password,
)
