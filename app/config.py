# app/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Core app config
    app_name: str = "Tips & Results Service"
    database_url: str = "sqlite:///./tips_results.db"
    environment: str = "local"

    # RA crawler + PF scratchings services
    ra_crawler_base_url: str = "https://ra-crawler.onrender.com"
    pf_scratchings_base_url: str = "https://pf-scratchings-conditions.onrender.com"

    # iReel config
    ireel_api_key: str | None = None
    ireel_api_base_url: str = "https://api.ireel.ai"
    ireel_assistant_id: str | None = None

    # Gemini (via Stablfy API) config
    stablfy_api_url: str = "https://api.stablfy.com"
    stablfy_username: str = ""
    stablfy_password: str = ""

    # Default tip source for app-facing endpoints (/tips, /stats/day,
    # /stats/range, /debug/day-summary). Flip this to "iReel" via env var to
    # temporarily route the apps away from Gemini (e.g. during a Gemini
    # outage or credit gap). Restore to "Gemini" when ready.
    tips_default_source: str = "Gemini"

    # Outage alias for /tips: when set, GET /tips?source=Gemini is rewritten
    # to the alias value (e.g. "iReel"). Lets us redirect existing app
    # builds that hardcode source=Gemini without shipping a client release.
    # Unset (None) = no rewrite, default behaviour.
    gemini_alias_source: str | None = None

    # Pydantic settings config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",   # don't blow up on unrelated env vars
    )


settings = Settings()
