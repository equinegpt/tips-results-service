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

    # Pydantic settings config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",   # don't blow up on unrelated env vars
    )


settings = Settings()
