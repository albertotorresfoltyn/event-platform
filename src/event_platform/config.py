"""Application settings, loaded from environment variables (12-factor)."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "event-platform"
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
