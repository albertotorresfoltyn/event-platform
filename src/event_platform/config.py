"""Application settings, loaded from environment variables (12-factor)."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "event-platform"
    log_level: str = "INFO"

    queue_max_size: int = Field(default=10_000, gt=0)
    queue_visibility_timeout_seconds: float = Field(default=30.0, gt=0)
    queue_max_receive_count: int = Field(default=5, gt=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
