"""Application settings, loaded from environment variables (12-factor)."""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "event-platform"
    log_level: str = "INFO"

    admin_api_key: SecretStr | None = None
    readiness_check_timeout_seconds: float = Field(default=1.0, gt=0)

    rate_limit_enabled: bool = True
    rate_limit_requests: int = Field(default=600, gt=0)
    rate_limit_window_seconds: int = Field(default=60, gt=0)

    mongo_url: str = "mongodb://localhost:27017"
    mongo_database: str = "event_platform"
    mongo_events_collection: str = "events"
    mongo_server_selection_timeout_ms: int = Field(default=5_000, gt=0)

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index: str = "events"
    elasticsearch_shards: int = Field(default=1, gt=0)
    elasticsearch_replicas: int = Field(default=0, ge=0)
    elasticsearch_request_timeout_seconds: float = Field(default=5.0, gt=0)

    redis_url: str = "redis://localhost:6379/0"
    redis_key_prefix: str = "event-platform"
    redis_socket_timeout_seconds: float = Field(default=0.5, gt=0)
    realtime_stats_ttl_seconds: int = Field(default=10, gt=0)
    realtime_stats_window_seconds: int = Field(default=3_600, gt=0)

    queue_max_size: int = Field(default=10_000, gt=0)
    queue_visibility_timeout_seconds: float = Field(default=30.0, gt=0)
    queue_max_receive_count: int = Field(default=5, gt=0)

    worker_enabled: bool = True
    worker_batch_size: int = Field(default=10, gt=0)
    worker_poll_wait_seconds: float = Field(default=1.0, ge=0)
    retry_base_delay_seconds: float = Field(default=1.0, gt=0)
    retry_max_delay_seconds: float = Field(default=60.0, gt=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()
