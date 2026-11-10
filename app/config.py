"""Runtime configuration, read from environment variables prefixed with ``CP_``."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CP_")

    # Local-development defaults only; real deployments inject these via the environment.
    database_url: str = (
        "postgresql+asyncpg://control_plane:control_plane@localhost:5432/control_plane"
    )
    amqp_url: str = "amqp://control_plane:control_plane@localhost:5672/"

    db_pool_size: int = 20
    db_max_overflow: int = 10

    # Outbox relay: how many rows to publish per transaction and how long to sleep when idle.
    # The relay is woken immediately by Postgres NOTIFY; polling is only the fallback.
    outbox_batch_size: int = 100
    outbox_poll_interval_s: float = 1.0

    # Progress consumer
    consumer_prefetch: int = 16
    consumer_max_attempts: int = 5

    # Start the outbox relay and progress consumer alongside the HTTP API.
    run_background_workers: bool = True

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
