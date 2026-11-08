"""Core configuration module using Pydantic Settings."""
from pydantic_settings import BaseSettings
from pydantic import field_validator
from typing import Optional


class Settings(BaseSettings):
    """Centralized application settings loaded from environment variables."""

    # Application
    app_env: str = "development"
    app_name: str = "reliable-webhook-platform"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    # Database
    database_url: str = "postgresql+psycopg://webhook_user:webhook_pass@localhost:5432/webhook_db"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Celery
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_task_always_eager: bool = False

    # Outbox Dispatcher
    outbox_poll_interval_seconds: int = 2
    outbox_batch_size: int = 50

    # Delivery HTTP timeouts
    delivery_connect_timeout_seconds: int = 5
    delivery_read_timeout_seconds: int = 10
    delivery_processing_lease_timeout_seconds: int = 30

    # Event payload size limit (bytes)
    max_event_payload_bytes: int = 1_048_576  # 1 MB

    # Demo tenant
    default_tenant_name: str = "demo-tenant"
    demo_api_key: Optional[str] = None

    # Security: allowed internal hostnames for test destinations
    allow_local_test_destinations: str = ""

    # Logging
    log_level: str = "INFO"

    # ------------------------------------------------------------------ #
    # Retry policy                                                         #
    # ------------------------------------------------------------------ #
    retry_max_attempts: int = 5  # includes the initial attempt
    retry_base_delay_seconds: float = 2.0
    retry_max_delay_seconds: float = 300.0
    retry_jitter_enabled: bool = True

    # ------------------------------------------------------------------ #
    # Rate limiting (Phase 2.3 — columns added now, full impl later)       #
    # ------------------------------------------------------------------ #
    default_rate_limit_per_second: float = 10.0
    default_rate_limit_burst: int = 20

    # ------------------------------------------------------------------ #
    # Circuit breaker (Phase 2.4 — add now, implement later)              #
    # ------------------------------------------------------------------ #
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_observation_window_seconds: int = 60
    circuit_breaker_open_duration_seconds: int = 60
    circuit_breaker_half_open_probes: int = 2
    circuit_breaker_success_threshold: int = 2

    # ------------------------------------------------------------------ #
    # Metrics                                                              #
    # ------------------------------------------------------------------ #
    metrics_enabled: bool = True

    @property
    def allowed_test_hosts(self) -> list[str]:
        """Parse comma-separated allowed test hostnames."""
        if not self.allow_local_test_destinations:
            return []
        return [h.strip() for h in self.allow_local_test_destinations.split(",") if h.strip()]

    @property
    def is_development(self) -> bool:
        return self.app_env in ("development", "test")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


# Singleton settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Return the cached settings singleton."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Reset settings singleton (for testing)."""
    global _settings
    _settings = None
