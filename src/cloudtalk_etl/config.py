# src/cloudtalk_etl/config.py
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """
    Type-safe configuration loaded from environment variables (or .env file).

    All secrets (API keys, DATABASE_URL) must be provided via environment variables.
    Optional fields have sensible defaults aligned with the technical specification.
    """

    # CloudTalk API
    cloudtalk_api_key_id: str
    cloudtalk_api_key_secret: str
    cloudtalk_api_base_url: str = "https://my.cloudtalk.io/api"
    cloudtalk_analytics_api_base_url: str = "https://analytics-api.cloudtalk.io/api"

    # Database
    database_url: str  # Full Neon connection string with sslmode=require

    # ETL Settings
    rate_limit_rpm: int = Field(default=50, ge=1, le=55)
    log_level: str = "INFO"
    etl_date_override: str | None = None  # YYYY-MM-DD format, None = yesterday
    test_mode: bool = False  # Fetch only 1 page / sample of records per endpoint for quick dry runs
    test_sample_size: int = 50  # Number of call details to fetch in test_mode

    # Retry settings
    max_retries: int = 5
    retry_base_wait: float = 2.0   # seconds
    retry_max_wait: float = 60.0   # seconds

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        # Silently ignore env vars not declared here (e.g. CRON_SCHEDULE, TZ —
        # those are consumed by Docker / supercronic, not Python).
        "extra": "ignore",
    }
