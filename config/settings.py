"""
config/settings.py
All runtime configuration via Pydantic BaseSettings.
Values are loaded from environment variables or a .env file.
Never hardcode secrets — reference this module everywhere.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Snowflake ──────────────────────────────────────────────
    snowflake_account: str = Field(..., description="e.g. xy12345.us-east-1")
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str = "DE_WH"
    snowflake_database_dev: str = "DE_AI_AGENT_DEV"
    snowflake_database_prod: str = "DE_AI_AGENT_PROD"

    # Role per operation type
    snowflake_ingest_role: str = "DE_INGEST_ROLE"
    snowflake_transform_role: str = "DE_TRANSFORM_ROLE"
    snowflake_serving_role: str = "DE_SERVING_ROLE"

    # ── Environment ───────────────────────────────────────────
    dbt_target: str = Field(default="dev", description="dev or prod")

    @property
    def snowflake_database(self) -> str:
        return self.snowflake_database_prod if self.dbt_target == "prod" else self.snowflake_database_dev

    # ── LLM / Groq ────────────────────────────────────────────
    groq_api_key: str
    groq_model: str = "llama-3.3-70b-versatile"
    groq_max_tokens: int = 1024           # cap per call — free tier safety
    agent_history_limit: int = 10         # max messages in chat history
    agent_sql_row_limit: int = 20         # max rows returned to LLM from SQL

    # ── Paths ─────────────────────────────────────────────────
    project_root: str = "~/AI_Projects/de-ai-agent"
    chroma_persist_dir: str = "~/AI_Projects/de-ai-agent/.chroma"
    dbt_project_dir: str = "~/AI_Projects/de-ai-agent/dbt_project"
    dbt_profiles_dir: str = "~/AI_Projects/de-ai-agent/dbt_project"
    sqlite_db_path: str = "~/AI_Projects/de-ai-agent/data/products.db"
    superstore_csv_path: str = "~/AI_Projects/de-ai-agent/data/superstore_sales.csv"

    # ── Ingestion ─────────────────────────────────────────────
    open_meteo_latitude: float = 49.2497   # Vancouver, BC
    open_meteo_longitude: float = -123.1193
    open_meteo_lookback_days: int = 7      # incremental: days to fetch if no watermark
    dead_letter_alert_threshold: int = 50  # Slack alert if dead_letter rows exceed this

    # ── FastAPI ───────────────────────────────────────────────
    api_key: str = Field(..., description="X-API-Key header value")
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    streamlit_origin: str = "http://localhost:8501"

    # ── Slack ─────────────────────────────────────────────────
    slack_webhook_url: str = Field(default="", description="Slack incoming webhook URL")
    slack_alerts_enabled: bool = True

    # ── Prefect ───────────────────────────────────────────────
    prefect_api_url: str = "http://localhost:4200/api"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton — import and call this everywhere."""
    return Settings()
