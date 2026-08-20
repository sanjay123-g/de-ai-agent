"""
config/settings.py
==================
Central Pydantic settings — reads from .env file and environment variables.
All credentials and configuration for Snowflake, Ollama, dbt, and FastAPI.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Snowflake ──────────────────────────────────────────────────────────
    snowflake_account:   str = ""
    snowflake_user:      str = ""
    snowflake_password:  str = ""
    snowflake_database:  str = "DE_AI_AGENT_DEV"
    snowflake_schema:    str = "BRONZE"
    snowflake_warehouse: str = "DE_WH"
    snowflake_role:      str = "DE_INGEST_ROLE"

    # ── DuckDB ────────────────────────────────────────────────────────────
    duckdb_path: str = "./data/fifa.duckdb"

    # ── dbt ───────────────────────────────────────────────────────────────
    dbt_target: str = "dev"
    dbt_project_dir: str = "./dbt_project"

    # ── Data source paths ────────────────────────────────────────────────
    superstore_csv_path: str = "./data/superstore.csv"
    sqlite_db_path: str = "./data/source.db"
    open_meteo_latitude: float = 49.2827
    open_meteo_longitude: float = -123.1207
    open_meteo_lookback_days: int = 7

    # ── Ollama (local LLM) ────────────────────────────────────────────────
    ollama_model:       str = "qwen2.5-coder:14b"
    ollama_embed_model: str = "nomic-embed-text"
    ollama_base_url:    str = "http://localhost:11434"

    # ── FastAPI ───────────────────────────────────────────────────────────
    api_key: str = "deagent-local-key-2026"

    # ── Optional: Slack alerts ────────────────────────────────────────────
    slack_webhook_url: str = ""

    # ── Optional: GitHub PR ───────────────────────────────────────────────
    github_token: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"           # silently ignore unknown .env keys


settings = Settings()


def get_settings() -> Settings:
    return settings
