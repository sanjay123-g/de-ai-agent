-- =============================================================
-- DE AI Agent — DuckDB Setup (idempotent)
-- Run once against data/fifa.duckdb before first ingestion.
-- Replaces snowflake/setup.sql — only the two tables ddl_agent.py
-- does NOT create dynamically (per-source RAW_* tables are created
-- at runtime by ddl_agent.py; these two are pipeline infrastructure).
-- =============================================================

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- Dead letter (contract violations from all sources)
CREATE TABLE IF NOT EXISTS BRONZE.DEAD_LETTER (
    _dl_id          VARCHAR       NOT NULL,
    _ingested_at    TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    _batch_id       VARCHAR,
    _source         VARCHAR,
    _error_type     VARCHAR,
    _error_detail   VARCHAR,
    _raw_payload    VARCHAR       -- JSON stored as string, not native VARIANT
);

-- Watermark table (tracks incremental extraction state)
CREATE TABLE IF NOT EXISTS BRONZE.INGESTION_WATERMARKS (
    source_name      VARCHAR      NOT NULL PRIMARY KEY,
    last_loaded_at   TIMESTAMP,
    last_batch_id    VARCHAR,
    rows_loaded      INTEGER,
    updated_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);
