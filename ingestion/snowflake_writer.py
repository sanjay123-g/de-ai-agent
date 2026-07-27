"""
ingestion/snowflake_writer.py

Shared Snowflake utilities used by the generic ingestor:
  - connection management
  - DYNAMIC table creation from a Pydantic contract (no hand-written DDL)
  - bulk write via write_pandas
  - dead letter routing
  - watermark read/write for incremental loads

This replaces the hand-written CREATE TABLE statements per source from
the original design — table schema is inferred from the Pydantic model.
"""

from __future__ import annotations
import json
from datetime import datetime
from typing import Optional, Type

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from pydantic import BaseModel
import structlog

from config.settings import get_settings

log = structlog.get_logger()

# Maps Python/Pydantic field types to Snowflake column types.
# This is the schema-inference layer — extend this dict to support
# new Python types as needed.
_TYPE_MAP = {
    str: "VARCHAR(1000)",
    int: "NUMBER(38,0)",
    float: "FLOAT",
    bool: "BOOLEAN",
    datetime: "TIMESTAMP_NTZ",
}


def get_connection(role: str) -> snowflake.connector.SnowflakeConnection:
    """Opens a Snowflake connection under the given role.
    Role is passed explicitly so callers always use least-privilege —
    ingestion always connects as DE_INGEST_ROLE, never a broader role."""
    s = get_settings()
    return snowflake.connector.connect(
        account=s.snowflake_account,
        user=s.snowflake_user,
        password=s.snowflake_password,
        warehouse=s.snowflake_warehouse,
        database=s.snowflake_database,
        role=role,
    )


def _infer_snowflake_schema(contract: Type[BaseModel]) -> dict[str, str]:
    """Reads a Pydantic model's field types and maps them to Snowflake
    column types. This is what lets Bronze tables get created automatically
    instead of being hand-written in a setup.sql file."""
    columns = {}
    for field_name, field_info in contract.model_fields.items():
        py_type = field_info.annotation
        # Unwrap Optional[X] -> X for type mapping purposes
        if hasattr(py_type, "__args__"):
            py_type = next((t for t in py_type.__args__ if t is not type(None)), str)
        sf_type = _TYPE_MAP.get(py_type, "VARCHAR(1000)")
        columns[field_name] = sf_type
    return columns


def create_table_if_not_exists(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    contract: Type[BaseModel],
) -> None:
    """Auto-creates a Bronze table from a Pydantic contract's schema.
    Adds standard metadata columns (_row_id, _batch_id, _ingested_at)
    on top of the contract's own fields. Idempotent — safe to call every run."""
    columns = _infer_snowflake_schema(contract)
    col_defs = ",\n    ".join(f'"{name}" {sf_type}' for name, sf_type in columns.items())

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "_row_id" VARCHAR(64),
        "_batch_id" VARCHAR(64),
        "_ingested_at" TIMESTAMP_NTZ,
        {col_defs}
    )
    """
    cur = conn.cursor()
    cur.execute(ddl)
    cur.close()
    log.info("table_ensured", table=table_name, columns=list(columns.keys()))


def write_dataframe(
    conn: snowflake.connector.SnowflakeConnection,
    df: pd.DataFrame,
    table_name: str,
) -> int:
    """Bulk writes a DataFrame to Snowflake using write_pandas — far faster
    than row-by-row INSERT for batch loads. Returns rows written."""
    # write_pandas expects uppercase column names matching Snowflake's
    # default identifier casing
    df.columns = [c.upper() for c in df.columns]
    success, _, nrows, _ = write_pandas(
        conn, df, table_name.split(".")[-1].upper(),
        database=table_name.split(".")[0] if "." in table_name else None,
        schema=table_name.split(".")[1] if table_name.count(".") >= 1 else None,
        auto_create_table=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {table_name}")
    log.info("rows_written", table=table_name, rows=nrows)
    return nrows


def write_dead_letter(
    conn: snowflake.connector.SnowflakeConnection,
    source: str,
    batch_id: str,
    error_type: str,
    error_detail: str,
    raw_payload: dict,
) -> None:
    """Routes a row that failed Pydantic validation to the dead letter table
    instead of silently dropping it. Stores the original payload as JSON
    (VARIANT column) so it can be inspected and reprocessed later."""
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO BRONZE.DEAD_LETTER
            (_batch_id, _source, _error_type, _error_detail, _raw_payload)
        SELECT %s, %s, %s, %s, PARSE_JSON(%s)
        """,
        (batch_id, source, error_type, error_detail, json.dumps(raw_payload, default=str)),
    )
    cur.close()


def get_watermark(conn: snowflake.connector.SnowflakeConnection, source_name: str) -> Optional[datetime]:
    """Reads the last successful load timestamp for a source. Used to fetch
    only NEW data on incremental runs instead of reprocessing everything."""
    cur = conn.cursor()
    cur.execute(
        "SELECT last_loaded_at FROM BRONZE.INGESTION_WATERMARKS WHERE source_name = %s",
        (source_name,),
    )
    row = cur.fetchone()
    cur.close()
    return row[0] if row else None


def update_watermark(
    conn: snowflake.connector.SnowflakeConnection,
    source_name: str,
    batch_id: str,
    rows_loaded: int,
) -> None:
    """Updates the watermark after a successful load — MERGE pattern so it
    works whether or not the source already has a watermark row."""
    cur = conn.cursor()
    cur.execute(
        """
        MERGE INTO BRONZE.INGESTION_WATERMARKS t
        USING (SELECT %s AS source_name) s
        ON t.source_name = s.source_name
        WHEN MATCHED THEN UPDATE SET
            last_loaded_at = CURRENT_TIMESTAMP(),
            last_batch_id = %s,
            rows_loaded = %s,
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (source_name, last_loaded_at, last_batch_id, rows_loaded, updated_at)
            VALUES (%s, CURRENT_TIMESTAMP(), %s, %s, CURRENT_TIMESTAMP())
        """,
        (source_name, batch_id, rows_loaded, source_name, batch_id, rows_loaded),
    )
    cur.close()
