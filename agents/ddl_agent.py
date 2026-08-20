"""agents/ddl_agent.py - DuckDB version. Was Snowflake, migrated."""
from __future__ import annotations
import datetime, decimal
from typing import Any
import duckdb
import structlog
from tenacity import retry, stop_after_attempt, wait_fixed
from agents.state import AgentState
from config.settings import settings

logger = structlog.get_logger()

_PY_TO_DUCKDB: dict[Any, str] = {
    str: "VARCHAR", int: "BIGINT", float: "DOUBLE", bool: "BOOLEAN",
    datetime.datetime: "TIMESTAMP", datetime.date: "DATE", datetime.time: "TIME",
    decimal.Decimal: "DECIMAL(38, 9)", bytes: "BLOB", bytearray: "BLOB",
    dict: "JSON", list: "JSON", type(None): "VARCHAR",
}

_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("_INGESTED_AT", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
    ("_SOURCE_NAME", "VARCHAR"),
    ("_PIPELINE_RUN_ID", "VARCHAR"),
]

def _derive_table_name(source_name: str) -> str:
    safe = source_name.upper().replace("-", "_").replace(" ", "_").replace(".", "_")
    return f"RAW_{safe}"

def _derive_secondary_table_name(source_name: str) -> str:
    safe = source_name.upper().replace("-", "_").replace(" ", "_").replace(".", "_")
    return f"RAW_{safe}_SECONDARY"

def _build_full_table_name(table_name: str) -> str:
    if "." in table_name:
        return table_name
    return f"BRONZE.{table_name}"

def build_create_table_ddl(table_name, schema_map, include_audit_cols=True) -> str:
    col_defs = []
    for col_name, py_type in schema_map.items():
        duck_type = _PY_TO_DUCKDB.get(py_type, "VARCHAR")
        safe_col = col_name.upper().replace(" ", "_").replace("-", "_").replace(".", "_")
        col_defs.append(f"    {safe_col} {duck_type}")
    if include_audit_cols:
        for col_name, col_def in _AUDIT_COLUMNS:
            col_defs.append(f"    {col_name} {col_def}")
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n" + ",\n".join(col_defs) + "\n);"

def _get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(settings.duckdb_path)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def _execute_ddl(ddl_sql, source_name, table_name) -> None:
    conn = _get_connection()
    try:
        logger.info("ddl_executing", source=source_name, op="ddl", table=table_name)
        conn.execute(ddl_sql)
        logger.info("ddl_done", source=source_name, op="ddl", table=table_name)
    finally:
        conn.close()

def _get_existing_columns(table_name, source_name) -> set[str]:
    schema, _, bare_table = table_name.rpartition(".")
    schema = schema or "main"
    conn = _get_connection()
    try:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
            (schema, bare_table),
        ).fetchall()
        return {row[0].upper() for row in rows}
    finally:
        conn.close()

def _apply_schema_drift(table_name, schema_map, source_name) -> list[str]:
    existing_cols = _get_existing_columns(table_name, source_name)
    if not existing_cols:
        return []
    alters = []
    conn = _get_connection()
    try:
        for col_name, py_type in schema_map.items():
            safe_col = col_name.upper().replace(" ", "_").replace("-", "_").replace(".", "_")
            if safe_col not in existing_cols:
                duck_type = _PY_TO_DUCKDB.get(py_type, "VARCHAR")
                sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {safe_col} {duck_type};"
                conn.execute(sql)
                alters.append(sql)
                logger.warning("schema_drift_detected", source=source_name, table=table_name, new_col=safe_col, duck_type=duck_type)
    finally:
        conn.close()
    return alters

def ddl_agent_node(state: AgentState) -> dict[str, Any]:
    source_name = state["source_name"]
    schema_map = state.get("schema_map")
    if not schema_map:
        return {"error": f"ddl_agent: schema_map empty for '{source_name}' — schema_agent must run first"}
    logger.info("ddl_agent_start", source=source_name)
    try:
        raw_table = state.get("target_table") or _derive_table_name(source_name)
        target_table = _build_full_table_name(raw_table)
        primary_ddl = build_create_table_ddl(target_table, schema_map)
        _execute_ddl(primary_ddl, source_name, target_table)
        _apply_schema_drift(target_table, schema_map, source_name)

        secondary_target = None
        secondary_schema = state.get("secondary_schema_map")
        if secondary_schema:
            raw_secondary = state.get("secondary_target_table") or _derive_secondary_table_name(source_name)
            secondary_target = _build_full_table_name(raw_secondary)
            secondary_ddl = build_create_table_ddl(secondary_target, secondary_schema)
            _execute_ddl(secondary_ddl, source_name, secondary_target)
            _apply_schema_drift(secondary_target, secondary_schema, source_name)
            logger.info("ddl_secondary_done", source=source_name, table=secondary_target)

        logger.info("ddl_agent_complete", source=source_name, primary=target_table, secondary=secondary_target)
        return {
            "target_table": target_table,
            "secondary_target_table": secondary_target,
            "ddl_sql": primary_ddl,
            "table_created": True,
            "status": "ddl_done",
            "error": None,
        }
    except Exception as e:
        logger.error("ddl_agent_error", source=source_name, error=str(e))
        return {"error": f"ddl_agent failed [{source_name}]: {str(e)}"}
