"""
agents/ddl_agent.py
===================
LangGraph node: generates and executes Snowflake DDL for ANY data source.

FULLY DYNAMIC — no hardcoded table names, no domain-specific logic.
Add a new source to SOURCE_REGISTRY in supervisor.py and this agent
handles it automatically without any changes here.

TABLE NAMING CONVENTION (automatic):
  Primary table:   {DB}.BRONZE.RAW_{source_name.upper()}
  Secondary table: {DB}.BRONZE.RAW_{source_name.upper()}_SECONDARY

  Override either via SOURCE_REGISTRY fields:
    table_name           → custom primary table name
    secondary_table_name → custom secondary table name

  Examples:
    source_name="sales_orders"    → RAW_SALES_ORDERS
    source_name="worldcup_api"    → RAW_WORLDCUP_API  (default)
                                  → RAW_WC2026_MATCHES (with override)

PYTHON → SNOWFLAKE TYPE MAPPING (comprehensive):
  Covers all types the schema inferrer can produce, plus semi-structured
  types for future extension.

SCHEMA DRIFT HANDLING:
  If table already exists, detects new columns and issues
  ALTER TABLE ADD COLUMN IF NOT EXISTS — preserves existing data.

QUERY TAGGING:
  Every Snowflake session is tagged: agent=ddl_agent,source=<source_name>
  Enables cost attribution per source in QUERY_HISTORY.
"""

from __future__ import annotations

import datetime
import decimal
from typing import Any

import snowflake.connector
import structlog
from tenacity import retry, stop_after_attempt, wait_fixed

from agents.state import AgentState
from config.settings import settings

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# COMPREHENSIVE PYTHON → SNOWFLAKE TYPE MAP
# Covers every type the schema inferrer can return, plus common Python
# types that may appear in future source extensions.
# ---------------------------------------------------------------------------

_PY_TO_SF: dict[Any, str] = {
    # Primitives — core inferrer output types
    str:              "VARCHAR",
    int:              "NUMBER(38, 0)",    # explicit max-precision integer
    float:            "FLOAT",
    bool:             "BOOLEAN",

    # Date / time — inferrer returns str for these at Bronze layer.
    # Included here so Silver models or future inferrer upgrades work.
    datetime.datetime: "TIMESTAMP_NTZ",  # no timezone — store UTC explicitly
    datetime.date:     "DATE",
    datetime.time:     "TIME",

    # Numeric precision — for financial/scientific sources
    decimal.Decimal:  "NUMBER(38, 9)",   # 9 decimal places for monetary values

    # Binary
    bytes:            "BINARY",
    bytearray:        "BINARY",

    # Semi-structured — for sources that contain nested dicts/lists
    # (after flattening, these shouldn't appear in Bronze; included as safety net)
    dict:             "VARIANT",
    list:             "VARIANT",

    # Null / unknown — fallback for columns where all sampled values were None
    type(None):       "VARCHAR",         # safe default; Silver will cast
}

_AUDIT_COLUMNS: list[tuple[str, str]] = [
    ("_INGESTED_AT",     "TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()"),
    ("_SOURCE_NAME",     "VARCHAR"),
    ("_PIPELINE_RUN_ID", "VARCHAR"),
]


# ---------------------------------------------------------------------------
# DYNAMIC TABLE NAME BUILDER
# ---------------------------------------------------------------------------

def _derive_table_name(source_name: str) -> str:
    """
    Derives Bronze table name from source_name.
    Convention: RAW_ + source_name uppercased, with special chars → underscore.

    Works for any source name:
      "sales_orders"     → "RAW_SALES_ORDERS"
      "historical_results" → "RAW_HISTORICAL_RESULTS"
      "worldcup_api"     → "RAW_WORLDCUP_API"
      "user-events"      → "RAW_USER_EVENTS"
      "iot sensor data"  → "RAW_IOT_SENSOR_DATA"
    """
    safe = source_name.upper().replace("-", "_").replace(" ", "_").replace(".", "_")
    return f"RAW_{safe}"


def _derive_secondary_table_name(source_name: str) -> str:
    """
    Derives secondary Bronze table name.
    Convention: RAW_ + source_name + _SECONDARY
    Override via SOURCE_REGISTRY["secondary_table_name"] for custom names
    (e.g. worldcup_api → RAW_WC2026_GOALS instead of RAW_WORLDCUP_API_SECONDARY).
    """
    safe = source_name.upper().replace("-", "_").replace(" ", "_").replace(".", "_")
    return f"RAW_{safe}_SECONDARY"


def _build_full_table_name(table_name: str) -> str:
    """Prepends database and schema to produce a fully-qualified name."""
    db     = settings.snowflake_database   # DE_AI_AGENT_DEV / DE_AI_AGENT_PROD
    schema = "BRONZE"
    # If table_name already includes DB.SCHEMA, return as-is
    if table_name.count(".") >= 2:
        return table_name
    if table_name.count(".") == 1:
        return f"{db}.{table_name}"
    return f"{db}.{schema}.{table_name}"


# ---------------------------------------------------------------------------
# DDL BUILDER — domain-agnostic
# ---------------------------------------------------------------------------

def build_create_table_ddl(
    table_name: str,
    schema_map: dict[str, type],
    include_audit_cols: bool = True,
) -> str:
    """
    Generates Snowflake CREATE TABLE IF NOT EXISTS DDL from any schema_map.

    Fully generic — works for any source:
      - Football results CSV
      - E-commerce orders CSV
      - IoT sensor SQLite
      - REST API JSON
      - Any future source

    Column name sanitisation:
      spaces, hyphens, dots → underscore
      forced to UPPERCASE (Snowflake convention)

    Audit columns appended to every table:
      _INGESTED_AT, _SOURCE_NAME, _PIPELINE_RUN_ID

    Args:
        table_name:         Fully-qualified or bare table name
        schema_map:         {col_name: python_type} — any types in _PY_TO_SF
        include_audit_cols: Add pipeline provenance columns (default True)

    Returns:
        CREATE TABLE IF NOT EXISTS DDL string
    """
    col_defs: list[str] = []

    for col_name, py_type in schema_map.items():
        sf_type = _PY_TO_SF.get(py_type, "VARCHAR")  # VARCHAR is safe fallback for unknown types
        safe_col = (
            col_name.upper()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )
        col_defs.append(f"    {safe_col} {sf_type}")

    if include_audit_cols:
        for col_name, col_def in _AUDIT_COLUMNS:
            col_defs.append(f"    {col_name} {col_def}")

    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        + ",\n".join(col_defs)
        + "\n);"
    )


# ---------------------------------------------------------------------------
# SNOWFLAKE HELPERS
# ---------------------------------------------------------------------------

def _get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        database=settings.snowflake_database,
        schema="BRONZE",
        warehouse=settings.snowflake_warehouse,
        role=settings.snowflake_role,
    )


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def _execute_ddl(ddl_sql: str, source_name: str, table_name: str) -> None:
    """
    Executes DDL with Snowflake query tagging for cost governance.
    Retries 3x with 10s waits — handles warehouse resume latency.
    """
    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"ALTER SESSION SET QUERY_TAG = 'agent=ddl_agent,source={source_name}'"
        )
        logger.info("ddl_executing", source=source_name, table=table_name)
        cur.execute(ddl_sql)
        logger.info("ddl_done", source=source_name, table=table_name)
    finally:
        conn.close()


def _get_existing_columns(table_name: str, source_name: str) -> set[str]:
    """Returns uppercased column names in an existing Snowflake table. Empty set if table doesn't exist."""
    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'agent=ddl_agent,source={source_name},op=describe'")
        cur.execute(f"SHOW COLUMNS IN TABLE {table_name}")
        return {row[2].upper() for row in cur.fetchall()}
    except snowflake.connector.errors.ProgrammingError:
        return set()  # Table doesn't exist yet — first run
    finally:
        conn.close()


def _apply_schema_drift(
    table_name: str,
    schema_map: dict[str, type],
    source_name: str,
) -> list[str]:
    """
    Detects new columns vs live table and issues ALTER TABLE ADD COLUMN.
    Preserves existing data. Logs every new column for governance audit.
    Returns list of ALTER statements executed.
    """
    existing_cols = _get_existing_columns(table_name, source_name)
    if not existing_cols:
        return []  # Brand new table — no drift possible

    alters: list[str] = []
    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'agent=ddl_agent,source={source_name},op=schema_drift'")
        for col_name, py_type in schema_map.items():
            safe_col = col_name.upper().replace(" ", "_").replace("-", "_").replace(".", "_")
            if safe_col not in existing_cols:
                sf_type = _PY_TO_SF.get(py_type, "VARCHAR")
                sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {safe_col} {sf_type};"
                cur.execute(sql)
                alters.append(sql)
                logger.warning(
                    "schema_drift_detected",
                    source=source_name,
                    table=table_name,
                    new_col=safe_col,
                    sf_type=sf_type,
                )
    finally:
        conn.close()
    return alters


# ---------------------------------------------------------------------------
# DDL AGENT NODE — main LangGraph entry point
# ---------------------------------------------------------------------------

def ddl_agent_node(state: AgentState) -> dict[str, Any]:
    """
    LangGraph node — generates and executes Bronze DDL for any source.

    Table name resolution (in priority order):
      1. state["target_table"] if pre-populated by supervisor (custom override)
      2. Derived dynamically from source_name via _derive_table_name()

    Same logic for secondary table (worldcup_api and any future multi-table source).
    """
    source_name = state["source_name"]
    schema_map  = state.get("schema_map")

    if not schema_map:
        return {
            "error": f"ddl_agent: schema_map empty for '{source_name}' — schema_agent must run first"
        }

    logger.info("ddl_agent_start", source=source_name)

    try:
        # ── Resolve primary table name ────────────────────────────────────
        # Use supervisor override if set, otherwise derive from source_name.
        raw_table    = state.get("target_table") or _derive_table_name(source_name)
        target_table = _build_full_table_name(raw_table)

        primary_ddl  = build_create_table_ddl(target_table, schema_map)
        _execute_ddl(primary_ddl, source_name, target_table)
        _apply_schema_drift(target_table, schema_map, source_name)

        # ── Resolve secondary table (any source with secondary_schema_map) ─
        secondary_target = None
        secondary_ddl    = None
        secondary_schema = state.get("secondary_schema_map")

        if secondary_schema:
            raw_secondary    = state.get("secondary_target_table") or _derive_secondary_table_name(source_name)
            secondary_target = _build_full_table_name(raw_secondary)
            secondary_ddl    = build_create_table_ddl(secondary_target, secondary_schema)
            _execute_ddl(secondary_ddl, source_name, secondary_target)
            _apply_schema_drift(secondary_target, secondary_schema, source_name)
            logger.info("ddl_secondary_done", source=source_name, table=secondary_target)

        logger.info(
            "ddl_agent_complete",
            source=source_name,
            primary=target_table,
            secondary=secondary_target,
        )

        return {
            "target_table":           target_table,
            "secondary_target_table": secondary_target,
            "ddl_sql":                primary_ddl,
            "table_created":          True,
            "status":                 "ddl_done",
            "error":                  None,
        }

    except Exception as e:
        logger.error("ddl_agent_error", source=source_name, error=str(e))
        return {"error": f"ddl_agent failed [{source_name}]: {str(e)}"}
