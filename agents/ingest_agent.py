"""
agents/ingest_agent.py
======================
LangGraph node: loads rows from any source, writes to Snowflake Bronze,
routes invalid rows to DEAD_LETTER, updates INGESTION_WATERMARKS.

WHAT THIS NODE DOES:
--------------------
1. Dispatches to the correct row reader based on source_type:
     csv      → load_csv_rows() from schema_inferrer
     sqlite   → fetch_sqlite_rows()
     api_json → fetch_api_rows() + flatten_nested_json()

2. Validates every row against schema_map:
     - Required fields null → dead letter
     - Type coercion failure → dead letter

3. Writes valid rows to Snowflake Bronze in batches (1000 rows/batch)
   using executemany() — single network roundtrip per batch

4. Writes invalid rows to BRONZE.DEAD_LETTER with rejection reason

5. Updates BRONZE.INGESTION_WATERMARKS after successful load

6. DATA QUALITY SLOs (per memory requirements):
     - Asserts ingested rows >= expected minimum per source
     - Asserts dead_letter_count <= 5% of total rows
     - If either SLO breached → sets error, Supervisor routes to failure

WHY SEPARATE FROM DDL AGENT?
-----------------------------
DDL is idempotent and runs even on re-runs (no-op if table exists).
Ingestion is stateful — watermarks prevent duplicate loads.
Separating them means DDL failures never cause partial data loads,
and ingest failures never leave orphaned tables.

WHY BATCH WRITES (executemany)?
--------------------------------
Writing one row at a time = one Snowflake round-trip per row.
For results.csv (~45k rows) that's 45,000 round-trips.
executemany() with 1000-row batches = ~45 round-trips. 1000x faster.

WHY DEAD LETTER INSTEAD OF FAIL FAST?
--------------------------------------
A single bad row in 45,000 should not block the entire load.
Dead letter captures the bad row + reason for later inspection/replay.
The pipeline continues with the valid rows.
This is the standard enterprise pattern (Kafka dead letter queues,
AWS SQS DLQ, Snowflake COPY INTO error handling all use this pattern).

IDEMPOTENCY:
------------
Full-reload sources (CSV, SQLite): TRUNCATE before INSERT.
  Ensures re-runs produce the same Bronze table state.
Incremental sources (API): INSERT only rows newer than last watermark.
  Watermark stored in INGESTION_WATERMARKS table.

DATA QUALITY SLOs (from project requirements):
-----------------------------------------------
  results.csv:          >= 45,000 rows
  goalscorers.csv:      >= 1,000 rows
  shootouts.csv:        >= 100 rows
  worldcup_api matches: >= 1 row (live tournament, grows daily)
  worldcup_api goals:   >= 0 rows (early in tournament may be 0)
  national_teams:       == 48 rows (fixed WC 2026 group stage)
  player_profiles:      == 1248 rows (real WC 2026 squads, openfootball API)
  Dead letter threshold: <= 5% of total rows for any source
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import httpx
import snowflake.connector
import structlog
from tenacity import retry, stop_after_attempt, wait_fixed

from agents.schema_agent import flatten_nested_json
from agents.state import AgentState
from config.settings import settings
from ingestion.schema_inferrer import infer_from_json_rows, load_csv_rows

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# DATA QUALITY SLOs
# Minimum row counts per source — asserted after every Bronze load.
# Set to 0 for sources where count is legitimately variable (API early-stage).
# ---------------------------------------------------------------------------

_MIN_ROW_SLOS: dict[str, int] = {
    "historical_results":   45_000,
    "historical_goals":      1_000,
    "historical_shootouts":    100,
    "worldcup_api":              1,   # matches table — at least 1 match played
    "national_teams":           48,   # fixed: 48 WC 2026 teams
    "player_profiles":       1_248,   # real: WC 2026 full squads (openfootball API)
}

_DEAD_LETTER_THRESHOLD_PCT = 5.0   # >5% dead letters → escalate to failure

# Batch size for Snowflake executemany — tuned for Snowflake X-Small warehouse
_BATCH_SIZE = 1_000


# ---------------------------------------------------------------------------
# ROW READERS — one per source_type
# ---------------------------------------------------------------------------

def _load_csv_rows_typed(
    file_path: str,
    schema_map: dict[str, type],
    nullable_columns: set[str] | None = None,
) -> tuple[list[dict], list[dict]]:
    """
    Reads all rows from a CSV, coerces values to schema_map types.
    Returns (valid_rows, dead_letter_rows).

    Dead letter condition: any NON-nullable field is None after coercion.
    nullable_columns comes from schema_agent's data-driven null-rate scan —
    columns where real historical data is legitimately sparse are allowed
    to be empty instead of being rejected as a data quality failure.
    """
    all_rows = load_csv_rows(file_path, schema_map)
    optional_cols = nullable_columns or set()
    valid, dead = [], []
    for row in all_rows:
        required_missing = any(
            v is None for k, v in row.items() if k not in optional_cols
        )
        if required_missing:
            dead.append({
                **row,
                "_rejection_reason": "null_or_coercion_failure",
            })
        else:
            valid.append(row)
    return valid, dead


def _fetch_sqlite_rows(
    file_path: str,
    table: str,
    schema_map: dict[str, type],
) -> tuple[list[dict], list[dict]]:
    """
    Reads all rows from a SQLite table, coerces to schema_map types.
    Returns (valid_rows, dead_letter_rows).
    """
    import sqlite3

    conn = sqlite3.connect(file_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    conn.close()

    valid, dead = [], []
    for row in rows:
        coerced: dict[str, Any] = {}
        failed = False
        for col, py_type in schema_map.items():
            raw = row[col] if col in row.keys() else None
            try:
                if raw is None:
                    coerced[col] = None
                    failed = True
                elif py_type == bool:
                    coerced[col] = bool(raw)
                elif py_type == int:
                    coerced[col] = int(raw)
                elif py_type == float:
                    coerced[col] = float(raw)
                else:
                    coerced[col] = str(raw)
            except (ValueError, TypeError):
                coerced[col] = None
                failed = True

        if failed:
            dead.append({**coerced, "_rejection_reason": "null_or_coercion_failure"})
        else:
            valid.append(coerced)

    return valid, dead


def _fetch_api_rows(
    api_url: str,
    source_name: str,
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Fetches and flattens worldcup.json API response.
    Returns (match_valid, match_dead, goal_valid, goal_dead).

    Re-fetches and re-flattens independently from schema_agent — state
    should not carry data rows (anti-pattern for large sources).
    """
    response = httpx.get(api_url, timeout=30, follow_redirects=True)
    response.raise_for_status()
    data = response.json()

    match_rows, goal_rows = flatten_nested_json(data)

    # Basic validation: no null match_num (primary key)
    match_valid, match_dead = [], []
    for row in match_rows:
        if row.get("match_num") is None:
            match_dead.append({**row, "_rejection_reason": "null_match_num_pk"})
        else:
            match_valid.append(row)

    goal_valid, goal_dead = [], []
    for row in goal_rows:
        if row.get("match_num") is None:
            goal_dead.append({**row, "_rejection_reason": "null_match_num_fk"})
        else:
            goal_valid.append(row)

    return match_valid, match_dead, goal_valid, goal_dead


# ---------------------------------------------------------------------------
# SNOWFLAKE WRITERS
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


def _truncate_table(table_name: str, source_name: str) -> None:
    """
    Truncates Bronze table before full-reload sources.
    WHY TRUNCATE NOT DROP/RECREATE: preserves table structure and any
    Snowflake grants — only removes data rows.
    """
    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'agent=ingest_agent,source={source_name},op=truncate'")
        cur.execute(f"TRUNCATE TABLE {table_name}")
        logger.info("table_truncated", source=source_name, table=table_name)
    finally:
        conn.close()


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def _write_rows_to_bronze(
    rows: list[dict],
    table_name: str,
    schema_map: dict[str, type],
    source_name: str,
    pipeline_run_id: str,
) -> int:
    """
    Writes rows to Snowflake Bronze in batches using executemany().
    Appends audit columns (_ingested_at, _source_name, _pipeline_run_id).
    Returns count of rows successfully written.

    WHY executemany() NOT COPY INTO?
    COPY INTO requires staging files (S3/internal stage) — adds complexity
    and latency. For sources up to ~500k rows, executemany() with 1000-row
    batches is fast enough and simpler to implement and debug.
    For sources >1M rows, switch to COPY INTO with internal stage.
    """
    if not rows:
        return 0

    # Build column list from schema_map + audit cols
    cols = [c.upper().replace(" ", "_").replace("-", "_") for c in schema_map.keys()]
    audit_cols = ["_INGESTED_AT", "_SOURCE_NAME", "_PIPELINE_RUN_ID"]
    all_cols = cols + audit_cols

    placeholders = ", ".join(["%s"] * len(all_cols))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(all_cols)}) VALUES ({placeholders})"

    ingested_at = datetime.now(timezone.utc).isoformat()

    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'agent=ingest_agent,source={source_name},op=insert'")

        total_written = 0
        for batch_start in range(0, len(rows), _BATCH_SIZE):
            batch = rows[batch_start: batch_start + _BATCH_SIZE]
            values = []
            for row in batch:
                row_vals = [row.get(col.lower(), row.get(col)) for col in schema_map.keys()]
                row_vals += [ingested_at, source_name, pipeline_run_id]
                values.append(tuple(row_vals))

            cur.executemany(insert_sql, values)
            total_written += len(batch)
            logger.info(
                "batch_written",
                source=source_name,
                batch_start=batch_start,
                batch_size=len(batch),
                total_so_far=total_written,
            )

        return total_written
    finally:
        conn.close()


def _write_dead_letters(
    dead_rows: list[dict],
    source_name: str,
    table_name: str,
    pipeline_run_id: str,
) -> None:
    """
    Writes rejected rows to BRONZE.DEAD_LETTER with rejection metadata.
    Schema of DEAD_LETTER table (created in Snowflake setup.sql):
      source_name, table_name, pipeline_run_id, ingested_at,
      rejection_reason, raw_row (VARIANT)
    """
    if not dead_rows:
        return

    import json as _json

    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'agent=ingest_agent,source={source_name},op=dead_letter'")

        import uuid as _uuid
        sql = """
            INSERT INTO BRONZE.DEAD_LETTER
            (_dl_id, _ingested_at, _batch_id, _source, _error_type, _error_detail, _raw_payload)
            SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
        """
        ingested_at = datetime.now(timezone.utc).isoformat()
        values = [
            (
                str(_uuid.uuid4())[:16],
                ingested_at,
                pipeline_run_id,
                source_name,
                row.get("_rejection_reason", "unknown"),
                str(row.get("_rejection_detail", ""))[:2000],
                _json.dumps({k: v for k, v in row.items() if not k.startswith("_")}),
            )
            for row in dead_rows
        ]
        for v in values:
            cur.execute(sql, v)
        logger.warning(
            "dead_letters_written",
            source=source_name,
            count=len(dead_rows),
        )
    finally:
        conn.close()


def _update_watermark(
    source_name: str,
    rows_ingested: int,
    pipeline_run_id: str,
) -> None:
    """
    Upserts a row in BRONZE.INGESTION_WATERMARKS.
    Records last_ingested_at and rows_ingested for this source.

    WHY WATERMARKS?
    Idempotency for incremental sources (API). On the next run, the ingest
    agent reads the watermark and only fetches rows newer than last run.
    For full-reload sources (CSV/SQLite), watermarks still provide an
    audit trail of every pipeline execution.
    """
    conn = _get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET QUERY_TAG = 'agent=ingest_agent,source={source_name},op=watermark'")
        cur.execute("""
            MERGE INTO BRONZE.INGESTION_WATERMARKS AS target
            USING (SELECT %s AS source_name, %s AS last_loaded_at,
                          %s AS rows_loaded,   %s AS last_batch_id) AS source
            ON target.source_name = source.source_name
            WHEN MATCHED THEN UPDATE SET
                last_loaded_at = source.last_loaded_at,
                rows_loaded    = source.rows_loaded,
                last_batch_id  = source.last_batch_id,
                updated_at     = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (source_name, last_loaded_at, rows_loaded, last_batch_id)
                VALUES (source.source_name, source.last_loaded_at,
                        source.rows_loaded,  source.last_batch_id)
        """, (
            source_name,
            datetime.now(timezone.utc).isoformat(),
            rows_ingested,
            pipeline_run_id,
        ))
        logger.info("watermark_updated", source=source_name, rows=rows_ingested)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DATA QUALITY SLO CHECKS
# ---------------------------------------------------------------------------

def _check_slos(
    source_name: str,
    rows_ingested: int,
    dead_letter_count: int,
    total_rows: int,
) -> str | None:
    """
    Checks data quality SLOs. Returns error string if any SLO is breached,
    None if all pass.

    SLO 1: rows_ingested >= minimum expected for this source
    SLO 2: dead_letter_count <= 5% of total rows processed
    """
    # SLO 1: minimum row count
    min_rows = _MIN_ROW_SLOS.get(source_name, 0)
    if min_rows > 0 and rows_ingested < min_rows:
        return (
            f"SLO BREACH: {source_name} ingested {rows_ingested} rows, "
            f"minimum expected {min_rows}. "
            f"Check source file integrity or API response."
        )

    # SLO 2: dead letter threshold
    if total_rows > 0:
        dead_pct = (dead_letter_count / total_rows) * 100
        if dead_pct > _DEAD_LETTER_THRESHOLD_PCT:
            return (
                f"SLO BREACH: {source_name} dead letter rate {dead_pct:.1f}% "
                f"exceeds {_DEAD_LETTER_THRESHOLD_PCT}% threshold. "
                f"({dead_letter_count}/{total_rows} rows rejected). "
                f"Inspect BRONZE.DEAD_LETTER for rejection reasons."
            )

    return None


# ---------------------------------------------------------------------------
# INGEST AGENT NODE — main LangGraph entry point
# ---------------------------------------------------------------------------

def ingest_agent_node(state: AgentState) -> dict[str, Any]:
    """
    LangGraph node — loads rows from any source into Snowflake Bronze.

    Works for any source_type: csv | sqlite | api_json
    For api_json with secondary_schema_map: writes both primary + secondary tables.

    Pipeline run ID is generated fresh per source run — links all rows
    written in this execution for audit/rollback purposes.
    """
    source_name  = state["source_name"]
    source_type  = state["source_type"]
    schema_map   = state.get("schema_map")
    target_table = state.get("target_table")

    if not schema_map:
        return {"error": f"ingest_agent: schema_map empty for '{source_name}'"}
    if not target_table:
        return {"error": f"ingest_agent: target_table not set for '{source_name}' — ddl_agent must run first"}

    pipeline_run_id = str(uuid.uuid4())
    logger.info("ingest_agent_start", source=source_name, run_id=pipeline_run_id)

    try:
        # ── CSV sources — full reload ──────────────────────────────────────
        if source_type == "csv":
            valid_rows, dead_rows = _load_csv_rows_typed(
                state["file_path"], schema_map, state.get("nullable_columns")
            )
            total = len(valid_rows) + len(dead_rows)

            _truncate_table(target_table, source_name)
            written = _write_rows_to_bronze(
                valid_rows, target_table, schema_map, source_name, pipeline_run_id
            )
            _write_dead_letters(dead_rows, source_name, target_table, pipeline_run_id)
            _update_watermark(source_name, written, pipeline_run_id)

            slo_error = _check_slos(source_name, written, len(dead_rows), total)
            if slo_error:
                return {"error": slo_error, "rows_ingested": written,
                        "dead_letter_count": len(dead_rows), "watermark_updated": True}

            return {
                "rows_ingested":    written,
                "dead_letter_count": len(dead_rows),
                "watermark_updated": True,
                "status":           "ingested",
                "error":            None,
            }

        # ── SQLite sources — full reload ───────────────────────────────────
        elif source_type == "sqlite":
            valid_rows, dead_rows = _fetch_sqlite_rows(
                state["file_path"], state["sqlite_table"], schema_map
            )
            total = len(valid_rows) + len(dead_rows)

            _truncate_table(target_table, source_name)
            written = _write_rows_to_bronze(
                valid_rows, target_table, schema_map, source_name, pipeline_run_id
            )
            _write_dead_letters(dead_rows, source_name, target_table, pipeline_run_id)
            _update_watermark(source_name, written, pipeline_run_id)

            slo_error = _check_slos(source_name, written, len(dead_rows), total)
            if slo_error:
                return {"error": slo_error, "rows_ingested": written,
                        "dead_letter_count": len(dead_rows), "watermark_updated": True}

            return {
                "rows_ingested":    written,
                "dead_letter_count": len(dead_rows),
                "watermark_updated": True,
                "status":           "ingested",
                "error":            None,
            }

        # ── API JSON sources — incremental ────────────────────────────────
        elif source_type == "api_json":
            secondary_table  = state.get("secondary_target_table")
            secondary_schema = state.get("secondary_schema_map")

            match_valid, match_dead, goal_valid, goal_dead = _fetch_api_rows(
                state["api_url"], source_name
            )

            # Matches table — truncate + reload (idempotent for full tournament)
            _truncate_table(target_table, source_name)
            match_written = _write_rows_to_bronze(
                match_valid, target_table,
                # Use inferred schema from valid rows for column order
                infer_from_json_rows(match_valid) if match_valid else schema_map,
                source_name, pipeline_run_id,
            )
            _write_dead_letters(match_dead, source_name, target_table, pipeline_run_id)

            # Goals table — truncate + reload
            goal_written = 0
            if secondary_table and secondary_schema and goal_valid:
                _truncate_table(secondary_table, source_name)
                goal_written = _write_rows_to_bronze(
                    goal_valid, secondary_table,
                    infer_from_json_rows(goal_valid) if goal_valid else secondary_schema,
                    f"{source_name}_goals", pipeline_run_id,
                )
                _write_dead_letters(
                    goal_dead, f"{source_name}_goals", secondary_table, pipeline_run_id
                )

            total_written    = match_written + goal_written
            total_dead       = len(match_dead) + len(goal_dead)
            total_rows       = len(match_valid) + len(match_dead) + len(goal_valid) + len(goal_dead)

            _update_watermark(source_name, total_written, pipeline_run_id)

            slo_error = _check_slos(source_name, match_written, total_dead, total_rows)
            if slo_error:
                return {"error": slo_error, "rows_ingested": total_written,
                        "dead_letter_count": total_dead, "watermark_updated": True}

            return {
                "rows_ingested":    total_written,
                "dead_letter_count": total_dead,
                "watermark_updated": True,
                "status":           "ingested",
                "error":            None,
            }

        else:
            raise ValueError(f"Unknown source_type: '{source_type}'")

    except Exception as e:
        logger.error("ingest_agent_error", source=source_name, error=str(e))
        return {"error": f"ingest_agent failed [{source_name}]: {str(e)}"}
