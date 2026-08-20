"""agents/ingest_agent.py - DuckDB version. Was Snowflake, migrated."""
from __future__ import annotations
import uuid, json as _json
from datetime import datetime, timezone
from typing import Any
import duckdb, httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_fixed
from agents.schema_agent import flatten_nested_json
from agents.state import AgentState
from config.settings import settings
from ingestion.schema_inferrer import infer_from_json_rows, load_csv_rows

logger = structlog.get_logger()

_MIN_ROW_SLOS: dict[str, int] = {
    "historical_results": 45_000,
    "historical_goals": 1_000,
    "historical_shootouts": 100,
    "worldcup_api": 1,
    "national_teams": 48,
    "player_profiles": 1_248,
}
_DEAD_LETTER_THRESHOLD_PCT = 5.0
_BATCH_SIZE = 1_000

def _load_csv_rows_typed(file_path, schema_map, nullable_columns=None):
    all_rows = load_csv_rows(file_path, schema_map)
    optional_cols = nullable_columns or set()
    valid, dead = [], []
    for row in all_rows:
        required_missing = any(v is None for k, v in row.items() if k not in optional_cols)
        if required_missing:
            dead.append({**row, "_rejection_reason": "null_or_coercion_failure"})
        else:
            valid.append(row)
    return valid, dead

def _fetch_sqlite_rows(file_path, table, schema_map):
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

def _fetch_api_rows(api_url, source_name):
    response = httpx.get(api_url, timeout=30, follow_redirects=True)
    response.raise_for_status()
    data = response.json()
    match_rows, goal_rows = flatten_nested_json(data)
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

def _get_connection() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(settings.duckdb_path)

def _truncate_table(table_name, source_name) -> None:
    conn = _get_connection()
    try:
        conn.execute(f"TRUNCATE TABLE {table_name}")
        logger.info("table_truncated", source=source_name, table=table_name)
    finally:
        conn.close()

@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def _write_rows_to_bronze(rows, table_name, schema_map, source_name, pipeline_run_id) -> int:
    if not rows:
        return 0
    cols = [c.upper().replace(" ", "_").replace("-", "_") for c in schema_map.keys()]
    audit_cols = ["_INGESTED_AT", "_SOURCE_NAME", "_PIPELINE_RUN_ID"]
    all_cols = cols + audit_cols
    placeholders = ", ".join(["?"] * len(all_cols))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(all_cols)}) VALUES ({placeholders})"
    ingested_at = datetime.now(timezone.utc).isoformat()
    conn = _get_connection()
    try:
        total_written = 0
        for batch_start in range(0, len(rows), _BATCH_SIZE):
            batch = rows[batch_start: batch_start + _BATCH_SIZE]
            values = []
            for row in batch:
                row_vals = [row.get(col.lower(), row.get(col)) for col in schema_map.keys()]
                row_vals += [ingested_at, source_name, pipeline_run_id]
                values.append(tuple(row_vals))
            conn.executemany(insert_sql, values)
            total_written += len(batch)
            logger.info("batch_written", source=source_name, op="insert", run_id=pipeline_run_id, batch_start=batch_start, batch_size=len(batch), total_so_far=total_written)
        return total_written
    finally:
        conn.close()

def _write_dead_letters(dead_rows, source_name, table_name, pipeline_run_id) -> None:
    if not dead_rows:
        return
    conn = _get_connection()
    try:
        sql = "INSERT INTO BRONZE.DEAD_LETTER (_dl_id, _ingested_at, _batch_id, _source, _error_type, _error_detail, _raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?)"
        ingested_at = datetime.now(timezone.utc).isoformat()
        values = [
            (
                str(uuid.uuid4())[:16], ingested_at, pipeline_run_id, source_name,
                row.get("_rejection_reason", "unknown"),
                str(row.get("_rejection_detail", ""))[:2000],
                _json.dumps({k: v for k, v in row.items() if not k.startswith("_")}),
            )
            for row in dead_rows
        ]
        conn.executemany(sql, values)
        logger.warning("dead_letters_written", source=source_name, op="dead_letter", run_id=pipeline_run_id, count=len(dead_rows))
    finally:
        conn.close()

def _update_watermark(source_name, rows_ingested, pipeline_run_id) -> None:
    conn = _get_connection()
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT INTO BRONZE.INGESTION_WATERMARKS (source_name, last_loaded_at, rows_loaded, last_batch_id, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (source_name) DO UPDATE SET
                last_loaded_at = excluded.last_loaded_at,
                rows_loaded = excluded.rows_loaded,
                last_batch_id = excluded.last_batch_id,
                updated_at = excluded.updated_at
        """, (source_name, now, rows_ingested, pipeline_run_id, now))
        logger.info("watermark_updated", source=source_name, op="watermark", rows=rows_ingested)
    finally:
        conn.close()

def _check_slos(source_name, rows_ingested, dead_letter_count, total_rows):
    min_rows = _MIN_ROW_SLOS.get(source_name, 0)
    if min_rows > 0 and rows_ingested < min_rows:
        return f"SLO BREACH: {source_name} ingested {rows_ingested} rows, minimum expected {min_rows}. Check source file integrity or API response."
    if total_rows > 0:
        dead_pct = (dead_letter_count / total_rows) * 100
        if dead_pct > _DEAD_LETTER_THRESHOLD_PCT:
            return f"SLO BREACH: {source_name} dead letter rate {dead_pct:.1f}% exceeds {_DEAD_LETTER_THRESHOLD_PCT}% threshold. ({dead_letter_count}/{total_rows} rows rejected). Inspect BRONZE.DEAD_LETTER for rejection reasons."
    return None

def ingest_agent_node(state: AgentState) -> dict[str, Any]:
    source_name = state["source_name"]
    source_type = state["source_type"]
    schema_map = state.get("schema_map")
    target_table = state.get("target_table")

    if not schema_map:
        return {"error": f"ingest_agent: schema_map empty for '{source_name}'"}
    if not target_table:
        return {"error": f"ingest_agent: target_table not set for '{source_name}' — ddl_agent must run first"}

    pipeline_run_id = str(uuid.uuid4())
    logger.info("ingest_agent_start", source=source_name, run_id=pipeline_run_id)

    try:
        if source_type == "csv":
            valid_rows, dead_rows = _load_csv_rows_typed(state["file_path"], schema_map, state.get("nullable_columns"))
            total = len(valid_rows) + len(dead_rows)
            _truncate_table(target_table, source_name)
            written = _write_rows_to_bronze(valid_rows, target_table, schema_map, source_name, pipeline_run_id)
            _write_dead_letters(dead_rows, source_name, target_table, pipeline_run_id)
            _update_watermark(source_name, written, pipeline_run_id)
            slo_error = _check_slos(source_name, written, len(dead_rows), total)
            if slo_error:
                return {"error": slo_error, "rows_ingested": written, "dead_letter_count": len(dead_rows), "watermark_updated": True}
            return {"rows_ingested": written, "dead_letter_count": len(dead_rows), "watermark_updated": True, "status": "ingested", "error": None}

        elif source_type == "sqlite":
            valid_rows, dead_rows = _fetch_sqlite_rows(state["file_path"], state["sqlite_table"], schema_map)
            total = len(valid_rows) + len(dead_rows)
            _truncate_table(target_table, source_name)
            written = _write_rows_to_bronze(valid_rows, target_table, schema_map, source_name, pipeline_run_id)
            _write_dead_letters(dead_rows, source_name, target_table, pipeline_run_id)
            _update_watermark(source_name, written, pipeline_run_id)
            slo_error = _check_slos(source_name, written, len(dead_rows), total)
            if slo_error:
                return {"error": slo_error, "rows_ingested": written, "dead_letter_count": len(dead_rows), "watermark_updated": True}
            return {"rows_ingested": written, "dead_letter_count": len(dead_rows), "watermark_updated": True, "status": "ingested", "error": None}

        elif source_type == "api_json":
            secondary_table = state.get("secondary_target_table")
            secondary_schema = state.get("secondary_schema_map")
            match_valid, match_dead, goal_valid, goal_dead = _fetch_api_rows(state["api_url"], source_name)
            _truncate_table(target_table, source_name)
            match_written = _write_rows_to_bronze(match_valid, target_table, infer_from_json_rows(match_valid) if match_valid else schema_map, source_name, pipeline_run_id)
            _write_dead_letters(match_dead, source_name, target_table, pipeline_run_id)
            goal_written = 0
            if secondary_table and secondary_schema and goal_valid:
                _truncate_table(secondary_table, source_name)
                goal_written = _write_rows_to_bronze(goal_valid, secondary_table, infer_from_json_rows(goal_valid) if goal_valid else secondary_schema, f"{source_name}_goals", pipeline_run_id)
                _write_dead_letters(goal_dead, f"{source_name}_goals", secondary_table, pipeline_run_id)
            total_written = match_written + goal_written
            total_dead = len(match_dead) + len(goal_dead)
            total_rows = len(match_valid) + len(match_dead) + len(goal_valid) + len(goal_dead)
            _update_watermark(source_name, total_written, pipeline_run_id)
            slo_error = _check_slos(source_name, match_written, total_dead, total_rows)
            if slo_error:
                return {"error": slo_error, "rows_ingested": total_written, "dead_letter_count": total_dead, "watermark_updated": True}
            return {"rows_ingested": total_written, "dead_letter_count": total_dead, "watermark_updated": True, "status": "ingested", "error": None}
        else:
            raise ValueError(f"Unknown source_type: '{source_type}'")
    except Exception as e:
        logger.error("ingest_agent_error", source=source_name, error=str(e))
        return {"error": f"ingest_agent failed [{source_name}]: {str(e)}"}
