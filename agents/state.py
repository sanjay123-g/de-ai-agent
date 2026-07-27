"""
agents/state.py
===============
Defines the shared state object that flows through every node in the
LangGraph pipeline, and the ResetNode that sanitises it between source runs.

WHY A SEPARATE FILE?
--------------------
LangGraph passes one mutable state dict between every node. If two source
runs (e.g. results.csv → then goalscorers.csv) share the same state object
without a deliberate reset, keys like `schema_map` and `ddl_sql` from the
first run silently pollute the second.  Isolating the state definition here
makes that contract explicit and testable.
"""

from __future__ import annotations

from typing import Any, Optional
from typing_extensions import TypedDict


# ---------------------------------------------------------------------------
# AgentState — the single shared object passed between every LangGraph node
# ---------------------------------------------------------------------------

class AgentState(TypedDict):
    """
    One instance of this dict is created per pipeline run (i.e. per data
    source).  The Supervisor resets source-specific keys via ResetNode before
    handing off to the next source, so the same graph object can be reused
    across all 6 sources without state bleed.

    Field groupings mirror the pipeline stages so each agent owns a clear
    slice of state and never writes to another agent's slice.
    """

    # ------------------------------------------------------------------
    # [SOURCE IDENTIFICATION]
    # Set by the Supervisor before the run begins; never mutated by agents.
    # ------------------------------------------------------------------

    source_name: str
    """
    Human-readable identifier for the source, e.g. "results_csv" or
    "worldcup_api".  Used in logs, dead-letter records, and Snowflake
    table naming conventions.  Having a stable name here means every
    downstream node can refer to it without re-deriving it.
    """

    source_type: str
    """
    One of: "csv" | "sqlite" | "api_json".
    The Schema Agent uses this to dispatch to the correct inferrer
    (infer_from_csv / infer_from_sqlite / infer_from_json_rows).
    Keeping it explicit avoids brittle file-extension sniffing.
    """

    file_path: Optional[str]
    """
    Absolute path to the local file (CSV or SQLite).  None for API sources.
    The Schema Agent reads this; the Ingest Agent reads it again independently
    so neither has to pass data through state (avoids bloating state with
    entire DataFrames or row lists).
    """

    api_url: Optional[str]
    """
    URL for API sources.  None for file sources.
    Kept separate from file_path so the Ingest Agent can branch on exactly
    one of {file_path, api_url} being set — no ambiguous logic.
    """

    sqlite_table: Optional[str]
    """
    For SQLite sources (fifa.db), which table to ingest.
    None for CSV and API sources.  Needed because the same fifa.db has two
    tables (national_teams, player_profiles) that produce two separate
    pipeline runs.
    """

    # ------------------------------------------------------------------
    # [SCHEMA AGENT OUTPUT]
    # Produced by schema_agent.py, consumed by ddl_agent.py and ingest_agent.py.
    # ------------------------------------------------------------------

    schema_map: Optional[dict[str, type]]
    nullable_columns: Optional[set[str]]
    """
    {column_name: python_type} mapping produced by schema_inferrer.py.
    e.g. {"date": str, "home_score": int, "neutral": bool}

    This is the central contract between schema inference and everything
    downstream.  DDL Agent converts it to Snowflake types; Ingest Agent uses
    it to coerce row values; QA Agent validates it against actual table
    metadata.

    Typed as dict[str, type] (not dict[str, str]) because the inferrer
    returns actual Python type objects — this keeps coercion logic clean.
    """

    flattened_sources: Optional[list[dict[str, Any]]]
    """
    Only populated for nested API sources (worldcup.json).
    After flatten_nested_json() runs, this holds the list of flat row dicts
    that infer_from_json_rows() can process.

    Kept separate from file_path / api_url to make the flattening step
    explicit in the graph — a Schema Agent node that fills this key signals
    clearly that flattening happened.
    """

    secondary_schema_map: Optional[dict[str, type]]
    """
    For worldcup.json only: the API produces TWO tables (MATCHES + GOALS).
    The primary schema_map holds RAW_WC2026_MATCHES columns.
    This holds RAW_WC2026_GOALS columns.

    Having it as a named key (not a list) keeps the DDL Agent simple:
    it checks `if state["secondary_schema_map"]` and issues a second
    CREATE TABLE if truthy.
    """

    # ------------------------------------------------------------------
    # [DDL AGENT OUTPUT]
    # Produced by ddl_agent.py; consumed by ingest_agent.py for table validation.
    # ------------------------------------------------------------------

    target_table: Optional[str]
    """
    Fully-qualified Snowflake table name the DDL Agent will create/verify,
    e.g. "DE_AI_AGENT_DEV.BRONZE.RAW_HISTORICAL_RESULTS".
    Built deterministically from source_name so naming is consistent across
    runs and human-readable in the Snowflake UI.
    """

    secondary_target_table: Optional[str]
    """
    For worldcup.json: second Snowflake table name
    e.g. "DE_AI_AGENT_DEV.BRONZE.RAW_WC2026_GOALS".
    """

    ddl_sql: Optional[str]
    """
    The CREATE TABLE IF NOT EXISTS statement issued to Snowflake.
    Stored in state so the QA Agent can log it and the Transform Agent
    can reference it when generating dbt source YAML.
    """

    table_created: Optional[bool]
    """
    True if the DDL executed without error.
    The Supervisor routes to the Ingest Agent only if this is True.
    False triggers a retry or dead-letter escalation — never a silent skip.
    """

    # ------------------------------------------------------------------
    # [INGEST AGENT OUTPUT]
    # Produced by ingest_agent.py; consumed by QA Agent and Supervisor.
    # ------------------------------------------------------------------

    rows_ingested: int
    """
    Count of rows successfully written to the Bronze Snowflake table.
    Default 0 (not None) so arithmetic in the Supervisor and dashboard
    never needs a None-guard.
    """

    dead_letter_count: int
    """
    Count of rows that failed validation (null required fields, type
    coercion failures) and were written to BRONZE.DEAD_LETTER.
    Default 0.  A non-zero value triggers a warning log but does NOT
    block the pipeline — the valid rows still proceed.
    """

    watermark_updated: Optional[bool]
    """
    True if INGESTION_WATERMARKS was updated after a successful ingest.
    Used by the Supervisor to confirm idempotency bookkeeping ran.
    """

    # ------------------------------------------------------------------
    # [TRANSFORM AGENT OUTPUT]
    # Produced by transform_agent.py; consumed by QA Agent.
    # ------------------------------------------------------------------

    dbt_model_path: Optional[str]
    """
    Relative path to the generated dbt staging model file,
    e.g. "dbt_project/models/staging/stg_historical_results.sql".
    Stored so the QA Agent knows which model to compile and test.
    """

    pr_url: Optional[str]
    """
    GitHub PR URL opened by the Transform Agent.
    The QA Agent blocks merge until tests_passed is True.
    Stored in state so the Streamlit dashboard can render a direct link.
    """

    # ------------------------------------------------------------------
    # [QA AGENT OUTPUT]
    # Produced by qa_agent.py; consumed by Supervisor for final routing.
    # ------------------------------------------------------------------

    tests_passed: Optional[bool]
    """
    True only if ALL dbt tests for this source's staging model pass.
    False blocks the PR from merging and triggers a failure alert.
    None means the QA Agent has not run yet (pre-transform state).
    """

    test_failure_summary: Optional[str]
    """
    Human-readable summary of failed tests, e.g.:
    "not_null.stg_historical_results.home_score: 3 failures"
    Surfaced in the Streamlit QA tab and the Slack alert payload.
    """

    # ------------------------------------------------------------------
    # [PIPELINE CONTROL]
    # Written by any agent when something goes wrong; read by Supervisor.
    # ------------------------------------------------------------------

    error: Optional[str]
    """
    Set by any agent that catches an unrecoverable exception.
    The Supervisor checks this at every edge — if set, it routes to a
    FailureNode that logs, alerts, and halts the current source run
    without crashing the overall graph process.
    """

    status: str
    """
    Pipeline lifecycle status for this source run.
    Values: "pending" | "schema_done" | "ddl_done" | "ingested" |
            "transformed" | "qa_passed" | "failed"
    The Streamlit dashboard polls this to render per-source badges.
    Default "pending".
    """

    retry_count: int
    """
    How many times the current failing node has been retried.
    The Supervisor increments this; nodes read it to adjust backoff.
    Capped at MAX_RETRIES (3) before escalating to error state.
    Default 0.
    """


# ---------------------------------------------------------------------------
# ResetNode — runs at the START of every new source execution cycle
# ---------------------------------------------------------------------------

def make_reset_node(
    source_name: str,
    source_type: str,
    file_path: Optional[str] = None,
    api_url: Optional[str] = None,
    sqlite_table: Optional[str] = None,
) -> dict[str, Any]:
    """
    Returns a fresh AgentState dict with all source-specific fields zeroed out.

    WHY THIS EXISTS (critical design decision):
    -------------------------------------------
    LangGraph nodes mutate state IN PLACE via reducer functions.
    If you run source A (results.csv) and then source B (goalscorers.csv)
    through the same compiled graph without resetting, source B's Schema Agent
    will see source A's schema_map still populated from the previous run.
    The DDL Agent will then either skip DDL generation (table_created=True
    from run A) or, worse, generate the wrong DDL.

    The fix is NOT to recreate the entire StateGraph per source (expensive,
    loses compiled graph optimisations).  The fix is a ResetNode that
    explicitly sets every source-specific field back to its zero/None value
    before the Supervisor hands off to the next source.

    Usage in supervisor.py:
        state.update(make_reset_node(
            source_name="goalscorers_csv",
            source_type="csv",
            file_path="/abs/path/goalscorers.csv"
        ))

    This is also why rows_ingested and dead_letter_count default to int(0)
    rather than None — arithmetic operations downstream never need a None guard.
    """
    return AgentState(
        # Source identification — set fresh per run
        source_name=source_name,
        source_type=source_type,
        file_path=file_path,
        api_url=api_url,
        sqlite_table=sqlite_table,

        # Schema Agent outputs — cleared
        schema_map=None,
        nullable_columns=None,
        flattened_sources=None,
        secondary_schema_map=None,

        # DDL Agent outputs — cleared
        target_table=None,
        secondary_target_table=None,
        ddl_sql=None,
        table_created=None,

        # Ingest Agent outputs — zeroed (not None, to allow safe arithmetic)
        rows_ingested=0,
        dead_letter_count=0,
        watermark_updated=None,

        # Transform Agent outputs — cleared
        dbt_model_path=None,
        pr_url=None,

        # QA Agent outputs — cleared
        tests_passed=None,
        test_failure_summary=None,

        # Pipeline control — reset to initial values
        error=None,
        status="pending",
        retry_count=0,
    )


# ---------------------------------------------------------------------------
# Sentinel values (used by Supervisor routing edges)
# ---------------------------------------------------------------------------

# LangGraph routing: return these strings from node functions to direct the
# graph to the next node.  Centralising them here prevents typo bugs in
# routing conditions spread across multiple agent files.

ROUTE_TO_SCHEMA    = "schema_agent"
ROUTE_TO_DDL       = "ddl_agent"
ROUTE_TO_INGEST    = "ingest_agent"
ROUTE_TO_TRANSFORM = "transform_agent"
ROUTE_TO_QA        = "qa_agent"
ROUTE_TO_DONE      = "done"
ROUTE_TO_FAILURE   = "failure"

MAX_RETRIES = 3
