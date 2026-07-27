"""
agents/supervisor.py
====================
Builds and compiles the LangGraph StateGraph, defines the source registry,
implements all routing logic between nodes, and exposes run_pipeline() which
drives the full ingestion cycle across all 6 data sources.

WHY A SUPERVISOR FILE?
----------------------
The Supervisor is not an LLM agent — it is pure deterministic routing logic.
It reads `state["status"]` and `state["error"]` after each node returns, then
decides which node to execute next.  Keeping this separate from the individual
agent nodes means:
  - Routing rules are in ONE place; changing retry behaviour doesn't touch
    schema_agent.py or ingest_agent.py
  - The graph topology is visible and auditable without reading agent logic
  - Testing routing is independent of testing agent logic (stub nodes below)

GRAPH TOPOLOGY (per source run):
  START
    │
    ▼
  schema_agent  ──(error)──► failure_node ──► END
    │
    ▼
  ddl_agent     ──(error)──► failure_node ──► END
    │
    ▼
  ingest_agent  ──(error)──► failure_node ──► END
    │
    ▼
  transform_agent ──(error)──► failure_node ──► END
    │
    ▼
  qa_agent      ──(fail/error)──► failure_node ──► END
    │
  (pass)
    ▼
  done_node ──► END

Retry logic is handled INSIDE each agent node using tenacity, not by
looping back in the graph.  This keeps the graph DAG acyclic and easier to
reason about.  If tenacity exhausts retries, the agent sets state["error"]
and the supervisor routes to failure_node.
"""

from __future__ import annotations

import os
from typing import Any

import structlog
from langgraph.graph import END, START, StateGraph

from agents.state import (
    MAX_RETRIES,
    ROUTE_TO_DDL,
    ROUTE_TO_DONE,
    ROUTE_TO_FAILURE,
    ROUTE_TO_INGEST,
    ROUTE_TO_QA,
    ROUTE_TO_SCHEMA,
    ROUTE_TO_TRANSFORM,
    AgentState,
    make_reset_node,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# SOURCE REGISTRY
# Declarative list of all 6 data sources.  The Supervisor iterates this list
# and runs the full graph once per entry.  Adding a new source = one new dict.
# ---------------------------------------------------------------------------

SOURCE_REGISTRY: list[dict[str, Any]] = [
    {
        # Historical international match results since 1872 (~45k rows)
        # Full reload on each run — no incremental key exists in this dataset.
        "source_name": "historical_results",
        "source_type": "csv",
        "file_path": os.path.join(os.path.dirname(__file__), "..", "data", "results.csv"),
        "api_url": None,
        "sqlite_table": None,
    },
    {
        # All historical international goal scorers linked to results.csv
        # Full reload — joins to results on (date, home_team, away_team).
        "source_name": "historical_goals",
        "source_type": "csv",
        "file_path": os.path.join(os.path.dirname(__file__), "..", "data", "goalscorers.csv"),
        "api_url": None,
        "sqlite_table": None,
    },
    {
        # Penalty shootout outcomes — sparse (only ~500 rows)
        # Full reload.
        "source_name": "historical_shootouts",
        "source_type": "csv",
        "file_path": os.path.join(os.path.dirname(__file__), "..", "data", "shootouts.csv"),
        "api_url": None,
        "sqlite_table": None,
    },
    {
        # Live 2026 World Cup data from openfootball API
        # Daily incremental refresh keyed on watermark.
        # Produces TWO Bronze tables — custom names override the default dynamic naming.
        # Default dynamic would give: RAW_WORLDCUP_API + RAW_WORLDCUP_API_SECONDARY
        # Override gives human-readable: RAW_WC2026_MATCHES + RAW_WC2026_GOALS
        "source_name": "worldcup_api",
        "source_type": "api_json",
        "file_path": None,
        "api_url": "https://raw.githubusercontent.com/openfootball/worldcup.json/master/2026/worldcup.json",
        "sqlite_table": None,
        "table_name": "RAW_WC2026_MATCHES",        # overrides dynamic RAW_WORLDCUP_API
        "secondary_table_name": "RAW_WC2026_GOALS", # overrides dynamic RAW_WORLDCUP_API_SECONDARY
    },
    {
        # 48 WC 2026 national teams from local SQLite (fifa.db)
        # Full reload — static reference data.
        "source_name": "national_teams",
        "source_type": "sqlite",
        "file_path": os.path.join(os.path.dirname(__file__), "..", "data", "fifa.db"),
        "api_url": None,
        "sqlite_table": "national_teams",
    },
    {
        # 1172 synthetic player profiles from local SQLite (fifa.db)
        # Full reload — synthetic seed data, not updated at runtime.
        "source_name": "player_profiles",
        "source_type": "sqlite",
        "file_path": os.path.join(os.path.dirname(__file__), "..", "data", "fifa.db"),
        "api_url": None,
        "sqlite_table": "player_profiles",
    },
]


# ---------------------------------------------------------------------------
# NODE STUBS
# These are placeholder implementations used during graph wiring and testing.
# Each will be replaced by a real import once the corresponding agent file
# is built (schema_agent.py, ddl_agent.py, etc.).
# The stub contract: return a dict with the MINIMUM keys needed to drive
# routing — status and optionally error.
#
# WHY STUBS INSTEAD OF IMPORTS?
# Allows the full graph topology to be compiled and tested before any agent
# logic exists.  Prevents circular import issues during incremental builds.
# Each stub is replaced by a one-line import swap — no graph rewiring needed.
# ---------------------------------------------------------------------------

def _schema_agent_node(state: AgentState) -> dict[str, Any]:
    """
    STUB — will be replaced by: from agents.schema_agent import schema_agent_node
    Real behaviour: dispatches to infer_from_csv / infer_from_sqlite /
    infer_from_json_rows based on state["source_type"], populates schema_map.
    """
    logger.info("schema_agent", source=state["source_name"], mode="STUB")
    return {
        "schema_map": {"stub_col": str},  # placeholder schema
        "status": "schema_done",
        "error": None,
    }


def _ddl_agent_node(state: AgentState) -> dict[str, Any]:
    """
    STUB — will be replaced by: from agents.ddl_agent import ddl_agent_node
    Real behaviour: calls build_create_table_ddl() then execute_snowflake_ddl().
    """
    logger.info("ddl_agent", source=state["source_name"], mode="STUB")
    return {
        "target_table": f"DE_AI_AGENT_DEV.BRONZE.RAW_{state['source_name'].upper()}",
        "ddl_sql": "CREATE TABLE IF NOT EXISTS ... (stub_col VARCHAR);",
        "table_created": True,
        "status": "ddl_done",
        "error": None,
    }


def _ingest_agent_node(state: AgentState) -> dict[str, Any]:
    """
    STUB — will be replaced by: from agents.ingest_agent import ingest_agent_node
    Real behaviour: loads rows from source, writes to Bronze, writes dead letters,
    updates INGESTION_WATERMARKS.
    """
    logger.info("ingest_agent", source=state["source_name"], mode="STUB")
    return {
        "rows_ingested": 42,  # placeholder count
        "dead_letter_count": 0,
        "watermark_updated": True,
        "status": "ingested",
        "error": None,
    }


def _transform_agent_node(state: AgentState) -> dict[str, Any]:
    """
    STUB — will be replaced by: from agents.transform_agent import transform_agent_node
    Real behaviour: runs dbt-codegen to draft staging model, opens GitHub PR.
    NEVER auto-merges — always PR-gated.
    """
    logger.info("transform_agent", source=state["source_name"], mode="STUB")
    return {
        "dbt_model_path": f"dbt_project/models/staging/stg_{state['source_name']}.sql",
        "pr_url": "https://github.com/sanjay123-g/de-ai-agent/pull/stub-1",
        "status": "transformed",
        "error": None,
    }


def _qa_agent_node(state: AgentState) -> dict[str, Any]:
    """
    STUB — will be replaced by: from agents.qa_agent import qa_agent_node
    Real behaviour: runs dbt test on the staging model, blocks PR if any fail.
    """
    logger.info("qa_agent", source=state["source_name"], mode="STUB")
    return {
        "tests_passed": True,
        "test_failure_summary": None,
        "status": "qa_passed",
        "error": None,
    }


def _done_node(state: AgentState) -> dict[str, Any]:
    """
    Terminal success node.  Logs the completed source run summary.
    No LLM call — pure logging and status bookkeeping.

    WHY A SEPARATE DONE NODE?
    The Streamlit dashboard polls state["status"] per source.  Having an
    explicit "done" status (not just "qa_passed") means the dashboard can
    show a clean green badge without needing to know the QA node name.
    """
    logger.info(
        "pipeline_complete",
        source=state["source_name"],
        rows_ingested=state["rows_ingested"],
        dead_letter_count=state["dead_letter_count"],
        pr_url=state.get("pr_url"),
    )
    return {"status": "done"}


def _failure_node(state: AgentState) -> dict[str, Any]:
    """
    Terminal failure node.  Logs the error, emits a Slack alert payload
    (when SLACK_WEBHOOK_URL is set in .env), and marks status as "failed".

    WHY NOT RETRY HERE?
    Retry logic (tenacity) runs INSIDE each agent node before it sets
    state["error"].  By the time the Supervisor routes here, the agent has
    already exhausted its retries.  Adding graph-level retry loops creates
    cycles in the DAG which complicate state reasoning and LangGraph's
    checkpoint/replay behaviour.

    SLACK ALERT:
    Emitting from this node means every failure path (schema, ddl, ingest,
    transform, qa) is alerted through one code path.  No need to add alert
    logic to each agent individually.
    """
    error_msg = state.get("error", "unknown error")
    logger.error(
        "pipeline_failed",
        source=state["source_name"],
        status_at_failure=state["status"],
        error=error_msg,
        rows_ingested=state["rows_ingested"],
        dead_letter_count=state["dead_letter_count"],
    )
    # Slack alert hook — real implementation in agents/ingest_agent.py utility
    # _emit_slack_alert(source=state["source_name"], error=error_msg)
    return {"status": "failed"}


# ---------------------------------------------------------------------------
# ROUTING FUNCTIONS
# Each routing function reads state and returns a ROUTE_TO_* string constant.
# LangGraph maps these strings to node names via the path_map in
# add_conditional_edges().
#
# WHY SEPARATE ROUTING FUNCTIONS PER NODE?
# Each node has different success criteria.  The schema node succeeds when
# schema_map is populated; the QA node succeeds when tests_passed is True.
# A single generic router would need a long if/elif chain tied to status
# values — harder to test and modify.  One function per transition = one
# failure condition per test.
# ---------------------------------------------------------------------------

def _route_after_schema(state: AgentState) -> str:
    """schema_agent → ddl_agent (success) or failure_node (any error)."""
    if state.get("error"):
        return ROUTE_TO_FAILURE
    if state.get("schema_map") and state.get("status") == "schema_done":
        return ROUTE_TO_DDL
    # schema_map empty or status unexpected — treat as failure
    return ROUTE_TO_FAILURE


def _route_after_ddl(state: AgentState) -> str:
    """ddl_agent → ingest_agent (table created) or failure_node."""
    if state.get("error"):
        return ROUTE_TO_FAILURE
    if state.get("table_created") is True and state.get("status") == "ddl_done":
        return ROUTE_TO_INGEST
    return ROUTE_TO_FAILURE


def _route_after_ingest(state: AgentState) -> str:
    """
    ingest_agent → transform_agent (rows ingested) or failure_node.

    Dead letter records do NOT block progression — partial ingestion is
    acceptable.  Only a hard error (exception in the agent) routes to failure.
    The QA agent will catch data quality issues downstream.
    """
    if state.get("error"):
        return ROUTE_TO_FAILURE
    if state.get("status") == "ingested":
        return ROUTE_TO_TRANSFORM
    return ROUTE_TO_FAILURE


def _route_after_transform(state: AgentState) -> str:
    """transform_agent → qa_agent (PR opened) or failure_node."""
    if state.get("error"):
        return ROUTE_TO_FAILURE
    if state.get("pr_url") and state.get("status") == "transformed":
        return ROUTE_TO_QA
    return ROUTE_TO_FAILURE


def _route_after_qa(state: AgentState) -> str:
    """
    qa_agent → done_node (all tests pass) or failure_node (any test failure).

    This is the ONLY gate that blocks a PR from merging.  If tests_passed is
    False, the failure_node logs the test_failure_summary and emits a Slack
    alert.  The PR remains open but unmerged until a human fixes the model.
    """
    if state.get("error"):
        return ROUTE_TO_FAILURE
    if state.get("tests_passed") is True and state.get("status") == "qa_passed":
        return ROUTE_TO_DONE
    return ROUTE_TO_FAILURE


# ---------------------------------------------------------------------------
# GRAPH BUILDER
# ---------------------------------------------------------------------------

def build_graph(use_real_agents: bool = False):
    """
    Compile and return the LangGraph StateGraph.

    Args:
        use_real_agents: If True, import real agent functions instead of stubs.
                         Set to False during unit testing and graph topology tests.
                         Will be switched to True once all agent files are built.

    WHY use_real_agents FLAG?
    Allows the graph topology tests to run in CI without Snowflake credentials
    or Ollama running.  The flag is the seam between graph structure testing
    (runs anywhere) and integration testing (requires full stack).

    Returns:
        A compiled LangGraph CompiledStateGraph ready for .invoke() calls.
    """
    if use_real_agents:
        # These imports will resolve once each agent file is built.
        # If any import fails, the error surfaces immediately on startup —
        # not silently at runtime mid-pipeline.
        from agents.ddl_agent import ddl_agent_node
        from agents.ingest_agent import ingest_agent_node
        from agents.qa_agent import qa_agent_node
        from agents.schema_agent import schema_agent_node
        from agents.transform_agent import transform_agent_node
    else:
        schema_agent_node    = _schema_agent_node
        ddl_agent_node       = _ddl_agent_node
        ingest_agent_node    = _ingest_agent_node
        transform_agent_node = _transform_agent_node
        qa_agent_node        = _qa_agent_node

    builder = StateGraph(AgentState)

    # ── Register nodes ────────────────────────────────────────────────────
    builder.add_node(ROUTE_TO_SCHEMA,    schema_agent_node)
    builder.add_node(ROUTE_TO_DDL,       ddl_agent_node)
    builder.add_node(ROUTE_TO_INGEST,    ingest_agent_node)
    builder.add_node(ROUTE_TO_TRANSFORM, transform_agent_node)
    builder.add_node(ROUTE_TO_QA,        qa_agent_node)
    builder.add_node(ROUTE_TO_DONE,      _done_node)
    builder.add_node(ROUTE_TO_FAILURE,   _failure_node)

    # ── Entry point ───────────────────────────────────────────────────────
    # Every source run begins at schema_agent.  The Supervisor calls
    # make_reset_node() externally before invoking, so the state is clean.
    builder.add_edge(START, ROUTE_TO_SCHEMA)

    # ── Conditional edges (routing logic) ─────────────────────────────────
    builder.add_conditional_edges(
        ROUTE_TO_SCHEMA,
        _route_after_schema,
        {ROUTE_TO_DDL: ROUTE_TO_DDL, ROUTE_TO_FAILURE: ROUTE_TO_FAILURE},
    )
    builder.add_conditional_edges(
        ROUTE_TO_DDL,
        _route_after_ddl,
        {ROUTE_TO_INGEST: ROUTE_TO_INGEST, ROUTE_TO_FAILURE: ROUTE_TO_FAILURE},
    )
    builder.add_conditional_edges(
        ROUTE_TO_INGEST,
        _route_after_ingest,
        {ROUTE_TO_TRANSFORM: ROUTE_TO_TRANSFORM, ROUTE_TO_FAILURE: ROUTE_TO_FAILURE},
    )
    builder.add_conditional_edges(
        ROUTE_TO_TRANSFORM,
        _route_after_transform,
        {ROUTE_TO_QA: ROUTE_TO_QA, ROUTE_TO_FAILURE: ROUTE_TO_FAILURE},
    )
    builder.add_conditional_edges(
        ROUTE_TO_QA,
        _route_after_qa,
        {ROUTE_TO_DONE: ROUTE_TO_DONE, ROUTE_TO_FAILURE: ROUTE_TO_FAILURE},
    )

    # ── Terminal edges ────────────────────────────────────────────────────
    builder.add_edge(ROUTE_TO_DONE,    END)
    builder.add_edge(ROUTE_TO_FAILURE, END)

    return builder.compile()


# ---------------------------------------------------------------------------
# PIPELINE RUNNER
# ---------------------------------------------------------------------------

def run_pipeline(
    source_overrides: list[dict[str, Any]] | None = None,
    use_real_agents: bool = True,
) -> list[dict[str, Any]]:
    """
    Run the full ingestion pipeline across all sources in SOURCE_REGISTRY.

    Args:
        source_overrides: Pass a subset of SOURCE_REGISTRY dicts to run only
                          specific sources.  Used in tests and manual re-runs.
        use_real_agents:  Passed through to build_graph().

    Returns:
        List of final AgentState dicts, one per source run, in registry order.

    WHY ONE GRAPH INSTANCE INVOKED MULTIPLE TIMES?
    build_graph() compiles once (registers nodes, validates edges).  Each
    .invoke() call gets its own state dict (passed in via make_reset_node),
    so there is no shared mutable state between source runs.  This is more
    efficient than recompiling the graph for each source.
    """
    app = build_graph(use_real_agents=use_real_agents)
    sources = source_overrides or SOURCE_REGISTRY
    results: list[dict[str, Any]] = []

    for source_cfg in sources:
        logger.info("pipeline_start", source=source_cfg["source_name"])

        # Fresh isolated state — prevents schema bleed between sources
        initial_state = make_reset_node(
            source_name=source_cfg["source_name"],
            source_type=source_cfg["source_type"],
            file_path=source_cfg.get("file_path"),
            api_url=source_cfg.get("api_url"),
            sqlite_table=source_cfg.get("sqlite_table"),
        )

        # Apply optional table name overrides — ddl_agent has zero hardcoded names.
        # All other sources use dynamic naming: RAW_{source_name.upper()}
        if source_cfg.get("table_name"):
            initial_state["target_table"] = source_cfg["table_name"]
        if source_cfg.get("secondary_table_name"):
            initial_state["secondary_target_table"] = source_cfg["secondary_table_name"]

        final_state = app.invoke(initial_state)
        results.append(final_state)

        logger.info(
            "pipeline_result",
            source=source_cfg["source_name"],
            status=final_state.get("status"),
            rows_ingested=final_state.get("rows_ingested", 0),
            dead_letter_count=final_state.get("dead_letter_count", 0),
        )

    return results


# ---------------------------------------------------------------------------
# ENTRY POINT
# Run the full pipeline from terminal:
#   cd ~/AI_Projects/de-ai-agent
#   source .venv/bin/activate
#   python -m agents.supervisor
#
# Run a single source for testing:
#   python -m agents.supervisor --source national_teams
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run de-ai-agent pipeline")
    parser.add_argument("--source", type=str, default=None,
                        help="Run single source by name (e.g. national_teams)")
    parser.add_argument("--no-real-agents", action="store_true",
                        help="Run with stub agents (no Snowflake/Ollama needed)")
    args = parser.parse_args()

    use_real = not args.no_real_agents

    if args.source:
        override = next(
            (s for s in SOURCE_REGISTRY if s["source_name"] == args.source), None
        )
        if not override:
            print(f"Unknown source: {args.source}")
            print(f"Available: {[s['source_name'] for s in SOURCE_REGISTRY]}")
            exit(1)
        results = run_pipeline(source_overrides=[override], use_real_agents=use_real)
    else:
        results = run_pipeline(use_real_agents=use_real)

    print("\n=== PIPELINE SUMMARY ===")
    for r in results:
        status_icon = "✅" if r.get("status") == "done" else "❌"
        print(f"{status_icon} {r.get('source_name'):30s} "
              f"status={r.get('status'):12s} "
              f"rows={r.get('rows_ingested', 0):>6} "
              f"dead={r.get('dead_letter_count', 0):>4}")
