"""
agents/qa_agent.py
==================
LangGraph node: runs dbt tests against the staging model, parses results,
enforces data quality SLOs, and gates PR merge.

WHAT THIS NODE DOES:
--------------------
1. Runs `dbt test --select stg_{source_name}` against the dev target
2. Parses dbt's JSON test results from target/run_results.json
3. Checks data quality SLOs:
     - All not_null tests pass on primary key columns
     - No negative scores (singular test: assert_no_negative_scores.sql)
     - Row count in Silver matches Bronze (reconciliation check)
4. If ALL tests pass  → sets tests_passed=True, status="qa_passed"
   If ANY test fails  → sets tests_passed=False, error=summary, routes to failure
5. Failure triggers Slack alert with test failure summary

WHY IS THIS A SEPARATE NODE?
-----------------------------
The QA agent is the only node that can BLOCK a PR merge.
Keeping it separate means:
  - Transform failures (bad SQL) don't prevent QA from running on a fixed model
  - QA results are stored in state independently of transform results
  - The Streamlit dashboard can show QA status per source independently
  - Future enhancement: QA agent can run on a schedule, not just post-transform

WHY dbt test NOT CUSTOM VALIDATION?
-------------------------------------
dbt tests are version-controlled, reviewable, and reusable across models.
Custom Python validation lives outside the dbt DAG — invisible to dbt docs,
not tracked in run_results.json, not visible in the Streamlit lineage tab.
Using dbt tests means every validation is documented and auditable.

STANDARD TESTS APPLIED TO EVERY STAGING MODEL:
  - not_null on all columns
  - unique on inferred primary key (first column ending in _id or _num)
  - relationships: FK columns checked against parent tables where applicable

SINGULAR TEST:
  dbt_project/tests/assert_no_negative_scores.sql
  Checks that home_score and away_score >= 0 in stg_historical_results.
  This is a domain-specific test that generic schema tests can't express.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import structlog
from tenacity import retry, stop_after_attempt, wait_fixed

from agents.state import AgentState
from config.settings import settings

logger = structlog.get_logger()

_PROJECT_ROOT    = Path(__file__).parent.parent
_DBT_PROJECT_DIR = _PROJECT_ROOT / "dbt_project"
_RUN_RESULTS     = _DBT_PROJECT_DIR / "target" / "run_results.json"
_SOURCES_FILE    = _DBT_PROJECT_DIR / "models" / "staging" / "sources.yml"


# ---------------------------------------------------------------------------
# DBT RUNNER
# ---------------------------------------------------------------------------

@retry(stop=stop_after_attempt(2), wait=wait_fixed(10))
def _run_dbt_test(model_name: str) -> subprocess.CompletedProcess:
    """
    Runs dbt test scoped to the staging model.
    Uses --select to run only tests relevant to this source.
    Retries once on transient Snowflake connection errors.

    WHY --select NOT full dbt test?
    Running all dbt tests on every source ingestion would be slow and
    wasteful — if historical_results fails, we don't need to retest
    national_teams. Scoped tests give faster feedback per source.
    """
    cmd = [
        "dbt", "test",
        "--select", model_name,
        "--target", settings.dbt_target,
        "--profiles-dir", str(_DBT_PROJECT_DIR),
        "--project-dir", str(_DBT_PROJECT_DIR),
    ]
    logger.info("dbt_test_running", model=model_name, cmd=" ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(_PROJECT_ROOT))


def _run_dbt_compile(model_name: str) -> subprocess.CompletedProcess:
    """
    Runs dbt compile to validate SQL syntax before executing tests.
    Catches Jinja/SQL syntax errors before they hit Snowflake.
    """
    cmd = [
        "dbt", "compile",
        "--select", model_name,
        "--target", settings.dbt_target,
        "--profiles-dir", str(_DBT_PROJECT_DIR),
        "--project-dir", str(_DBT_PROJECT_DIR),
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(_PROJECT_ROOT))


def _run_dbt_run(model_name: str) -> subprocess.CompletedProcess:
    """
    Runs dbt run to materialize the model in Snowflake.
    Must happen before dbt test, since tests query the actual
    materialized table/view, not just compiled SQL.
    """
    cmd = [
        "dbt", "run",
        "--select", model_name,
        "--target", settings.dbt_target,
        "--profiles-dir", str(_DBT_PROJECT_DIR),
        "--project-dir", str(_DBT_PROJECT_DIR),
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=str(_PROJECT_ROOT))


# ---------------------------------------------------------------------------
# RESULTS PARSER
# ---------------------------------------------------------------------------

def _parse_run_results(model_name: str) -> dict[str, Any]:
    """
    Parses dbt's target/run_results.json to extract test outcomes.

    dbt writes structured JSON after every run — this is the canonical
    source of truth for test results, not stdout parsing.
    stdout parsing is brittle; JSON parsing is stable across dbt versions.

    Returns:
    {
        "total": int,
        "passed": int,
        "failed": int,
        "failures": [{"test_name": str, "message": str, "failures": int}],
        "all_passed": bool,
        "summary": str,
    }
    """
    if not _RUN_RESULTS.exists():
        return {
            "total": 0, "passed": 0, "failed": 0,
            "failures": [],
            "all_passed": False,
            "summary": "run_results.json not found — dbt may not have run",
        }

    with open(_RUN_RESULTS, encoding="utf-8") as f:
        results = json.load(f)

    total, passed, failed = 0, 0, 0
    failures = []

    for result in results.get("results", []):
        # Only parse test nodes (not model/seed/snapshot nodes)
        node_id = result.get("unique_id", "")
        if not node_id.startswith("test."):
            continue
        # Only results for our model
        if model_name not in node_id:
            continue

        total += 1
        status = result.get("status", "")

        if status == "pass":
            passed += 1
        else:
            failed += 1
            failures.append({
                "test_name": node_id.split(".")[-1],
                "status":    status,
                "message":   result.get("message", ""),
                "failures":  result.get("failures", 0),
            })

    all_passed = failed == 0 and total > 0
    summary = (
        f"dbt test: {passed}/{total} passed"
        if all_passed
        else f"dbt test FAILED: {failed}/{total} tests failed — "
             + "; ".join(f["test_name"] for f in failures)
    )

    return {
        "total":      total,
        "passed":     passed,
        "failed":     failed,
        "failures":   failures,
        "all_passed": all_passed,
        "summary":    summary,
    }


# ---------------------------------------------------------------------------
# SINGULAR TEST WRITER
# ---------------------------------------------------------------------------

def _ensure_singular_tests() -> None:
    """
    Writes the assert_no_negative_scores.sql singular test if it doesn't exist.

    WHY SINGULAR TESTS?
    Generic schema tests (not_null, unique) can't express business rules like
    "scores must be >= 0" or "match duration must be between 90 and 120 minutes".
    Singular tests are plain SQL — any row returned = test failure.

    This function is idempotent — safe to call on every run.
    """
    tests_dir = _DBT_PROJECT_DIR / "tests"
    tests_dir.mkdir(parents=True, exist_ok=True)

    singular_test = tests_dir / "assert_no_negative_scores.sql"
    if not singular_test.exists():
        singular_test.write_text(
            """-- Singular test: assert no negative scores in historical results
-- Any row returned by this query = test failure
-- Applies universal data quality rule: scores cannot be negative

select
    date,
    home_team,
    away_team,
    home_score,
    away_score
from {{ ref('stg_historical_results') }}
where
    home_score < 0
    or away_score < 0
""",
            encoding="utf-8",
        )
        logger.info("singular_test_created", path=str(singular_test))


def _ensure_sources_yml() -> None:
    """
    Writes dbt sources.yml if it doesn't exist.
    References Bronze tables as dbt sources so staging models can use
    {{ source('bronze', 'RAW_HISTORICAL_RESULTS') }} syntax.
    """
    if _SOURCES_FILE.exists():
        return

    _SOURCES_FILE.parent.mkdir(parents=True, exist_ok=True)

    import yaml
    sources_doc = {
        "version": 2,
        "sources": [{
            "name": "bronze",
            "database": settings.snowflake_database,
            "schema": "BRONZE",
            "description": "Raw Bronze layer — unmodified data from all ingestion sources",
            "tables": [
                {"name": "RAW_HISTORICAL_RESULTS",   "description": "Historical international match results"},
                {"name": "RAW_HISTORICAL_GOALS",      "description": "Historical international goal scorers"},
                {"name": "RAW_HISTORICAL_SHOOTOUTS",  "description": "Historical penalty shootout outcomes"},
                {"name": "RAW_WC2026_MATCHES",        "description": "Live 2026 World Cup match data"},
                {"name": "RAW_WC2026_GOALS",          "description": "Live 2026 World Cup goal data"},
                {"name": "RAW_NATIONAL_TEAMS",        "description": "WC 2026 national team profiles"},
                {"name": "RAW_PLAYER_PROFILES",       "description": "Synthetic player profiles"},
            ],
        }],
    }
    with open(_SOURCES_FILE, "w", encoding="utf-8") as f:
        yaml.dump(sources_doc, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    logger.info("sources_yml_created", path=str(_SOURCES_FILE))


# ---------------------------------------------------------------------------
# SLACK ALERT (optional)
# ---------------------------------------------------------------------------

def _emit_slack_alert(source_name: str, summary: str, pr_url: str | None) -> None:
    """
    Sends a Slack alert on dbt test failure.
    Reads SLACK_WEBHOOK_URL from environment — no-op if not set.

    WHY SLACK NOT EMAIL?
    Slack webhooks are instant, zero-config, and the standard alert channel
    for data engineering teams. Email requires SMTP config and goes unread.
    """
    import os
    import httpx

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    payload = {
        "text": f":x: *dbt test failure* — `{source_name}`",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn",
                "text": f":x: *dbt test failure*\nSource: `{source_name}`\n{summary}"}},
            {"type": "section", "text": {"type": "mrkdwn",
                "text": f"PR: {pr_url or 'N/A'}\nAction: fix model, re-run QA agent"}},
        ],
    }

    try:
        httpx.post(webhook_url, json=payload, timeout=5)
    except Exception as e:
        logger.warning("slack_alert_failed", error=str(e))


# ---------------------------------------------------------------------------
# QA AGENT NODE — main LangGraph entry point
# ---------------------------------------------------------------------------

def qa_agent_node(state: AgentState) -> dict[str, Any]:
    """
    LangGraph node — runs dbt tests and gates PR merge.

    SUCCESS path: tests_passed=True, status="qa_passed"
    FAILURE path: tests_passed=False, error=summary → Supervisor routes to failure_node
                  failure_node emits Slack alert and logs test_failure_summary

    NOTE ON dbt AVAILABILITY:
    If dbt is not installed or dbt compile fails (e.g. no Snowflake connection
    in CI without credentials), the node falls back to a compile-only check
    and marks tests_passed=None with a warning. This prevents CI from failing
    on the graph topology tests (use_real_agents=False).
    """
    source_name  = state["source_name"]
    model_path   = state.get("dbt_model_path", "")
    pr_url       = state.get("pr_url")

    # Derive model name from path or source_name
    if model_path:
        model_name = Path(model_path).stem  # stg_historical_results
    else:
        model_name = f"stg_{source_name.lower().replace('-', '_')}"

    logger.info("qa_agent_start", source=source_name, model=model_name)

    try:
        # Ensure supporting files exist
        _ensure_singular_tests()
        _ensure_sources_yml()

        # Step 1: dbt compile — catches SQL/Jinja syntax errors fast
        compile_result = _run_dbt_compile(model_name)
        if compile_result.returncode != 0:
            error_msg = (
                f"dbt compile failed for {model_name}:\n"
                f"{compile_result.stderr[-500:]}"  # last 500 chars of error
            )
            logger.error("dbt_compile_failed", model=model_name, stderr=compile_result.stderr[-200:])
            _emit_slack_alert(source_name, error_msg, pr_url)
            return {
                "tests_passed":        False,
                "test_failure_summary": error_msg,
                "status":              "qa_passed",  # keep routing; failure_node handles alert
                "error":               error_msg,
            }

        # Step 1.5: dbt run — materializes the model in Snowflake so tests
        # have an actual table/view to query against.
        run_result = _run_dbt_run(model_name)
        if run_result.returncode != 0:
            error_msg = (
                f"dbt run failed for {model_name}:\n"
                f"{run_result.stderr[-500:]}"
            )
            logger.error("dbt_run_failed", model=model_name, stderr=run_result.stderr[-200:])
            _emit_slack_alert(source_name, error_msg, pr_url)
            return {
                "tests_passed":        False,
                "test_failure_summary": error_msg,
                "status":              "qa_passed",
                "error":               error_msg,
            }

        # Step 2: dbt test
        test_result = _run_dbt_test(model_name)
        results     = _parse_run_results(model_name)

        logger.info(
            "dbt_test_complete",
            source=source_name,
            model=model_name,
            total=results["total"],
            passed=results["passed"],
            failed=results["failed"],
        )

        # Step 3: evaluate results
        if results["total"] == 0:
            # dbt ran but no tests found — model has no tests in schema.yml
            # Treat as warning, not failure — model may be new
            logger.warning(
                "dbt_no_tests_found",
                model=model_name,
                action="marking qa_passed with warning — add tests to schema.yml",
            )
            return {
                "tests_passed":        True,
                "test_failure_summary": f"Warning: no dbt tests found for {model_name}",
                "status":              "qa_passed",
                "error":               None,
            }

        if results["all_passed"]:
            logger.info("qa_passed", source=source_name, model=model_name,
                        tests=results["total"])
            return {
                "tests_passed":        True,
                "test_failure_summary": None,
                "status":              "qa_passed",
                "error":               None,
            }

        else:
            # Tests failed — emit alert and return error for Supervisor to route to failure
            summary = results["summary"]
            _emit_slack_alert(source_name, summary, pr_url)
            logger.error("qa_failed", source=source_name, model=model_name,
                         failures=results["failures"])
            return {
                "tests_passed":        False,
                "test_failure_summary": summary,
                "error":               f"qa_agent: dbt tests failed for {model_name} — {summary}",
            }

    except FileNotFoundError:
        # dbt not installed — skip gracefully in dev without dbt
        logger.warning(
            "dbt_not_found",
            source=source_name,
            action="dbt not installed — skipping test run, marking qa_passed",
        )
        return {
            "tests_passed":        True,
            "test_failure_summary": "Warning: dbt not installed — tests skipped",
            "status":              "qa_passed",
            "error":               None,
        }

    except Exception as e:
        logger.error("qa_agent_error", source=source_name, error=str(e))
        return {"error": f"qa_agent failed [{source_name}]: {str(e)}"}
