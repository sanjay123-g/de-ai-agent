"""
agents/transform_agent.py
=========================
LangGraph node: generates dbt Silver staging models and opens a GitHub PR.

WHAT THIS NODE DOES:
--------------------
1. Uses dbt-codegen to generate a base staging model SQL from the Bronze table
2. Uses llama3.1:8b to enhance the generated SQL:
     - Casts VARCHAR date columns to DATE/TIMESTAMP
     - Renames columns to snake_case conventions
     - Adds CASE WHEN for boolean normalisation (0/1 → TRUE/FALSE)
     - Adds column-level comments for ChromaDB embedding later
3. Writes the staging model file to dbt_project/models/staging/
4. Generates schema.yml entry with column descriptions + dbt tests
5. Opens a GitHub PR — NEVER auto-merges
6. QA agent gates the PR merge on dbt test results

WHY NEVER AUTO-MERGE?
---------------------
Bronze → Silver is the trust boundary. Bronze has raw uncleaned data.
Silver is what analysts query. Auto-merging means bad transformations
reach Gold without human review. Every Silver model change goes through
PR → dbt test → human approve → merge. This is the same pattern used
at Airbnb, GitLab, and most mature dbt shops.

WHY dbt-codegen?
----------------
dbt-codegen reads your Bronze table schema and generates boilerplate:
  - SELECT with every column named
  - Basic CAST statements for obvious types
The LLM then enhances this boilerplate with domain-aware transformations.
This is better than LLM-only (hallucinated column names) and better than
codegen-only (no type casting, no business logic).

WHY A SEPARATE TRANSFORM AGENT NODE?
-------------------------------------
Transform is the only node that touches the Git repo (writes files, opens PR).
Keeping it isolated means:
  - Ingest failures never leave orphaned dbt model files
  - Git operations can be retried without re-running ingestion
  - The PR URL is stored in state for the QA agent and Streamlit dashboard

STAGING MODEL NAMING CONVENTION:
---------------------------------
Bronze table RAW_HISTORICAL_RESULTS → stg_historical_results.sql
Bronze table RAW_WC2026_MATCHES     → stg_wc2026_matches.sql
General: RAW_{X} → stg_{x_lowercase}.sql
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import structlog
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from agents.state import AgentState
from config.settings import settings

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent.parent
_DBT_PROJECT_DIR = _PROJECT_ROOT / "dbt_project"
_STAGING_DIR = _DBT_PROJECT_DIR / "models" / "staging"
_SCHEMA_FILE = _STAGING_DIR / "schema.yml"

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

_llm = ChatOllama(model=settings.ollama_model, temperature=0, format="json")

_TRANSFORM_SYSTEM_PROMPT = """You are a senior analytics engineer writing dbt Silver staging models.
Given a Bronze table name and its column schema, generate a production-quality dbt staging model.

Rules:
- Source Bronze table is in schema BRONZE, referenced as {{ source('bronze', 'table_name') }}
- Apply these casts universally (not domain-specific):
    VARCHAR columns ending in _at, _date, _time, date, time → TRY_CAST(col AS DATE) or TIMESTAMP_NTZ
    NUMBER/INT columns that are boolean flags (is_, has_, was_) → col::BOOLEAN
    VARCHAR columns with only uppercase → LOWER(col) AS col
- Rename columns to snake_case if not already
- Add a _loaded_at audit column: CURRENT_TIMESTAMP() AS _loaded_at
- Exclude audit columns _ingested_at, _source_name, _pipeline_run_id from SELECT
  (these are pipeline metadata, not business data)
- Use CTEs: one `source` CTE, one `renamed` CTE, final SELECT from renamed
- Add a brief SQL comment above each column explaining what it represents

Return ONLY a JSON object with these exact keys:
{
  "model_sql": "full dbt SQL model as a string",
  "column_descriptions": {"col_name": "plain English description for schema.yml"},
  "suggested_tests": {"col_name": ["not_null", "unique"]}
}

No markdown. No explanation outside the JSON."""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(Exception),
    reraise=False,
)
def _llm_generate_staging_model(
    source_name: str,
    table_name: str,
    schema_map: dict[str, type],
) -> dict | None:
    """
    Uses llama3.1:8b to generate a dbt staging model from Bronze schema.
    Returns dict with model_sql, column_descriptions, suggested_tests.
    Returns None if LLM call fails after all retries.
    """
    # Convert types to strings for the prompt
    schema_str = {
        col: {
            str: "VARCHAR", int: "NUMBER", float: "FLOAT",
            bool: "BOOLEAN"
        }.get(t, "VARCHAR")
        for col, t in schema_map.items()
        if not col.startswith("_")  # exclude audit cols
    }

    response = _llm.invoke([
        SystemMessage(content=_TRANSFORM_SYSTEM_PROMPT),
        HumanMessage(content=f"""Source name: {source_name}
Bronze table: {table_name}
Column schema: {json.dumps(schema_str, indent=2)}

Generate the dbt Silver staging model."""),
    ])

    result = json.loads(response.content)
    return result


def _build_fallback_model(
    source_name: str,
    table_name: str,
    schema_map: dict[str, type],
    file_path: str | None = None,
) -> dict:
    """
    Generates a basic dbt staging model without LLM when Ollama is unavailable.
    Produces a valid, runnable model — just without smart type casting.
    This ensures the pipeline never blocks on LLM availability.
    """
    bare_table = table_name.split(".")[-1]  # strip DB.SCHEMA prefix
    source_ref = bare_table.lower()

    # Build column list excluding audit cols
    cols = [
        f"    {col.upper()} AS {col.lower()}"
        for col in schema_map.keys()
        if not col.startswith("_")
    ]
    cols_sql = ",\n".join(cols)

    model_sql = f"""with source as (
    select * from {{{{ source('bronze', '{bare_table}') }}}}
),

renamed as (
    select
{cols_sql},
        CURRENT_TIMESTAMP() AS _loaded_at
    from source
)

select * from renamed"""

    col_descriptions = {
        col.lower(): f"Raw {col.lower().replace('_', ' ')} from Bronze {bare_table}"
        for col in schema_map.keys()
        if not col.startswith("_")
    }

    # Data-driven tests via single-pass column profiling — replaces the
    # old "not_null on everything" default. Falls back to not_null-only
    # if file_path isn't available (e.g. SQLite/API sources not yet
    # wired to the profiler).
    nonnegative_columns: set[str] = set()
    categorical_columns: dict[str, list] = {}
    # Profiler only works on CSV files (uses csv.DictReader) — SQLite
    # sources (e.g. national_teams, player_profiles from fifa.db) are
    # binary files and must not be opened as text/CSV.
    if file_path and str(file_path).lower().endswith(".csv"):
        from ingestion.schema_inferrer import profile_columns, suggest_tests_from_profile
        profile = profile_columns(file_path, schema_map)
        suggested_tests, nonnegative_columns, categorical_columns = suggest_tests_from_profile(
            profile, schema_map
        )
        # normalize keys to lowercase to match col_descriptions
        suggested_tests = {k.lower(): v for k, v in suggested_tests.items()}
    else:
        suggested_tests = {
            col.lower(): ["not_null"]
            for col in schema_map.keys()
            if not col.startswith("_")
        }

    return {
        "model_sql": model_sql,
        "column_descriptions": col_descriptions,
        "suggested_tests": suggested_tests,
        "nonnegative_columns": {c.lower() for c in nonnegative_columns},
        "categorical_columns": categorical_columns,
    }


# ---------------------------------------------------------------------------
# FILE WRITERS
# ---------------------------------------------------------------------------

def _derive_model_name(source_name: str) -> str:
    """
    Derives dbt model filename from source_name.
    Convention: stg_{source_name_lowercase}
    RAW_HISTORICAL_RESULTS → stg_historical_results
    worldcup_api           → stg_worldcup_api
    """
    return f"stg_{source_name.lower().replace('-', '_').replace(' ', '_')}"


def _write_staging_model(model_name: str, model_sql: str) -> Path:
    """
    Writes the dbt staging SQL file.
    Creates the staging directory if it doesn't exist.
    Returns the path to the written file.
    """
    _STAGING_DIR.mkdir(parents=True, exist_ok=True)
    model_path = _STAGING_DIR / f"{model_name}.sql"
    model_path.write_text(model_sql, encoding="utf-8")
    logger.info("staging_model_written", path=str(model_path))
    return model_path


def _write_schema_yml_entry(
    model_name: str,
    column_descriptions: dict[str, str],
    suggested_tests: dict[str, list[str]],
    schema_map: dict[str, type],
    nonnegative_columns: set[str] | None = None,
    categorical_columns: dict[str, list] | None = None,
) -> None:
    """
    Appends a schema.yml entry for the staging model.
    Creates schema.yml if it doesn't exist.

    Schema.yml serves two purposes:
    1. dbt test definitions (not_null, unique, accepted_values, relationships)
    2. Column descriptions that get embedded into ChromaDB by the RAG embedder

    WHY COLUMN-LEVEL DESCRIPTIONS?
    The semantic agent answers questions like "what does home_score mean?"
    by searching ChromaDB which is populated from these descriptions.
    Without them, the RAG layer has nothing to search.

    DATA GOVERNANCE — PII TAGGING:
    Any column containing player/scorer/person names gets meta: {pii: true}
    This satisfies the data governance requirement for PII identification.
    """
    _STAGING_DIR.mkdir(parents=True, exist_ok=True)

    # Load existing schema.yml or create fresh structure
    import yaml  # imported here to avoid top-level dep if yaml not installed

    if _SCHEMA_FILE.exists():
        with open(_SCHEMA_FILE, encoding="utf-8") as f:
            schema_doc = yaml.safe_load(f) or {"version": 2, "models": [], "sources": []}
    else:
        schema_doc = {
            "version": 2,
            "sources": [{
                "name": "bronze",
                "database": settings.snowflake_database,
                "schema": "BRONZE",
                "tables": []
            }],
            "models": []
        }

    # Build column entries with descriptions, tests, and PII tags
    _PII_KEYWORDS = {"name", "scorer", "player", "person", "email", "phone", "address"}

    columns = []
    for col, desc in column_descriptions.items():
        col_entry: dict[str, Any] = {
            "name": col,
            "description": desc,
        }

        # PII tagging — data governance requirement (whole-word match only,
        # avoids false positives like "tournament" containing "name")
        import re as _re
        if any(_re.search(rf"\b{kw}\b", col.lower()) for kw in _PII_KEYWORDS):
            col_entry["meta"] = {"pii": True}
            logger.warning("pii_column_tagged", model=model_name, column=col)

        # dbt tests
        tests: list[Any] = list(suggested_tests.get(col, []))
        if nonnegative_columns and col.lower() in nonnegative_columns:
            tests.append({
                "dbt_expectations.expect_column_values_to_be_between": {
                    "min_value": 0,
                    "strictly": False,
                }
            })
        # Categorical detection intentionally NOT auto-enforced as a hard
        # test — sampled categorical columns (e.g. tournament names) are
        # often open, growing sets in real data; a fixed accepted_values
        # list from a sample breaks as new legitimate values appear.
        # Surfaced as metadata for human/LLM review instead.
        if categorical_columns and col.lower() in categorical_columns:
            col_entry.setdefault("meta", {})["candidate_categorical"] = True
        if tests:
            col_entry["tests"] = tests

        columns.append(col_entry)

    # Add or update model entry
    model_entry = {
        "name": model_name,
        "description": f"Silver staging model for {model_name.replace('stg_', '').replace('_', ' ')}",
        "columns": columns,
    }

    # Replace if model already exists, append if new
    existing_names = [m["name"] for m in schema_doc.get("models", [])]
    if model_name in existing_names:
        schema_doc["models"] = [
            model_entry if m["name"] == model_name else m
            for m in schema_doc["models"]
        ]
    else:
        schema_doc.setdefault("models", []).append(model_entry)

    with open(_SCHEMA_FILE, "w", encoding="utf-8") as f:
        yaml.dump(schema_doc, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    logger.info("schema_yml_updated", model=model_name, columns=len(columns))


# ---------------------------------------------------------------------------
# GITHUB PR OPENER
# ---------------------------------------------------------------------------

def _open_github_pr(
    model_name: str,
    source_name: str,
    model_path: Path,
) -> str | None:
    """
    Creates a feature branch, commits the staging model, and opens a GitHub PR.
    Returns the PR URL, or None if git operations fail (non-blocking).

    WHY NON-BLOCKING?
    Git failures (no remote configured, no GitHub token) should not prevent
    the dbt model from being written locally. The PR step is best-effort in
    dev — mandatory in production CI/CD.

    BRANCH NAMING: feature/stg_{source_name}_{timestamp}
    This ensures each pipeline run gets a unique branch — no conflicts on
    re-runs.

    GITHUB TOKEN:
    Reads GITHUB_TOKEN from environment. If not set, logs a warning and
    skips the PR step. The model file is still written locally.
    """
    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        logger.warning(
            "github_pr_skipped",
            reason="GITHUB_TOKEN not set",
            model=model_name,
            action="model written locally, PR skipped",
        )
        return None

    import time
    branch_name = f"feature/stg_{source_name}_{int(time.time())}"

    try:
        repo_root = str(_PROJECT_ROOT)
        git = lambda cmd: subprocess.run(
            cmd, cwd=repo_root, capture_output=True, text=True, check=True
        )

        git(["git", "checkout", "-b", branch_name])
        git(["git", "add", str(model_path), str(_SCHEMA_FILE)])
        git(["git", "commit", "-m",
             f"feat: add Silver staging model {model_name}\n\n"
             f"Auto-generated by transform_agent for source: {source_name}\n"
             f"Requires QA agent dbt test pass before merge."])
        git(["git", "push", "origin", branch_name])

        # Open PR via GitHub CLI (gh) if available
        result = subprocess.run(
            ["gh", "pr", "create",
             "--title", f"feat: Silver staging model {model_name}",
             "--body", (
                 f"Auto-generated staging model for `{source_name}`.\n\n"
                 f"**Do not merge until QA agent confirms all dbt tests pass.**\n\n"
                 f"Model path: `{model_path.relative_to(_PROJECT_ROOT)}`"
             ),
             "--base", "main",
             "--head", branch_name],
            cwd=repo_root, capture_output=True, text=True,
        )

        if result.returncode == 0:
            pr_url = result.stdout.strip()
            logger.info("github_pr_opened", model=model_name, pr_url=pr_url)
            return pr_url
        else:
            logger.warning("github_pr_failed", stderr=result.stderr)
            return None

    except subprocess.CalledProcessError as e:
        logger.warning("github_pr_error", error=str(e), model=model_name)
        return None


# ---------------------------------------------------------------------------
# TRANSFORM AGENT NODE — main LangGraph entry point
# ---------------------------------------------------------------------------

def transform_agent_node(state: AgentState) -> dict[str, Any]:
    """
    LangGraph node — generates dbt Silver staging model and opens GitHub PR.

    Works for any source — model name and SQL derived dynamically.
    For worldcup_api with secondary_schema_map: generates TWO staging models
    (stg_worldcup_api_matches + stg_worldcup_api_goals).
    """
    source_name  = state["source_name"]
    schema_map   = state.get("schema_map")
    target_table = state.get("target_table", "")

    if not schema_map:
        return {"error": f"transform_agent: schema_map empty for '{source_name}'"}

    logger.info("transform_agent_start", source=source_name)

    try:
        # ── Primary staging model ─────────────────────────────────────────
        model_name  = _derive_model_name(source_name)
        bare_table  = target_table.split(".")[-1] if target_table else f"RAW_{source_name.upper()}"

        llm_result = None
        try:
            llm_result = _llm_generate_staging_model(source_name, bare_table, schema_map)
        except Exception as e:
            logger.warning("transform_llm_fallback", source=source_name, reason=str(e))

        if not llm_result:
            llm_result = _build_fallback_model(
                source_name, bare_table, schema_map, state.get("file_path")
            )
            logger.info("transform_fallback_model_used", source=source_name)

        model_path = _write_staging_model(model_name, llm_result["model_sql"])

        # Filter out not_null tests on columns known to be nullable from
        # real data (schema_agent's null-rate scan), regardless of whether
        # this schema.yml came from the LLM path or the fallback path —
        # both must respect the same data-driven nullability facts.
        raw_tests = llm_result.get("suggested_tests", {})
        known_nullable = {c.lower() for c in (state.get("nullable_columns") or set())}
        filtered_tests = {}
        for col, tests in raw_tests.items():
            if col in known_nullable:
                tests = [t for t in tests if t != "not_null"]
            if tests:
                filtered_tests[col] = tests

        try:
            _write_schema_yml_entry(
                model_name,
                llm_result.get("column_descriptions", {}),
                filtered_tests,
                schema_map,
                llm_result.get("nonnegative_columns"),
                llm_result.get("categorical_columns"),
            )
        except Exception as e:
            # schema.yml write failure is non-blocking — model SQL is more important
            logger.warning("schema_yml_write_failed", source=source_name, error=str(e))

        # ── Secondary staging model (worldcup_api goals, or any multi-table source)
        secondary_schema = state.get("secondary_schema_map")
        secondary_table  = state.get("secondary_target_table", "")

        if secondary_schema and secondary_table:
            sec_model_name = f"{model_name}_secondary"
            sec_bare_table = secondary_table.split(".")[-1]

            sec_llm = None
            try:
                sec_llm = _llm_generate_staging_model(
                    f"{source_name}_secondary", sec_bare_table, secondary_schema
                )
            except Exception:
                pass

            if not sec_llm:
                sec_llm = _build_fallback_model(
                    f"{source_name}_secondary", sec_bare_table, secondary_schema
                )

            _write_staging_model(sec_model_name, sec_llm["model_sql"])
            try:
                _write_schema_yml_entry(
                    sec_model_name,
                    sec_llm.get("column_descriptions", {}),
                    sec_llm.get("suggested_tests", {}),
                    secondary_schema,
                )
            except Exception:
                pass

        # ── GitHub PR ─────────────────────────────────────────────────────
        pr_url = _open_github_pr(model_name, source_name, model_path)

        logger.info(
            "transform_agent_complete",
            source=source_name,
            model=model_name,
            model_path=str(model_path),
            pr_url=pr_url,
        )

        return {
            "dbt_model_path": str(model_path.relative_to(_PROJECT_ROOT)),
            "pr_url":         pr_url or f"local:{model_path}",
            "status":         "transformed",
            "error":          None,
        }

    except Exception as e:
        logger.error("transform_agent_error", source=source_name, error=str(e))
        return {"error": f"transform_agent failed [{source_name}]: {str(e)}"}
