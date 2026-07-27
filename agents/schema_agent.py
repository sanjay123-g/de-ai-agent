"""
agents/schema_agent.py
======================
LangGraph node: infers and validates schema for ANY data source.

Domain-agnostic — works for football stats, sales CSVs, IoT JSON,
user profiles, financial transactions, or any other structured source.
The football data in this project is purely the test dataset.

TWO-STEP APPROACH:
  Step 1 — schema_inferrer.py reads actual data and derives Python types
  Step 2 — LLM reviews inferred types against universal data engineering
            rules and corrects mistakes (e.g. ID columns that look numeric
            but should be str, boolean flags inferred as int 0/1)

If the LLM is unavailable, Step 1 result is used as-is — pipeline continues.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import structlog
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from agents.state import AgentState
from config.settings import settings
from ingestion.schema_inferrer import (
    infer_from_csv,
    infer_from_json_rows,
    infer_from_sqlite,
    infer_nullable_columns,
)

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# TYPE MAPPING UTILITIES
# ---------------------------------------------------------------------------

_TYPE_TO_STR: dict[type, str] = {
    str:   "str",
    int:   "int",
    float: "float",
    bool:  "bool",
}

_STR_TO_TYPE: dict[str, type] = {v: k for k, v in _TYPE_TO_STR.items()}


def _schema_to_str(schema: dict[str, type]) -> dict[str, str]:
    return {col: _TYPE_TO_STR.get(t, "str") for col, t in schema.items()}


def _str_to_schema(schema: dict[str, str]) -> dict[str, type]:
    return {col: _STR_TO_TYPE.get(t, str) for col, t in schema.items()}


# ---------------------------------------------------------------------------
# LLM — domain-agnostic schema validator
# ---------------------------------------------------------------------------

_llm = ChatOllama(model=settings.ollama_model, temperature=0, format="json")

# Generic data engineering rules — no domain-specific assumptions.
# These rules apply universally across any industry or dataset type.
_SYSTEM_PROMPT = """You are a senior data engineer reviewing column type inferences.
Apply these universal data engineering rules to correct mistakes:

ALWAYS str:
- Any column ending in: _id, _code, _key, _ref, _num, _no, _hash, _uuid
- Any column containing: code, identifier, reference, key, hash, uuid, slug
- Phone numbers, zip codes, postal codes — look numeric but must be str
- Version strings, status codes, category codes

ALWAYS int:
- Columns containing: count, qty, quantity, num, number, rank, position, age, year, month, day
- Score, goals, points, votes, duration_seconds, duration_minutes
- Any column that is clearly a whole-number measurement

ALWAYS float:
- Columns containing: price, amount, cost, revenue, salary, rate, ratio, pct, percent, latitude, longitude, weight, height, temperature, score (if fractional)
- Any column that represents a measured/calculated value that can be fractional

ALWAYS bool:
- Columns starting with: is_, has_, can_, should_, was_, will_
- Columns ending with: _flag, _active, _enabled, _visible, _deleted
- Binary columns containing only 0/1, true/false, yes/no, t/f

ALWAYS str (Bronze layer rule — parse in Silver):
- Any column containing: date, time, timestamp, created_at, updated_at, dt
- Bronze layer stores raw strings; Silver dbt models cast to DATE/TIMESTAMP

Return ONLY a JSON object:
{
  "validated_schema": {"col_name": "type_name", ...},
  "corrections": ["description of each type correction made"],
  "issues": ["data quality concerns (nullable PK, suspicious values, etc.)"]
}

type_name must be one of: str, int, float, bool
No markdown. No explanation outside the JSON."""


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(Exception),
    reraise=False,
)
def _llm_validate_schema(
    source_name: str,
    schema_map: dict[str, type],
) -> dict[str, type] | None:
    schema_str = _schema_to_str(schema_map)
    response = _llm.invoke([
        SystemMessage(content=_SYSTEM_PROMPT),
        HumanMessage(content=f"Source: {source_name}\n\nInferred schema:\n{json.dumps(schema_str, indent=2)}\n\nValidate and correct."),
    ])
    result = json.loads(response.content)
    validated = result.get("validated_schema", schema_str)
    corrections = result.get("corrections", [])
    issues = result.get("issues", [])
    if corrections:
        logger.info("schema_llm_corrections", source=source_name, corrections=corrections)
    if issues:
        logger.warning("schema_llm_issues", source=source_name, issues=issues)
    return _str_to_schema(validated)


def _validate_with_fallback(
    source_name: str,
    schema_map: dict[str, type],
) -> dict[str, type]:
    """LLM validation with guaranteed fallback — pipeline never dies over schema validation."""
    try:
        result = _llm_validate_schema(source_name, schema_map)
        if result is None:
            raise ValueError("LLM returned None")
        return result
    except Exception as e:
        logger.warning(
            "schema_llm_fallback",
            source=source_name,
            reason=str(e),
            action="using_inferred_schema_unchanged",
        )
        return schema_map


# ---------------------------------------------------------------------------
# JSON FLATTENER
# Handles nested JSON APIs that produce multiple flat table streams.
# Currently wired for openfootball worldcup.json structure.
# To support a new nested API: add a new flattener function and dispatch
# on source_name or a shape-detection heuristic.
# ---------------------------------------------------------------------------

def flatten_nested_json(data: dict[str, Any]) -> tuple[list[dict], list[dict]]:
    """
    Flattens worldcup.json → (match_rows, goal_rows).

    Structure: rounds[] → matches[] → goals1[]/goals2[]
    Output:
      match_rows — one flat dict per match, all scalar fields
      goal_rows  — one flat dict per goal, with match FK fields

    WHY TWO TABLES NOT ONE WITH VARIANT?
    Separate flat tables = simple equi-joins in Silver dbt models.
    VARIANT columns require LATERAL FLATTEN in every query — unnecessary
    complexity when the nested structure is known and fixed.

    To add support for a different nested API (e.g. Stripe events JSON):
    Write a new flatten_stripe_events() function with the same signature
    and dispatch to it in schema_agent_node based on source_name.
    """
    match_rows: list[dict] = []
    goal_rows:  list[dict] = []

    # Real API structure (verified against live response 2026-07-26):
    # top-level "matches" array, no "rounds" wrapper. team1/team2 are
    # plain strings, not objects. score is {"ft": [n,n], "ht": [n,n]}.
    # goals1/goals2 are [{"name": str, "minute": str, ...}].
    for idx, match in enumerate(data.get("matches", [])):
        team1 = str(match.get("team1", ""))
        team2 = str(match.get("team2", ""))
        score = match.get("score", {}) or {}
        ft    = score.get("ft") or [None, None]
        ht    = score.get("ht") or [None, None]
        round_name = str(match.get("round", ""))

        match_rows.append({
            "match_num":      idx,
            "match_date":     str(match.get("date", "")),
            "match_time":     str(match.get("time", "")),
            "round_name":     round_name,
            "group_name":     str(match.get("group", "")),
            "ground":         str(match.get("ground", "")),
            "team1_name":     team1,
            "team2_name":     team2,
            "score_ft_team1": ft[0] if len(ft) > 0 else None,
            "score_ft_team2": ft[1] if len(ft) > 1 else None,
            "score_ht_team1": ht[0] if len(ht) > 0 else None,
            "score_ht_team2": ht[1] if len(ht) > 1 else None,
        })

        for goal in (match.get("goals1") or []):
            goal_rows.append({
                "match_num":     idx,
                "match_date":    str(match.get("date", "")),
                "round_name":    round_name,
                "team1_name":    team1,
                "team2_name":    team2,
                "scoring_team":  team1,
                "scorer_name":   str(goal.get("name", "")),
                "minute":        str(goal.get("minute", "")),
                "is_team1_goal": True,
                "is_own_goal":   bool(goal.get("owngoal", False)),
                "is_penalty":    bool(goal.get("penalty", False)),
            })

        for goal in (match.get("goals2") or []):
            goal_rows.append({
                "match_num":     idx,
                "match_date":    str(match.get("date", "")),
                "round_name":    round_name,
                "team1_name":    team1,
                "team2_name":    team2,
                "scoring_team":  team2,
                "scorer_name":   str(goal.get("name", "")),
                "minute":        str(goal.get("minute", "")),
                "is_team1_goal": False,
                "is_own_goal":   bool(goal.get("owngoal", False)),
                "is_penalty":    bool(goal.get("penalty", False)),
            })

    logger.info("flatten_json", matches=len(data.get("matches", [])),
                match_rows=len(match_rows), goal_rows=len(goal_rows))
    return match_rows, goal_rows


# ---------------------------------------------------------------------------
# SCHEMA AGENT NODE
# ---------------------------------------------------------------------------

def schema_agent_node(state: AgentState) -> dict[str, Any]:
    """
    LangGraph node — infers and validates schema for any source type.
    Dispatches on state["source_type"]: csv | sqlite | api_json
    """
    source_name = state["source_name"]
    source_type = state["source_type"]
    logger.info("schema_agent_start", source=source_name, source_type=source_type)

    try:
        if source_type == "csv":
            if not state.get("file_path"):
                raise ValueError("file_path is None for CSV source")
            schema_map = infer_from_csv(state["file_path"])
            nullable_columns = infer_nullable_columns(state["file_path"])
            logger.info(
                "schema_inferred",
                source=source_name,
                columns=len(schema_map),
                nullable_columns=sorted(nullable_columns),
            )
            return {
                "schema_map": _validate_with_fallback(source_name, schema_map),
                "nullable_columns": nullable_columns,
                "status": "schema_done",
                "error": None,
            }

        elif source_type == "sqlite":
            if not state.get("file_path"):
                raise ValueError("file_path is None for SQLite source")
            if not state.get("sqlite_table"):
                raise ValueError("sqlite_table is None for SQLite source")
            schema_map = infer_from_sqlite(state["file_path"], state["sqlite_table"])
            logger.info("schema_inferred", source=source_name, columns=len(schema_map))
            return {
                "schema_map": _validate_with_fallback(source_name, schema_map),
                "status": "schema_done",
                "error": None,
            }

        elif source_type == "api_json":
            if not state.get("api_url"):
                raise ValueError("api_url is None for api_json source")
            response = httpx.get(state["api_url"], timeout=30, follow_redirects=True)
            response.raise_for_status()
            data = response.json()
            logger.info("api_fetched", source=source_name, bytes=len(response.content))

            match_rows, goal_rows = flatten_nested_json(data)
            if not match_rows:
                raise ValueError("flatten_nested_json produced 0 rows — check API structure")

            match_schema = infer_from_json_rows(match_rows)
            goal_schema  = infer_from_json_rows(goal_rows) if goal_rows else {}

            return {
                "schema_map":           _validate_with_fallback(f"{source_name}_matches", match_schema),
                "secondary_schema_map": _validate_with_fallback(f"{source_name}_goals", goal_schema) if goal_schema else None,
                "status":               "schema_done",
                "error":                None,
            }

        else:
            raise ValueError(
                f"Unknown source_type: '{source_type}'. "
                f"Supported: csv | sqlite | api_json. "
                f"To add a new type, extend schema_agent_node() dispatch."
            )

    except Exception as e:
        logger.error("schema_agent_error", source=source_name, error=str(e))
        return {"error": f"schema_agent failed [{source_name}]: {str(e)}"}
