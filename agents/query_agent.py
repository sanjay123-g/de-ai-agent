"""
agents/query_agent.py

Natural-language to SQL query agent over the Gold-layer marts.
Uses local qwen2.5-coder:14b for SQL generation. Every generated query
passes through sql_guardrails.validate_readonly_sql() before execution.
"""

import ollama
import structlog
from config.settings import get_settings
from agents.sql_guardrails import validate_readonly_sql, UnsafeSQLError
import snowflake.connector

logger = structlog.get_logger()
settings = get_settings()

_MART_SCHEMA_CONTEXT = """
Available tables (Snowflake schema SILVER):

mart_team_performance(team, tournament, matches_played, wins, draws, losses,
  win_pct, points, goals_for, goals_against, goal_difference,
  avg_goals_scored_per_match, avg_goals_conceded_per_match, clean_sheets,
  failed_to_score, biggest_win_margin, home_matches, away_matches,
  home_wins, away_wins)

mart_goal_analytics(team, total_goals, penalty_goals, penalty_goal_pct,
  own_goals_against, first_half_goals, second_half_goals,
  stoppage_time_goals, unique_scorers)

mart_squad_profile(team_name, squad_size, avg_age, youngest_player_age,
  oldest_player_age, goalkeepers, defenders, midfielders, forwards,
  players_at_foreign_clubs, legionnaire_pct)
"""

_SYSTEM_PROMPT = f"""You are a Snowflake SQL generator. You only write
SELECT or WITH queries - never DROP, DELETE, UPDATE, INSERT, ALTER,
CREATE, GRANT, or TRUNCATE.

{_MART_SCHEMA_CONTEXT}

Rules:
- Use only the tables and columns listed above.
- Always fully qualify tables as SILVER.<table_name>.
- Return ONLY the SQL query, no explanation, no markdown code fences.
- If the question cannot be answered with the available tables/columns,
  return exactly: UNANSWERABLE
"""


def _get_connection():
    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        role=settings.snowflake_role,
    )


def generate_sql(question: str) -> str:
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
    )
    sql = response["message"]["content"].strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql


def answer_question(question: str, max_retries: int = 2) -> dict:
    last_error = None

    for attempt in range(max_retries + 1):
        sql = generate_sql(
            question if attempt == 0
            else f"{question}\n\nYour previous query failed with this error:\n{last_error}\nFix it."
        )

        if sql == "UNANSWERABLE":
            return {"error": "Question cannot be answered with available tables", "sql": None}

        try:
            validated_sql = validate_readonly_sql(sql)
        except UnsafeSQLError as e:
            logger.error("query_agent_blocked_unsafe_sql", question=question, sql=sql, reason=str(e))
            return {"error": f"Blocked unsafe SQL: {e}", "sql": sql}

        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("ALTER SESSION SET QUERY_TAG = 'agent=query_agent,op=nl_to_sql'")
            cur.execute(validated_sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            logger.info("query_agent_success", question=question, sql=validated_sql, rows=len(rows))
            return {"sql": validated_sql, "columns": columns, "rows": rows, "error": None}
        except Exception as e:
            last_error = str(e)
            logger.warning("query_agent_sql_failed", attempt=attempt, error=last_error, sql=sql)
            continue
        finally:
            conn.close()

    return {"error": f"Failed after {max_retries + 1} attempts. Last error: {last_error}", "sql": sql}
