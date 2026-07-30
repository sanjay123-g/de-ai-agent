"""
agents/query_agent.py

Natural-language data analyst agent over the Gold-layer marts.
Routes each input into one of three modes:
  1. Metadata request (e.g. "what tables do you have?") -> deterministic
     answer from real schema, no LLM guessing.
  2. Conversational (greeting, unclear, "what can you ask?") -> plain
     English reply grounded in the real schema.
  3. Data question -> SQL generation (qwen2.5-coder:14b) -> guardrail
     validation -> Snowflake execution -> plain-English result summary.

Every generated SQL query passes through sql_guardrails.validate_readonly_sql()
before execution, regardless of mode.
"""

import ollama
import structlog
from config.settings import get_settings
from agents.sql_guardrails import validate_readonly_sql, UnsafeSQLError
import snowflake.connector

logger = structlog.get_logger()
settings = get_settings()

_MART_SCHEMA_CONTEXT = """
Available tables (Snowflake schema GOLD):

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
- If the question is genuinely ambiguous about WHICH metric to use
  (e.g. "best team" could mean most wins, most points, or best goal
  difference), pick the most natural default (points, or wins if no
  points-like column exists) rather than refusing.
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


def _get_categorical_context() -> str:
    """
    Dynamic value grounding: introspects Snowflake's information schema
    to find every string column across SILVER mart tables, computes
    cardinality live, and fetches real distinct values for columns that
    look categorical. No hardcoded table/column names.
    """
    conn = _get_connection()
    lines = []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name
            FROM DE_AI_AGENT_DEV.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'GOLD'
              AND table_name ILIKE 'MART_%%'
              AND data_type IN ('TEXT', 'VARCHAR', 'STRING')
        """)
        candidates = cur.fetchall()
        for table_name, column_name in candidates:
            cur.execute(
                f'SELECT COUNT(DISTINCT "{column_name}"), COUNT(*) FROM GOLD.{table_name}'
            )
            distinct_count, total = cur.fetchone()
            if not total:
                continue
            if distinct_count / total <= 0.10 and distinct_count <= 60:
                cur.execute(f'SELECT DISTINCT "{column_name}" FROM GOLD.{table_name} ORDER BY 1')
                values = [r[0] for r in cur.fetchall()]
                lines.append(f'Actual {column_name} values in GOLD.{table_name}: {values}')
        return "\n".join(lines) if lines else "No categorical columns detected."
    finally:
        conn.close()


def _get_metadata_answer() -> str:
    """
    Deterministic metadata listing -- no LLM call needed, since the real
    schema is already known in code. Used when the user asks what
    tables/columns/data are available.
    """
    return (
        "Here's what's available:\n\n"
        f"{_MART_SCHEMA_CONTEXT.strip()}\n\n"
        "Each table covers a different angle: team_performance for wins/"
        "points/goals, goal_analytics for scoring patterns, and "
        "squad_profile for roster composition."
    )


def _classify_intent(question: str) -> str:
    """
    Classifies the input into 'metadata', 'conversational', or 'data'.
    Deterministic keyword check for metadata (fast, no LLM needed);
    LLM classification for conversational vs data question.
    """
    lowered = question.lower()
    metadata_signals = ["what tables", "what data", "what columns", "list tables",
                         "what can you", "schema", "metadata", "what fields"]
    if any(sig in lowered for sig in metadata_signals):
        return "metadata"

    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": """Classify the user's message as
either DATA_QUESTION (a genuine question that requires querying data --
e.g. "which team has the most wins", "average squad age") or
CONVERSATIONAL (greetings, thanks, unclear requests, small talk).
Respond with exactly one word: DATA_QUESTION or CONVERSATIONAL."""},
            {"role": "user", "content": question},
        ],
    )
    reply = response["message"]["content"].strip().upper()
    return "data" if "DATA_QUESTION" in reply else "conversational"


def _conversational_reply(question: str) -> str:
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": f"""You are a friendly data
analyst assistant for a World Cup 2026 dataset.

{_MART_SCHEMA_CONTEXT}

Respond naturally and helpfully in 2-3 sentences, grounded in the real
tables/columns above. If asked what you can do, give concrete example
questions."""},
            {"role": "user", "content": question},
        ],
    )
    return response["message"]["content"].strip()


def generate_sql(question: str, categorical_context: str) -> str:
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT + "\n\n" + categorical_context},
            {"role": "user", "content": question},
        ],
    )
    sql = response["message"]["content"].strip()
    return sql.replace("```sql", "").replace("```", "").strip()


def _summarize_result(question: str, columns: list, rows: list) -> str:
    """Turns raw query results into a one-sentence plain-English answer."""
    if not rows:
        return "The query ran successfully but returned no matching rows."
    preview = str(rows[:5])
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": """Summarize this query result in
ONE plain-English sentence, as a data analyst reporting a finding.
No SQL, no column names verbatim, no markdown."""},
            {"role": "user", "content": f"Question: {question}\nColumns: {columns}\nResults: {preview}"},
        ],
    )
    return response["message"]["content"].strip()


def answer_question(question: str, max_retries: int = 2, history: list | None = None) -> dict:
    intent = _classify_intent(question)

    if intent == "metadata":
        return {"error": None, "sql": None, "summary": _get_metadata_answer(),
                "columns": None, "rows": None}

    if intent == "conversational":
        return {"error": None, "sql": None, "summary": _conversational_reply(question),
                "columns": None, "rows": None}

    categorical_context = _get_categorical_context()
    last_error = None

    history_context = ""
    if history:
        recent = history[-3:]
        history_context = "\n\nRecent conversation (resolve pronouns/references like same/that/those teams against this):\n"
        lines = []
        for h in recent:
            q = h.get("question", "")
            a = h.get("summary", "")
            lines.append("Q: " + q + "\nA: " + a)
        history_context += "\n".join(lines)

    for attempt in range(max_retries + 1):
        base_q = question + history_context
        sql = generate_sql(
            base_q if attempt == 0
            else f"{base_q}\n\nYour previous query failed with this error:\n{last_error}\nFix it.",
            categorical_context,
        )

        if sql == "UNANSWERABLE":
            return {
                "error": "This question can't be answered with the available data "
                         "(team performance, goal analytics, or squad composition "
                         "for WC 2026). Try asking about wins, goals, or squad stats instead.",
                "sql": None, "summary": None, "columns": None, "rows": None,
            }

        try:
            validated_sql = validate_readonly_sql(sql)
        except UnsafeSQLError as e:
            logger.error("query_agent_blocked_unsafe_sql", question=question, sql=sql, reason=str(e))
            return {"error": f"Blocked unsafe SQL: {e}", "sql": sql, "summary": None,
                    "columns": None, "rows": None}

        conn = _get_connection()
        try:
            cur = conn.cursor()
            cur.execute("ALTER SESSION SET QUERY_TAG = 'agent=query_agent,op=nl_to_sql'")
            cur.execute(validated_sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            summary = _summarize_result(question, columns, rows)
            logger.info("query_agent_success", question=question, sql=validated_sql, rows=len(rows))
            return {"sql": validated_sql, "columns": columns, "rows": rows,
                    "summary": summary, "error": None}
        except Exception as e:
            last_error = str(e)
            logger.warning("query_agent_sql_failed", attempt=attempt, error=last_error, sql=sql)
            continue
        finally:
            conn.close()

    return {"error": f"Failed after {max_retries + 1} attempts. Last error: {last_error}",
            "sql": sql, "summary": None, "columns": None, "rows": None}
