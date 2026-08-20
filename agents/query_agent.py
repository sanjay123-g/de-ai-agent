"""
agents/query_agent.py

Natural-language data analyst agent over the Gold-layer marts.
DuckDB + RAG schema retrieval via ChromaDB.
"""

import chromadb
import duckdb
import ollama
import structlog
from config.settings import get_settings
from agents.sql_guardrails import validate_readonly_sql, UnsafeSQLError

logger = structlog.get_logger()
settings = get_settings()

_GOLD_TABLES = {
    "mart_team_performance": {
        "columns": "team, tournament, matches_played, wins, draws, losses, "
                   "win_pct, points, goals_for, goals_against, goal_difference, "
                   "avg_goals_scored_per_match, avg_goals_conceded_per_match, "
                   "clean_sheets, failed_to_score, biggest_win_margin, "
                   "home_matches, away_matches, home_wins, away_wins",
        "description": "Team-level match results and standings: wins, losses, "
                        "draws, points, goals scored/conceded, home vs away "
                        "record, per tournament. Use for questions about "
                        "team rankings, records, points, win rate, goal "
                        "difference, clean sheets, or home/away performance.",
    },
    "mart_goal_analytics": {
        "columns": "team, total_goals, penalty_goals, penalty_goal_pct, "
                   "own_goals_against, first_half_goals, second_half_goals, "
                   "stoppage_time_goals, unique_scorers",
        "description": "Goal-scoring patterns per team: penalties, own goals, "
                        "first-half vs second-half goals, stoppage-time goals, "
                        "number of different scorers. Use for questions about "
                        "how or when a team scores, penalty reliance, or "
                        "scoring variety.",
    },
    "mart_squad_profile": {
        "columns": "team_name, squad_size, avg_age, youngest_player_age, "
                   "oldest_player_age, goalkeepers, defenders, midfielders, "
                   "forwards, players_at_foreign_clubs, legionnaire_pct",
        "description": "Squad composition per national team: age profile, "
                        "position breakdown (goalkeepers/defenders/midfielders/"
                        "forwards), and how many players are based abroad. "
                        "Use for questions about squad age, roster makeup, or "
                        "players playing outside their home country.",
    },
}


def _format_table_context(table_name):
    info = _GOLD_TABLES[table_name]
    return f"{table_name}({info['columns']})\n  -- {info['description']}"


_MART_SCHEMA_CONTEXT = "Available tables (DuckDB schema GOLD):\n\n" + "\n\n".join(
    _format_table_context(t) for t in _GOLD_TABLES
)

_CHROMA_PATH = "./chroma_db"
_COLLECTION_NAME = "gold_schema"

_chroma_client = chromadb.PersistentClient(path=_CHROMA_PATH)


def _get_or_build_collection():
    collection = _chroma_client.get_or_create_collection(name=_COLLECTION_NAME)

    if collection.count() == len(_GOLD_TABLES):
        return collection

    existing_ids = collection.get()["ids"]
    if existing_ids:
        collection.delete(ids=existing_ids)

    collection.add(
        ids=list(_GOLD_TABLES.keys()),
        documents=[
            f"{info['description']} Columns: {info['columns']}"
            for info in _GOLD_TABLES.values()
        ],
        metadatas=[{"table_name": t} for t in _GOLD_TABLES],
    )
    logger.info("rag_schema_collection_built", tables=list(_GOLD_TABLES.keys()))
    return collection


def _retrieve_relevant_tables(question, top_k=2):
    collection = _get_or_build_collection()
    result = collection.query(query_texts=[question], n_results=top_k)
    retrieved = result["ids"][0]
    logger.info("rag_schema_retrieved", question=question, tables=retrieved)
    return retrieved


def _build_retrieved_context(table_names):
    return "Available tables (DuckDB schema GOLD):\n\n" + "\n\n".join(
        _format_table_context(t) for t in table_names
    )


_unstructured_collection_cache = None

def _get_unstructured_collection():
    global _unstructured_collection_cache
    if _unstructured_collection_cache is None:
        _unstructured_collection_cache = _chroma_client.get_or_create_collection(name="unstructured_content")
    return _unstructured_collection_cache


def _classify_structured_or_unstructured(question):
    """
    Router: classifies a DATA_QUESTION as needing SQL (numbers, stats,
    counts, rankings from the Gold marts) or narrative/background text
    (team history, description) from the unstructured Wikipedia content.
    Separate from _classify_intent, which handles metadata/conversational/
    data at a higher level -- this only runs once a question is already
    confirmed to be a real data question.
    """
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": """Classify this question as either:
STRUCTURED -- needs numeric/statistical data (wins, goals, ages, counts,
rankings, percentages) that would come from a database table.
UNSTRUCTURED -- needs narrative/background information (team history,
description, general facts) that would come from an encyclopedia article,
not a numeric query.
Respond with exactly one word: STRUCTURED or UNSTRUCTURED."""},
            {"role": "user", "content": question},
        ],
    )
    reply = response["message"]["content"].strip().upper()
    return "unstructured" if "UNSTRUCTURED" in reply else "structured"


def _answer_unstructured(question):
    """
    Retrieves the most relevant unstructured content (Wikipedia team
    summaries) for this question and answers directly from it -- no SQL,
    no Gold tables involved. Separate answer path from the structured
    SQL flow.
    """
    collection = _get_unstructured_collection()
    result = collection.query(query_texts=[question], n_results=2)
    retrieved_docs = result["documents"][0]
    retrieved_teams = [m["team"] for m in result["metadatas"][0]]

    if not retrieved_docs:
        return {
            "error": "No relevant background information found for this question.",
            "sql": None, "summary": None, "columns": None, "rows": None,
        }

    context = "\n\n".join(retrieved_docs)
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": f"""Answer the user's question using
ONLY the background information below. If the information doesn't contain
the answer, say so honestly rather than guessing.

Background information:
{context}"""},
            {"role": "user", "content": question},
        ],
    )
    answer = response["message"]["content"].strip()
    logger.info("unstructured_answer", question=question, retrieved_teams=retrieved_teams)
    return {
        "sql": None, "columns": None, "rows": None,
        "summary": answer, "error": None,
    }


_SYSTEM_PROMPT_TEMPLATE = """You are a DuckDB SQL generator. You only write
SELECT or WITH queries - never DROP, DELETE, UPDATE, INSERT, ALTER,
CREATE, GRANT, or TRUNCATE.

{schema_context}

Rules:
- Use only the tables and columns listed above.
- Always fully qualify tables as GOLD.<table_name>.
- Return ONLY the SQL query, no explanation, no markdown code fences.
- If the question cannot be answered with the available tables/columns,
  return exactly: UNANSWERABLE
- If the question is genuinely ambiguous about WHICH metric to use
  (e.g. "best team" could mean most wins, most points, or best goal
  difference), pick the most natural default (points, or wins if no
  points-like column exists) rather than refusing.
- If a team appears in multiple rows (e.g. multiple tournaments) and the
  question asks for a total/overall figure ("how many wins does X have",
  not "how many wins does X have in the world cup"), aggregate with
  SUM()/COUNT() in the SQL itself. Never return multiple raw rows for a
  question that implies one combined total -- the database must do the
  math, not the summary step.

Examples:
Q: which team has scored the most penalties
A: SELECT team FROM GOLD.mart_goal_analytics ORDER BY penalty_goals DESC LIMIT 1

Q: how many wins does germany have
A: SELECT wins FROM GOLD.mart_team_performance WHERE team = 'germany'

(Note: text values like team names are stored lowercase -- always match
the ACTUAL VALUES list below, never assume capitalization.)
"""


def _get_connection():
    return duckdb.connect(settings.duckdb_path)


def _get_categorical_context(table_names, question=""):
    """
    Dynamic value grounding, in two tiers:
      - LOW cardinality columns (<=60 distinct values, e.g. tournament
        names): dump the full real list -- small enough to be cheap, and
        having every value visible helps the LLM regardless of what the
        question mentions.
      - HIGH cardinality columns (e.g. team, 336 distinct values): dumping
        all of them would bloat every single prompt. Instead, only fetch
        values that fuzzy-match words actually present in the question
        (e.g. question mentions "germany" -> look up team values
        containing "german"). This is targeted retrieval, not a full dump
        -- the same RAG principle as schema retrieval, applied to values.
    """
    conn = _get_connection()
    lines = []
    try:
        placeholders = ", ".join(f"'{t}'" for t in table_names)
        candidates = conn.execute(f"""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'GOLD'
              AND table_name IN ({placeholders})
              AND data_type IN ('VARCHAR')
        """).fetchall()

        question_words = [
            w.strip(".,?!'\"").lower() for w in question.split() if len(w.strip(".,?!'\"")) >= 4
        ]

        for table_name, column_name in candidates:
            distinct_count, total = conn.execute(
                f'SELECT COUNT(DISTINCT "{column_name}"), COUNT(*) FROM GOLD.{table_name}'
            ).fetchone()
            if not total:
                continue

            if distinct_count <= 60:
                # Low cardinality -- full dump is cheap and always useful.
                values = [
                    r[0] for r in conn.execute(
                        f'SELECT DISTINCT "{column_name}" FROM GOLD.{table_name} ORDER BY 1'
                    ).fetchall()
                ]
                lines.append(f'Actual {column_name} values in GOLD.{table_name}: {values}')
            elif question_words:
                # High cardinality -- only fetch values matching words
                # actually in this question, instead of dumping everything.
                matched = set()
                for word in question_words:
                    rows = conn.execute(
                        f'SELECT DISTINCT "{column_name}" FROM GOLD.{table_name} '
                        f'WHERE LOWER("{column_name}") LIKE ?',
                        (f"%{word}%",),
                    ).fetchall()
                    matched.update(r[0] for r in rows)
                if matched:
                    lines.append(
                        f'{column_name} values in GOLD.{table_name} matching this question: '
                        f'{sorted(matched)}'
                    )

        return "\n".join(lines) if lines else "No categorical columns detected."
    finally:
        conn.close()


def _get_metadata_answer():
    return (
        "Here's what's available:\n\n"
        f"{_MART_SCHEMA_CONTEXT.strip()}\n\n"
        "Each table covers a different angle: team_performance for wins/"
        "points/goals, goal_analytics for scoring patterns, and "
        "squad_profile for roster composition."
    )


def _classify_intent(question):
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


def _conversational_reply(question):
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


def generate_sql(question, schema_context, categorical_context):
    system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(schema_context=schema_context)
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": system_prompt + "\n\n" + categorical_context},
            {"role": "user", "content": question},
        ],
    )
    sql = response["message"]["content"].strip()
    return sql.replace("```sql", "").replace("```", "").strip()


def _summarize_result(question, columns, rows):
    if not rows:
        return "The query ran successfully but returned no matching rows."
    preview = str(rows[:5])
    response = ollama.chat(
        model="qwen2.5-coder:14b",
        messages=[
            {"role": "system", "content": """Summarize this query result in
ONE plain-English sentence, as a data analyst reporting a finding.
No SQL, no column names verbatim, no markdown.
CRITICAL: Report ONLY the numbers actually present in the results.
NEVER sum, average, or otherwise calculate a new number yourself --
if multiple rows are shown, describe them as multiple rows/breakdown,
do not silently add them into one total."""},
            {"role": "user", "content": f"Question: {question}\nColumns: {columns}\nResults: {preview}"},
        ],
    )
    return response["message"]["content"].strip()


def answer_question(question, max_retries=2, history=None):
    intent = _classify_intent(question)

    if intent == "metadata":
        return {"error": None, "sql": None, "summary": _get_metadata_answer(),
                "columns": None, "rows": None}

    if intent == "conversational":
        return {"error": None, "sql": None, "summary": _conversational_reply(question),
                "columns": None, "rows": None}

    # Router: within a real data question, decide structured (SQL) vs
    # unstructured (narrative background) before doing any retrieval work
    # for the wrong path.
    data_type = _classify_structured_or_unstructured(question)
    if data_type == "unstructured":
        return _answer_unstructured(question)

    retrieved_tables = _retrieve_relevant_tables(question)
    schema_context = _build_retrieved_context(retrieved_tables)
    categorical_context = _get_categorical_context(retrieved_tables, question)
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
            schema_context,
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
            result = conn.execute(validated_sql)
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            summary = _summarize_result(question, columns, rows)
            logger.info("query_agent_success", question=question, sql=validated_sql,
                        rows=len(rows), retrieved_tables=retrieved_tables)
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
