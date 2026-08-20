# Runbook

Common operational tasks for de-ai-agent.

## Restart a failed pipeline run

Failures route to `failure_node` and log the error via structlog. To retry a single source after fixing the underlying issue:

```bash
python -m agents.supervisor --source <source_name>
```

Available source names: `historical_results`, `historical_goals`, `historical_shootouts`, `worldcup_api`, `national_teams`, `player_profiles`.

## Replay dead-letter records

Rejected rows are written to `BRONZE.DEAD_LETTER` with a rejection reason. Inspect them directly:

```bash
python3 -c "
import duckdb
from config.settings import settings
conn = duckdb.connect(settings.duckdb_path)
print(conn.execute('SELECT * FROM BRONZE.DEAD_LETTER ORDER BY _ingested_at DESC LIMIT 20').fetchall())
"
```

There's no automated replay yet — fix the root cause (usually a schema/type mismatch) and re-run the source; `ingest_agent.py` truncates and reloads full-refresh sources each run.

## Roll back a bad dbt model

```bash
cd dbt_project
git log --oneline -- models/staging/stg_<source>.sql   # find the last known-good commit
git checkout <commit_hash> -- models/staging/stg_<source>.sql
dbt run --select stg_<source>
```

## Add a new data source

1. Add an entry to `SOURCE_REGISTRY` in `agents/supervisor.py` (source_name, source_type, file_path/api_url, sqlite_table if applicable).
2. Run `python -m agents.supervisor --source <new_source_name>` — `schema_agent`, `ddl_agent`, `ingest_agent`, `transform_agent`, and `qa_agent` handle the rest automatically; no other code changes needed for a standard CSV/SQLite/API source.
3. If the source is nested JSON, add a flattener function in `agents/schema_agent.py`'s `flatten_nested_json` pattern and dispatch to it by `source_name`.

## Query agent won't answer a question it should be able to

Check which path it routed to:

```bash
python3 -c "
from agents.query_agent import _classify_structured_or_unstructured
print(_classify_structured_or_unstructured('<the question>'))
"
```

If it's structured but returning wrong data, check `_retrieve_relevant_tables` picked the right Gold table(s) — if not, the table's description in `_GOLD_TABLES` (in `query_agent.py`) may need to be more specific.

If it's unstructured and refusing to answer, the retrieved Wikipedia content likely doesn't cover that team/topic — 18 of 48 teams don't yet have content ingested (title-mapping gaps); see `docs/adr/DECISIONS.md`.

## Rebuild the RAG embeddings from scratch

```bash
rm -rf chroma_db/
python3 -c "from agents.query_agent import _get_or_build_collection; _get_or_build_collection()"
```

This re-embeds Gold schema descriptions. Unstructured Wikipedia content needs its own re-ingestion script run (see `docs/adr/DECISIONS.md` for the ingestion approach used).
