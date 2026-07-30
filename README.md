# de-ai-agent

Multi-agent data engineering pipeline that ingests FIFA World Cup 2026 data from 6 heterogeneous sources into Snowflake, transforms it with dbt into Gold-layer analytics marts, and exposes it through a local-LLM natural-language query agent — orchestrated by a LangGraph state machine.

![Architecture](docs/architecture.svg)

## What this demonstrates

- **Multi-agent orchestration** (LangGraph): 5 agents run as a deterministic state graph per source, with retry logic and error routing.
- **Heterogeneous source ingestion**: CSV, SQLite, and a live REST API — three real-world ingestion patterns.
- **Data-driven quality profiling**: a single-pass column profiler computes null rate, uniqueness, and value-range statistics from real sampled data and auto-generates dbt tests.
- **Gold-layer analytics marts**: real joins/aggregations (team performance, goal analytics, squad profile) — not just renamed tables.
- **AI query layer**: natural-language question → SQL → guardrail validation → Snowflake execution → plain-English answer, using a local `qwen2.5-coder:14b` model. Includes conversational routing, session memory, and an evaluation suite.
- **Governance**: automatic PII detection, Snowflake query tagging, dead-letter routing, SLO breach detection, deterministic SQL guardrails blocking destructive queries.
- **Real data only**: all 6 sources use real, verifiable data sourced from [openfootball](https://github.com/openfootball/worldcup.json) (public domain) — synthetic data was explicitly rejected during development.
- **Deployed as a service**: FastAPI `/v1/nl-to-sql` endpoint, containerized with Docker, with a Streamlit front end.

## Sources

| Source | Type | Rows | Notes |
|---|---|---|---|
| `historical_results` | CSV | 49,481 | International match results, 1872–present |
| `historical_goals` | CSV | 47,575 | Goalscorers linked to results |
| `historical_shootouts` | CSV | 680 | Penalty shootout outcomes |
| `worldcup_api` | REST API | 412 | Live WC 2026 fixtures/scores, 2 tables |
| `national_teams` | SQLite | 48 | Real WC 2026 qualified teams + groups |
| `player_profiles` | SQLite | 1,248 | Real WC 2026 full squad rosters |

## Gold-layer marts

- `mart_team_performance` — points, W/D/L, clean sheets, home/away splits
- `mart_goal_analytics` — goal-minute distribution, penalty share, own goals
- `mart_squad_profile` — squad age, position mix, legionnaire %

## Stack

- **Orchestration**: LangGraph (pipeline) + Prefect (flow written, not yet deployed to a live server — see `docs/adr/DECISIONS.md`)
- **Warehouse**: Snowflake — Bronze / Silver / Gold layering
- **Transformation**: dbt + `dbt_expectations`, `dbt_date` packages
- **Ingestion**: `snowflake-connector-python`, `httpx`, `sqlite3`
- **LLM**: local Ollama, `qwen2.5-coder:14b` — code-specialized model for SQL generation
- **Serving**: FastAPI + Streamlit, Docker containerized

## Running it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in Snowflake credentials

python seed_fifa_db.py                                    # seed local SQLite sources
python -m agents.supervisor --source historical_results   # run one source
python -m agents.supervisor                                # run all 6 sources

# Query layer
uvicorn api:app --reload          # FastAPI on :8000
streamlit run streamlit_app.py    # UI on :8501
```

## Documentation

- Architecture decisions: `docs/adr/DECISIONS.md`
- Governance (retention, PII, access, human-in-the-loop): `docs/governance/GOVERNANCE.md`
- Runbook (common operational tasks): `docs/RUNBOOK.md`
- Eval suite: `evals/`

## Status

All 6 sources ingest, transform, and pass dbt tests end-to-end against Snowflake. Gold-layer marts built with real joins/aggregations. Query agent, guardrails, eval suite, FastAPI, Docker, and Streamlit are working end-to-end. Still in progress: Prefect deployment to a live server, observability/tracing (LangSmith/Phoenix), automated schema-diff detection.
