# Architecture Decision Records — de-ai-agent

Each entry: Context → Decision → Alternatives considered → Consequences.

---

## ADR-001: Staging models stay 1:1 with Bronze; joins live in mart models

**Context:** Staging models could have included join logic directly.

**Decision:** Staging (Bronze → Silver) is strictly column rename/type-cast/light
cleanup only — no joins, no business logic. All joins and aggregation live in
`models/marts/` (Silver → Gold), built via `{{ ref() }}` on staging models.

**Alternatives considered:**
- Joins in staging — rejected: couples foundational models to specific
  business questions, making every downstream consumer fragile to change.
- No mart layer, query staging directly — rejected: risks re-implementing
  the same join logic (e.g., home/away unioning) inconsistently across
  consumers.

**Consequences:** Staging stays stable and low-risk to depend on. New marts
can be added without touching staging. Matches dbt Labs' standard project
structure convention.

---

## ADR-002: LangGraph for orchestration, not Airflow or plain sequential scripts

**Context:** Needed to orchestrate 5 sequential steps (schema → DDL → ingest →
transform → QA) per source, with retry and error-routing behavior.

**Decision:** LangGraph state machine — deterministic routing logic
(`supervisor.py`) reads `state["status"]`/`state["error"]` after each node and
decides the next hop; retries live inside each agent via `tenacity`.

**Alternatives considered:**
- Airflow — rejected for this scope: heavier infra (scheduler, metadata DB)
  than justified for a single-machine portfolio project; better fit once
  Prefect/cron-based scheduling is added on top later.
- Plain sequential Python script with try/except — rejected: harder to
  reason about routing logic as complexity grows, no clean separation
  between "what a step does" and "what happens next."

**Consequences:** Graph topology is visible and auditable independent of
agent internals. Adding a new agent node or changing routing doesn't require
touching agent logic. Retry behavior is centralized in one pattern
(tenacity) rather than scattered per-agent try/except blocks.

---

## ADR-003: Bronze auto-created dynamically from inferred schema, not hand-written DDL

**Context:** Six heterogeneous sources (CSV, SQLite, API) each need a Bronze
table; hand-writing DDL per source doesn't scale and drifts from actual data.

**Decision:** `schema_agent` infers column types + nullability directly from
sampled real data; `ddl_agent` generates `CREATE TABLE IF NOT EXISTS` DDL
dynamically from that inferred schema — zero hardcoded columns anywhere.

**Alternatives considered:**
- Hand-written `setup.sql` per source — rejected: doesn't scale past a
  handful of sources, and drifts silently if source data shape changes.

**Consequences:** Adding a new source requires zero DDL work — just a
`SOURCE_REGISTRY` entry and a fetch function. Risk: schema inference can be
wrong on edge cases (mitigated by LLM review step with deterministic
fallback).

---

## ADR-004: Data-driven column profiling instead of hardcoded per-source test rules

**Context:** Initial validation logic rejected any row with a null value in
any column, regardless of whether that column is legitimately sparse in real
data (e.g., historical shootouts before "first shooter" was tracked) — this
produced false-positive data-quality failures.

**Decision:** A single-pass column profiler (`ingestion/schema_inferrer.py`)
computes null rate, uniqueness, and value-range facts directly from sampled
data. A separate rules layer converts those facts into dbt test suggestions
(`not_null`, `unique`, `dbt_expectations` range checks) — generic across any
source, not hardcoded per column.

**Alternatives considered:**
- Hardcode which columns are nullable per source (e.g., `first_shooter`) —
  rejected after implementing: doesn't generalize, requires manual
  intervention for every new source's edge cases.
- LLM decides nullability/tests per column — rejected for this piece:
  uniqueness/null-rate/range are provable statistical facts computable
  deterministically in milliseconds; reserving the LLM for semantic
  judgment calls (e.g., PII, business meaning) rather than facts.

**Consequences:** New sources automatically get correct, tailored data
quality tests without hand-tuning. Matches the industry pattern used by
tools like Great Expectations (profile → suggest rules), implemented at
small scale for this project.

---

## ADR-005: dbt_expectations package over hand-written singular tests

**Context:** Early non-negative-value checks were implemented as one-off
hand-written singular SQL test files per source (e.g.,
`assert_no_negative_scores.sql`) — doesn't scale to many sources/columns.

**Decision:** Adopted the `dbt_expectations` package (via `packages.yml`)
for range/distribution tests, generated automatically per-column by the
data-driven profiler (ADR-004).

**Alternatives considered:**
- Continue hand-writing singular tests per numeric column — rejected:
  linear growth in maintenance burden per source.
- Build a fully custom test-generation DSL — rejected as unnecessary
  reinvention; `dbt_expectations` is the maintained, widely-adopted
  industry package for this exact purpose.

**Consequences:** Uses the tool a real production team would actually reach
for, rather than a bespoke solution — a deliberate signal in interviews
that the tradeoff was considered, not defaulted to "build it myself."

---

## ADR-006: Real data only — no synthetic fallback data in the pipeline

**Context:** `national_teams` and `player_profiles` sources initially had no
real data source available; synthetic/generated placeholder data was
considered as a stopgap.

**Decision:** Rejected synthetic data. Sourced real WC 2026 team and full
squad roster data from `openfootball`'s public-domain API
(`worldcup.squads.json`) — same free, no-key-required source already used
for match results.

**Alternatives considered:**
- Synthetic/generated fake team and player data — rejected: undermines the
  project's credibility as a real data engineering demonstration; a
  reviewer who notices fabricated data would reasonably question the rest
  of the project's rigor.

**Consequences:** All 6 sources are backed by genuinely real, verifiable
data end to end. Cost: had to research and verify a real free data source
mid-build rather than generating placeholder data instantly — worth the
tradeoff for project credibility.

---

## Planned / not yet decided

The following are scoped for future sessions and don't yet have an ADR:
- BI / natural-language query agent (question → SQL → answer) over the
  mart layer
- Chart/report generation layer
- Prefect-based scheduling and retry policy
- Streamlit or FastAPI presentation layer (currently neither exists)
- Bronze retention policy, schema-change handling, Snowflake
  role-to-access documentation
