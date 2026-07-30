# Data Governance — de-ai-agent

## Bronze retention policy
- Bronze (raw) tables: 90-day retention. Raw ingested data is a
  reproducible snapshot of the source (CSV/API/SQLite) — not the
  system of record. Since sources are re-fetchable, 90 days balances
  debugging/replay capability against storage cost.
- Silver/Gold: retained indefinitely. These represent modeled,
  business-usable state and are cheaper to keep than to fully
  reconstruct from Bronze if a downstream consumer depends on history.

## Schema-change handling
- Bronze tables are created via `CREATE TABLE IF NOT EXISTS`, generated
  dynamically from `schema_agent`'s inferred schema each run.
- Current behavior: if a source's column set changes, ddl_agent does
  NOT alter the existing table — new columns would be silently dropped
  from the insert, not added to the table.
- Required fix (not yet implemented): before ingest, diff the newly
  inferred schema against the existing table's actual columns; if they
  differ, log the diff and halt (route to failure_node) rather than
  silently inserting a partial row set. Schema changes should require
  explicit human review, never silent auto-alter.

## Snowflake role-to-access map

| Role | Used by | Permissions |
|---|---|---|
| `DE_INGEST_ROLE` | ingest_agent | INSERT/SELECT on BRONZE schema only |
| `DE_TRANSFORM_ROLE` | qa_agent, dbt run/test | CREATE SCHEMA, CREATE VIEW on SILVER; SELECT on BRONZE |
| `SYSADMIN` | one-time manual grants only | Used only to grant the above roles their permissions; never used by any agent at runtime |

Least-privilege principle: no agent role has DROP, DELETE, or
cross-schema write access. `query_agent`'s generated SQL is additionally
constrained in code (see `agents/sql_guardrails.py`) to SELECT/WITH only,
regardless of what role executes it.

## PII handling
- Columns matching whole-word keywords (name, scorer, player, person,
  email, phone, address) are automatically tagged `meta: {pii: true}`
  in dbt schema.yml during model generation.
- Known limitation: keyword-based detection can miss PII that doesn't
  match these terms, and can be evaded by unusual column naming. A
  production system would supplement this with value-level scanning
  (e.g. detecting name-shaped strings), not rely on column names alone.

## Human-in-the-loop boundaries
The following actions are never auto-executed, regardless of what any
agent (including the LLM-driven query_agent) proposes:
- Any DDL/DML beyond SELECT (enforced in code via sql_guardrails.py,
  not just prompt instruction)
- Role or permission grants (SYSADMIN actions are manual, one-time,
  outside the agent pipeline entirely)
- Schema changes to existing Bronze/Silver tables (see schema-change
  handling above — halts and alerts rather than silently altering)
- Dead-letter data is never auto-corrected or auto-reprocessed; it
  requires manual review of `BRONZE.DEAD_LETTER` before any reload

## Known gaps (not yet implemented)
- Automated schema-diff detection (currently would fail silently on
  column changes, not caught by any test)
- Row/column-level access control for a future multi-tenant scenario
- Formal data classification beyond PII tagging
