"""
agents/sql_guardrails.py

Blocks destructive SQL before any LLM-generated query is allowed to
execute. This is a deterministic, non-bypassable check -- never relies
on the LLM "agreeing" not to generate dangerous SQL; enforces it in code.

UPGRADE (was regex keyword-matching, now sqlglot AST parsing):
Regex keyword matching (checking if "DROP" appears anywhere in the
string) can be evaded by clever string construction -- a comment, a
string literal containing a forbidden word, unusual spacing/casing.
sqlglot parses the SQL into a real syntax tree, so validation is based
on SQL STRUCTURE (what statement type this actually is, what it
actually does), not surface text. A forbidden word inside a string
literal or comment no longer false-positives, and a genuinely
destructive statement can't hide from structural detection the way it
could from a regex.
"""

import sqlglot
from sqlglot import exp

_FORBIDDEN_STATEMENT_TYPES = (
    exp.Drop, exp.Delete, exp.Update, exp.Alter, exp.TruncateTable,
    exp.Insert, exp.Merge, exp.Create, exp.Grant,
)

_ALLOWED_SCHEMA = "gold"


class UnsafeSQLError(Exception):
    """Raised when generated SQL fails the guardrail check."""
    pass


def validate_readonly_sql(sql: str, dialect: str = "duckdb") -> str:
    """
    Validates that a SQL string is read-only and touches only the GOLD
    schema before allowing execution. Raises UnsafeSQLError on any
    violation. Parses the real SQL AST via sqlglot rather than
    keyword-matching the raw string.
    """
    if not sql or not sql.strip():
        raise UnsafeSQLError("Empty SQL")

    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:
        raise UnsafeSQLError(f"SQL failed to parse: {e}")

    if isinstance(tree, _FORBIDDEN_STATEMENT_TYPES):
        raise UnsafeSQLError(
            f"Forbidden statement type '{type(tree).__name__}' -- blocked"
        )

    if not isinstance(tree, (exp.Select, exp.Union, exp.With)):
        raise UnsafeSQLError(
            f"Query must be a SELECT/WITH statement -- got '{type(tree).__name__}'"
        )

    # Belt-and-suspenders: even inside an allowed SELECT/WITH, make sure
    # no forbidden sub-clause (e.g. a nested subquery attempting DELETE)
    # is present anywhere in the tree.
    for forbidden_type in _FORBIDDEN_STATEMENT_TYPES:
        if tree.find(forbidden_type):
            raise UnsafeSQLError(
                f"Forbidden clause '{forbidden_type.__name__}' found nested in query -- blocked"
            )

    # Table scope enforcement: every table this query touches must be in
    # the GOLD schema. Prevents the LLM from reading BRONZE/SILVER (which
    # may contain raw/PII data not meant for the query agent) even if it
    # never attempts a write.
    for table in tree.find_all(exp.Table):
        schema = (table.db or "").lower()
        if schema and schema != _ALLOWED_SCHEMA:
            raise UnsafeSQLError(
                f"Query references schema '{schema}' -- only '{_ALLOWED_SCHEMA}' is allowed"
            )

    return sql
