"""
agents/sql_guardrails.py

Blocks destructive SQL before any LLM-generated query is allowed to
execute. This is a deterministic, non-bypassable check — never relies
on the LLM "agreeing" not to generate dangerous SQL; enforces it in code.
"""

import re

_FORBIDDEN_KEYWORDS = [
    "DROP", "DELETE", "UPDATE", "ALTER", "TRUNCATE",
    "INSERT", "MERGE", "CREATE", "GRANT", "REVOKE",
]

_ALLOWED_START = re.compile(r"^\s*(WITH|SELECT)\b", re.IGNORECASE)


class UnsafeSQLError(Exception):
    """Raised when generated SQL fails the guardrail check."""
    pass


def validate_readonly_sql(sql: str) -> str:
    """
    Validates that a SQL string is read-only before allowing execution.
    Raises UnsafeSQLError if the query contains any destructive keyword
    or does not start with SELECT/WITH.

    Deliberately conservative — a real production system would also
    parse the SQL AST (e.g. via sqlglot) rather than keyword-match,
    since keyword matching can be evaded by clever string construction.
    Documented as a known limitation, not hidden.
    """
    if not sql or not sql.strip():
        raise UnsafeSQLError("Empty SQL")

    if not _ALLOWED_START.match(sql):
        raise UnsafeSQLError(
            f"Query must start with SELECT or WITH — got: {sql[:50]}"
        )

    upper_sql = sql.upper()
    for keyword in _FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", upper_sql):
            raise UnsafeSQLError(
                f"Forbidden keyword '{keyword}' found in generated SQL — blocked"
            )

    return sql
