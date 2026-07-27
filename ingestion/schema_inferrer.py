"""
ingestion/schema_inferrer.py
=============================
Core schema inference engine — reads actual data and derives Python types.
Zero hardcoded schemas. Works for any CSV, SQLite table, or flat JSON rows.
"""

from __future__ import annotations

import csv
import sqlite3
from typing import Any

_SQLITE_TYPE_MAP: dict[str, type] = {
    "TEXT": str, "VARCHAR": str, "CHAR": str, "CLOB": str, "STRING": str,
    "INTEGER": int, "INT": int, "TINYINT": int, "SMALLINT": int, "BIGINT": int,
    "NUMERIC": float, "REAL": float, "DOUBLE": float, "FLOAT": float, "DECIMAL": float,
    "BOOLEAN": bool, "BOOL": bool, "BLOB": str,
}


def _coerce(value: Any, py_type: type) -> Any:
    """
    Safely coerces value to py_type. Returns None on any failure.
    None return → row goes to dead letter in ingest_agent.
    """
    if value is None or value == "":
        return None
    try:
        if py_type == bool:
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("true", "1", "yes", "t", "y")
        if py_type == int:
            return int(float(str(value).strip()))
        if py_type == float:
            return float(str(value).strip())
        return str(value).strip()
    except (ValueError, TypeError):
        return None


def _looks_like_bool(values: list[str]) -> bool:
    bool_set = {"true", "false", "1", "0", "yes", "no", "t", "f", "y", "n"}
    return all(v.strip().lower() in bool_set for v in values if v.strip())


def _looks_like_int(values: list[str]) -> bool:
    for v in values:
        v = v.strip()
        if not v:
            continue
        try:
            f = float(v)
            if f != int(f):
                return False
        except (ValueError, TypeError):
            return False
    return True


def _looks_like_float(values: list[str]) -> bool:
    for v in values:
        v = v.strip()
        if not v:
            continue
        try:
            float(v)
        except (ValueError, TypeError):
            return False
    return True


def _infer_type_from_str_samples(values: list[str]) -> type:
    """Infers Python type from string samples. Precedence: bool > int > float > str."""
    non_empty = [v for v in values if v and v.strip()]
    if not non_empty:
        return str
    if _looks_like_bool(non_empty):
        return bool
    if _looks_like_int(non_empty):
        return int
    if _looks_like_float(non_empty):
        return float
    return str


def infer_from_csv(file_path: str, sample_rows: int = 200) -> dict[str, type]:
    """
    Infers column types from a CSV by sampling up to sample_rows rows.
    Returns {column_name: python_type} for every column.
    """
    samples: dict[str, list[str]] = {}
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no headers: {file_path}")
        for col in reader.fieldnames:
            samples[col] = []
        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for col in reader.fieldnames:
                samples[col].append(row.get(col, "") or "")
    return {col: _infer_type_from_str_samples(vals) for col, vals in samples.items()}


_NULL_RATE_THRESHOLD = 0.05
_CARDINALITY_CATEGORICAL_MAX = 0.05
_MIN_SAMPLES_FOR_UNIQUE = 10


def profile_columns(file_path, schema_map, sample_rows: int = 500) -> dict:
    """Single-pass, source-agnostic column profiler."""
    cols = list(schema_map.keys())
    non_empty = {c: [] for c in cols}
    counts = {c: 0 for c in cols}
    nulls = {c: 0 for c in cols}

    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for col in cols:
                counts[col] += 1
                val = (row.get(col) or "").strip()
                if not val:
                    nulls[col] += 1
                else:
                    non_empty[col].append(val)

    profile = {}
    for col in cols:
        total = counts[col]
        vals = non_empty[col]
        distinct_vals = set(vals)
        distinct = len(distinct_vals)
        null_rate = (nulls[col] / total) if total else 0.0
        ever_negative = None
        if schema_map[col] in (int, float) and vals:
            ever_negative = any(_is_negative_number(v) for v in vals)
        profile[col] = {
            "null_rate": null_rate,
            "distinct_count": distinct,
            "sample_count": len(vals),
            "cardinality_ratio": (distinct / len(vals)) if vals else 0.0,
            "is_unique": len(vals) >= _MIN_SAMPLES_FOR_UNIQUE and distinct == len(vals),
            "ever_negative": ever_negative,
            "distinct_values": sorted(distinct_vals) if distinct <= 20 else [],
        }
    return profile


def _is_negative_number(s: str) -> bool:
    try:
        return float(s) < 0
    except ValueError:
        return False


def suggest_tests_from_profile(profile: dict, schema_map: dict):
    """Rules layer: facts -> (suggested_tests, nonnegative_columns, categorical_columns)."""
    suggested_tests = {}
    nonnegative_columns = set()
    categorical_columns = {}

    for col, facts in profile.items():
        tests = []
        if facts["null_rate"] <= _NULL_RATE_THRESHOLD:
            tests.append("not_null")
        if facts["is_unique"]:
            tests.append("unique")

        if (
            schema_map.get(col) == str
            and not facts["is_unique"]
            and facts["sample_count"] >= 20
            and facts["cardinality_ratio"] <= _CARDINALITY_CATEGORICAL_MAX
            and facts["distinct_count"] >= 2
        ):
            categorical_columns[col.lower()] = facts["distinct_values"]

        if tests:
            suggested_tests[col.lower()] = tests

        if (
            schema_map.get(col) in (int, float)
            and facts["ever_negative"] is False
            and facts["sample_count"] > 0
        ):
            nonnegative_columns.add(col)

    return suggested_tests, nonnegative_columns, categorical_columns


def infer_nullable_columns_from_profile(profile: dict) -> set:
    return {c for c, f in profile.items() if f["null_rate"] > _NULL_RATE_THRESHOLD}


def infer_nullable_columns(file_path: str, sample_rows: int = 200) -> set:
    """Back-compat wrapper used by schema_agent.py."""
    schema_map = infer_from_csv(file_path, sample_rows)
    profile = profile_columns(file_path, schema_map, sample_rows)
    return infer_nullable_columns_from_profile(profile)


def infer_from_sqlite(file_path: str, table: str) -> dict[str, type]:
    """
    Infers column types from SQLite PRAGMA table_info().
    Returns {column_name: python_type} for every column.
    """
    conn = sqlite3.connect(file_path)
    try:
        cur = conn.execute(f"PRAGMA table_info({table})")
        rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        raise ValueError(f"Table '{table}' not found or empty in {file_path}")
    schema: dict[str, type] = {}
    for row in rows:
        col_name = row[1]
        declared = row[2].upper().split("(")[0].strip()
        schema[col_name] = _SQLITE_TYPE_MAP.get(declared, str)
    return schema


def infer_from_json_rows(rows: list[dict[str, Any]]) -> dict[str, type]:
    """
    Infers column types from a list of flat dicts (post-flatten JSON rows).
    Uses actual Python types — JSON values are already typed.
    Returns {column_name: python_type}.
    """
    if not rows:
        return {}
    sample = rows[:200]
    schema: dict[str, type] = {}
    for col in sample[0].keys():
        values = [row[col] for row in sample if row.get(col) is not None]
        if not values:
            schema[col] = str
        elif all(isinstance(v, bool) for v in values):
            schema[col] = bool
        elif all(isinstance(v, bool) or isinstance(v, int) for v in values):
            schema[col] = int
        elif all(isinstance(v, (int, float)) for v in values):
            schema[col] = float
        else:
            schema[col] = str
    return schema


def load_csv_rows(file_path: str, schema_map: dict[str, type]) -> list[dict[str, Any]]:
    """
    Reads full CSV, coerces every value to its inferred Python type.
    None = coercion failed → ingest_agent routes row to dead letter.
    """
    rows: list[dict[str, Any]] = []
    with open(file_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                col: _coerce(row.get(col), py_type)
                for col, py_type in schema_map.items()
            })
    return rows
