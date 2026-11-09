"""SELECT builders and the guard that refuses anything else.

Session settings (read-only transaction, statement timeout, application name)
are libpq startup parameters in ``connect``. They are not SQL text, so the
only statements this package sends through a cursor are the ones built here.
"""

import re
from datetime import date

from lft_pipelines.bounds import predicate_bounds
from lft_pipelines.registry import TableSpec

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_SELECT = re.compile(r"(?is)^select\b")
# Words that would make a statement more than a read, including inside a CTE.
_FORBIDDEN = re.compile(
    r"(?is)\b(insert|update|delete|drop|alter|truncate|grant|revoke|copy|create|call|do|merge|comment)\b"
)


def quote_ident(name: str) -> str:
    """Quote one registry identifier. Mixed-case Postgres names require quotes."""

    if not _IDENT.fullmatch(name):
        raise ValueError(f"refusing identifier {name!r}")
    return f'"{name}"'


def ensure_select(statement: str) -> str:
    """Return ``statement`` when it is a single SELECT. Raise otherwise."""

    text = statement.strip()
    if text.endswith(";"):
        text = text[:-1].rstrip()
    if not text or ";" in text:
        raise ValueError("only a single SELECT statement is allowed")
    if not _SELECT.match(text):
        raise ValueError("only SELECT statements are allowed")
    if _FORBIDDEN.search(text):
        raise ValueError("only SELECT statements are allowed")
    return text


def _relation(table: TableSpec) -> str:
    return f"{quote_ident(table.schema)}.{quote_ident(table.table)}"


def queries_for(table: TableSpec, day: date) -> tuple[tuple[str, tuple], tuple[str, tuple]]:
    """Return ``(select, count)`` as ``(sql, params)`` pairs for one partition.

    Snapshot tables ignore ``day`` for the predicate and read the whole
    relation. The caller still labels the file with ``day``.
    """

    relation = _relation(table)
    if table.partition == "snapshot":
        select_sql = ensure_select(f"SELECT * FROM {relation}")
        count_sql = ensure_select(f"SELECT count(*) FROM {relation}")
        return (select_sql, ()), (count_sql, ())

    if table.column is None or table.column_kind is None:
        raise ValueError(f"{table.key} has no partition column")
    column = quote_ident(table.column)
    where = f"WHERE {column} >= %s AND {column} < %s"
    params = predicate_bounds(table.column_kind, day)
    select_sql = ensure_select(f"SELECT * FROM {relation} {where}")
    count_sql = ensure_select(f"SELECT count(*) FROM {relation} {where}")
    return (select_sql, params), (count_sql, params)


def range_queries(table: TableSpec) -> tuple[str, str]:
    """One data SELECT and one GROUP BY count for an unindexed day column.

    The data SELECT filters with a bare half-open range so it can become an
    index range later. NULL partition values are included in that one scan
    and are not written. The Chicago calendar date is computed only in the
    count query, which runs once for the whole span.
    """

    if table.column is None or table.column_kind != "timestamptz":
        raise ValueError(f"{table.key} is not a timestamp range scan")
    relation = _relation(table)
    column = quote_ident(table.column)
    select_sql = ensure_select(
        f"SELECT * FROM {relation} "
        f"WHERE {column} IS NULL OR ({column} >= %s AND {column} < %s)"
    )
    count_sql = ensure_select(
        f"SELECT ({column} AT TIME ZONE 'America/Chicago')::date AS chicago_day, count(*) "
        f"FROM {relation} WHERE {column} >= %s AND {column} < %s GROUP BY 1"
    )
    return select_sql, count_sql
