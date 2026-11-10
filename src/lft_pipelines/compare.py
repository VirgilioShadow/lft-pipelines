"""Compare two archive prefixes by their manifests.

Ignores ``extracted_at`` and ``tool_version``. A difference, or a partition
that exists on only one side, is a failure. The CLI does not open a database.
"""

import argparse
import sys
from datetime import date

from lft_pipelines.registry import resolve_tables
from lft_pipelines.storage import open_store

_COMPARED = ("row_count", "min_timestamp", "max_timestamp", "columns")


def compare_manifests(left: dict | None, right: dict | None) -> list[str]:
    """Field names that differ. ``only-left`` or ``only-right`` when one side is missing."""

    if left is None and right is None:
        return []
    if left is None:
        return ["only-right"]
    if right is None:
        return ["only-left"]
    return [name for name in _COMPARED if left.get(name) != right.get(name)]


def compare_stores(store_a, store_b, tables, start: date, end: date) -> tuple[list[tuple[str, date, list[str]]], int]:
    """Differences for ``[start, end)``, plus a snapshot labeled ``end``.

    A partition is skipped, not failed, when either manifest has
    ``partial_by_retention`` set. The second value is that skip count.
    """

    rows = []
    skipped_partial = 0
    for table in tables:
        days = _days(start, end)
        if table.partition == "snapshot":
            days = (end,)
        for day in days:
            left = _one(store_a, table, day)
            right = _one(store_b, table, day)
            if _partial(left) or _partial(right):
                skipped_partial += 1
                continue
            diff = compare_manifests(left, right)
            if diff and not (left is None and right is None):
                rows.append((table.key, day, diff))
    return rows, skipped_partial


def format_compare(rows: list[tuple[str, date, list[str]]], skipped_partial: int = 0) -> str:
    """A text table of differences and a one-line summary."""

    lines = ["table\tdate\tdifferences"]
    for key, day, diff in rows:
        lines.append(f"{key}\t{day.isoformat()}\t{','.join(diff)}")
    lines.append(f"summary differences={len(rows)} skipped_partial={skipped_partial}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """CLI entry. Exit 1 when any partition differs or is one-sided."""

    parser = argparse.ArgumentParser(prog="python -m lft_pipelines.compare")
    parser.add_argument("--a", required=True, help="Left archive URL or directory")
    parser.add_argument("--b", required=True, help="Right archive URL or directory")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--tables", default="all")
    args = parser.parse_args(argv)
    try:
        tables = resolve_tables(args.tables)
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    rows, skipped_partial = compare_stores(open_store(args.a), open_store(args.b), tables, start, end)
    print(format_compare(rows, skipped_partial))
    return 1 if rows else 0


def _partial(payload: dict | None) -> bool:
    """True when this manifest is the retention-boundary extract."""

    return bool(payload and payload.get("partial_by_retention"))


def _days(start: date, end: date) -> tuple[date, ...]:
    from datetime import timedelta

    count = (end - start).days
    return tuple(start + timedelta(days=offset) for offset in range(count))


def _one(store, table, day: date) -> dict | None:
    from lft_pipelines.storage import manifest_rel

    return store.read_json(manifest_rel(table.schema, table.table, day))


if __name__ == "__main__":
    raise SystemExit(main())
