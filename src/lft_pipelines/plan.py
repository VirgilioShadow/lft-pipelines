"""The list of partitions one invocation will visit.

``--end`` is exclusive for day partitions. A snapshot is written once, labeled
with ``--end``, which is the run's as-of date.
"""

from dataclasses import dataclass
from datetime import date, timedelta

from lft_pipelines.registry import TableSpec


@dataclass(frozen=True)
class WorkItem:
    table: TableSpec
    day: date
    status: str
    partial_by_retention: bool


def iter_days(start: date, end: date) -> tuple[date, ...]:
    if end <= start:
        raise ValueError("--end must be after --start")
    count = (end - start).days
    return tuple(start + timedelta(days=offset) for offset in range(count))


def build_plan(
    tables: tuple[TableSpec, ...],
    start: date,
    end: date,
    *,
    at_risk: bool,
    today: date,
) -> tuple[WorkItem, ...]:
    """Build the partition list.

    Without ``at_risk``, tables stay in registry order and each day table
    visits ``[start, end)``. With ``at_risk``, a pruned table drops days that
    retention has already deleted (``day < today - retention_days``), and the
    remaining work is ordered by the shortest retention window, then by that
    table, then by the oldest day. Unpruned tables and snapshots follow.

    A day with ``day >= today - settle_days`` is kept in the plan as
    ``not_settled`` and is not queried. The retention-boundary day
    (``day == today - retention_days``) is still extracted and marked partial.
    """

    days = iter_days(start, end)
    items: list[WorkItem] = []
    for table in tables:
        if table.partition == "snapshot":
            items.append(WorkItem(table, end, "extract", False))
            continue
        chosen = days
        if at_risk and table.retention_days is not None:
            oldest = today - timedelta(days=table.retention_days)
            chosen = tuple(day for day in days if day >= oldest)
        unsettled_on = today - timedelta(days=table.settle_days)
        for day in chosen:
            if day >= unsettled_on:
                items.append(WorkItem(table, day, "not_settled", False))
                continue
            boundary = today - timedelta(days=table.retention_days) if table.retention_days is not None else None
            partial = boundary is not None and day == boundary
            items.append(WorkItem(table, day, "extract", partial))

    if not at_risk:
        return tuple(items)

    order = {table.key: position for position, table in enumerate(tables)}

    def sort_key(item: WorkItem) -> tuple:
        retention = item.table.retention_days
        retention_rank = retention if retention is not None else 10**9
        snapshot_rank = 1 if item.table.partition == "snapshot" else 0
        return (retention_rank, snapshot_rank, order[item.table.key], item.day)

    return tuple(sorted(items, key=sort_key))


def format_plan(out: str, items: tuple[WorkItem, ...]) -> str:
    """Text of a dry-run. No connection and no filesystem."""

    root = out.rstrip("/")
    extract_count = sum(1 for item in items if item.status == "extract")
    unsettled_count = sum(1 for item in items if item.status == "not_settled")
    lines = [f"partitions={extract_count} dry-run not_settled={unsettled_count}"]
    for item in items:
        retention = "none" if item.table.retention_days is None else f"{item.table.retention_days}d"
        column = item.table.column or "snapshot"
        partial = "true" if item.partial_by_retention else "false"
        state = "not settled" if item.status == "not_settled" else "extract"
        rel = f"{item.table.schema}/{item.table.table}/date={item.day.isoformat()}"
        lines.append(
            f"{item.table.key}\tretention={retention}\tpartition={item.table.partition}"
            f"\tcolumn={column}\tdate={item.day.isoformat()}\t{state}"
            f"\tpartial_by_retention={partial}\tscan={item.table.scan_mode}"
            f"\t{root}/{rel}/part-0.parquet"
        )
    return "\n".join(lines)
