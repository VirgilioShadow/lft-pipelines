"""Run one table's at-risk plan.

The CLI calls ``run_table``. ``run_plan`` calls ``run_groups``, which uses the
same executor on a caller-built plan so that plan is not rebuilt. A skipped
day is not printed on its own; the invocation prints one total at the end.
"""

import time
from datetime import date, datetime

from lft_pipelines.plan import build_plan
from lft_pipelines.progress import Heartbeat, plan_line, result_line
from lft_pipelines.registry import TableSpec
from lft_pipelines.storage import manifest_rel

BATCH_ROWS = 5_000


def run_table(
    conn,
    table: TableSpec,
    store,
    *,
    start: date,
    end: date,
    today: date,
    overwrite: bool = False,
    pause_seconds: float = 0,
    progress: Heartbeat | None = None,
    at_risk: bool = True,
    quiet: bool = False,
    batch_size: int = BATCH_ROWS,
    extracted_at: datetime | None = None,
    sleep=time.sleep,
    clock=time.monotonic,
    prior_query: bool = False,
    announce_skips: bool = True,
) -> list:
    """Extract one table for ``[start, end)`` and return one result per planned partition.

    ``at_risk`` drops days retention has already deleted. Days that are not
    settled are listed and not queried. ``scan_mode="range"`` is one SELECT.
    ``prior_query`` pauses before the first query when an earlier table already
    queried. ``progress`` replaces the default heartbeat when it is set.
    """

    items = build_plan((table,), start, end, at_risk=at_risk, today=today)
    results, _queried, skipped = _run_items(
        conn,
        items,
        store,
        overwrite=overwrite,
        pause_seconds=pause_seconds,
        progress=progress,
        quiet=quiet,
        batch_size=batch_size,
        extracted_at=extracted_at,
        sleep=sleep,
        clock=clock,
        prior_query=prior_query,
    )
    if announce_skips and skipped:
        print(f"skipped {skipped} already-written partitions", flush=True)
    return results


def run_groups(conn, plan, store, **kwargs) -> list:
    """Run a prebuilt plan one table at a time, in plan order.

    ``run_plan`` uses this so a caller-built plan is not rebuilt. Skipped
    partitions are one total for the whole plan.
    """

    groups: list[list] = []
    for item in plan:
        if not groups or groups[-1][0].table.key != item.table.key:
            groups.append([item])
        else:
            groups[-1].append(item)
    prior = False
    results = []
    skipped_total = 0
    for group in groups:
        part, wrote_now, skipped = _run_items(conn, group, store, prior_query=prior, **kwargs)
        prior = prior or wrote_now
        results.extend(part)
        skipped_total += skipped
    if skipped_total:
        print(f"skipped {skipped_total} already-written partitions", flush=True)
    return results


def _run_items(
    conn,
    plan,
    store,
    *,
    overwrite: bool = False,
    pause_seconds: float = 0,
    progress: Heartbeat | None = None,
    quiet: bool = False,
    batch_size: int = BATCH_ROWS,
    extracted_at: datetime | None = None,
    sleep=time.sleep,
    clock=time.monotonic,
    prior_query: bool = False,
) -> tuple[list, bool, int]:
    """Execute work items that all belong to one table. Returns results, queried, skipped count."""

    from lft_pipelines.extract import extract_partition, extract_range

    results = []
    queried = prior_query
    skipped = 0
    index = 0
    total = len(plan)
    while index < len(plan):
        item = plan[index]
        if item.status == "not_settled":
            began = clock()
            print(plan_line(index + 1, index + 1, total, item.table.key, item.day, item.day, scanning=False), flush=True)
            from lft_pipelines.extract import PartitionResult

            result = PartitionResult(item.table.key, item.day, "not_settled", None)
            print(result_line(result, clock() - began), flush=True)
            results.append(result)
            index += 1
            continue
        if item.table.scan_mode == "range" and item.table.partition == "day":
            end = index + 1
            while (
                end < len(plan)
                and plan[end].status == "extract"
                and plan[end].table.key == item.table.key
                and plan[end].table.scan_mode == "range"
            ):
                end += 1
            group = plan[index:end]
            will_query = overwrite or any(
                not store.exists(manifest_rel(part.table.schema, part.table.table, part.day)) for part in group
            )
            if not will_query:
                skipped += len(group)
                for part in group:
                    from lft_pipelines.extract import PartitionResult

                    results.append(PartitionResult(part.table.key, part.day, "skipped", None))
                index = end
                continue
            print(
                plan_line(index + 1, end, total, item.table.key, group[0].day, group[-1].day, scanning=True),
                flush=True,
            )
            if queried and pause_seconds > 0:
                sleep(pause_seconds)
            began = clock()
            beat = progress if progress is not None else Heartbeat(
                item.table.key, clock=clock, enabled=not quiet, range_mode=True
            )
            grouped, nulls = extract_range(
                conn,
                group,
                store,
                overwrite=overwrite,
                batch_size=batch_size,
                extracted_at=extracted_at,
                progress=beat,
            )
            elapsed = clock() - began
            for result in grouped:
                if result.status == "skipped":
                    skipped += 1
                    continue
                print(result_line(result, elapsed), flush=True)
            print(f"null_partition_rows={nulls} table={item.table.key}", flush=True)
            if any(result.status == "wrote" for result in grouped):
                queried = True
            results.extend(grouped)
            index = end
            continue
        path = manifest_rel(item.table.schema, item.table.table, item.day)
        will_query = overwrite or not store.exists(path)
        if not will_query:
            from lft_pipelines.extract import PartitionResult

            results.append(PartitionResult(item.table.key, item.day, "skipped", None))
            skipped += 1
            index += 1
            continue
        print(plan_line(index + 1, index + 1, total, item.table.key, item.day, item.day, scanning=False), flush=True)
        if queried and pause_seconds > 0:
            sleep(pause_seconds)
        began = clock()
        beat = progress if progress is not None else Heartbeat(item.table.key, clock=clock, enabled=not quiet)
        result = extract_partition(
            conn,
            item.table,
            item.day,
            store,
            overwrite=overwrite,
            batch_size=batch_size,
            extracted_at=extracted_at,
            partial_by_retention=item.partial_by_retention,
            progress=beat,
        )
        print(result_line(result, clock() - began), flush=True)
        if result.status == "wrote":
            queried = True
        results.append(result)
        index += 1
    wrote_now = any(result.status == "wrote" for result in results)
    return results, wrote_now, skipped
