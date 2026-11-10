"""Read one partition at a time and write it to Parquet.

Import ``extract_partition`` and ``build_plan`` from an Airflow task. The CLI
is a thin wrapper so the same functions run on a laptop and, later, on a Droplet.
"""

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone

from lft_pipelines import __version__
from lft_pipelines.arrow_io import (
    arrow_schema,
    column_names,
    empty_batch,
    rows_to_batch,
    schema_columns,
)
from lft_pipelines.bounds import CHICAGO, chicago_date, predicate_bounds
from lft_pipelines.plan import build_plan, format_plan
from lft_pipelines.progress import Heartbeat, summary_line
from lft_pipelines.registry import resolve_tables
from lft_pipelines.sql import queries_for, range_queries
from lft_pipelines.storage import manifest_rel, open_store, parquet_rel

APP_NAME = "lft-pipelines-extract"
DEFAULT_STATEMENT_TIMEOUT_MS = 300_000
DEFAULT_PAUSE_SECONDS = 2.0
BATCH_ROWS = 5_000


@dataclass(frozen=True)
class PartitionResult:
    table: str
    day: date
    status: str
    manifest: dict | None


def connect(environ: dict | None = None):
    """Open a read-only session.

    ``default_transaction_read_only``, ``statement_timeout``, and
    ``application_name`` are set as libpq startup parameters. That is the
    session equivalent of ``SET TRANSACTION READ ONLY`` and ``SET
    statement_timeout``, and it keeps every cursor statement a SELECT.
    """

    env = os.environ if environ is None else environ
    url = env.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not set")
    timeout = int(env.get("STATEMENT_TIMEOUT_MS", str(DEFAULT_STATEMENT_TIMEOUT_MS)))
    if timeout <= 0:
        raise RuntimeError("STATEMENT_TIMEOUT_MS must be positive")
    import psycopg

    return psycopg.connect(
        url,
        application_name=APP_NAME,
        options=f"-c default_transaction_read_only=on -c statement_timeout={timeout}",
        autocommit=False,
    )


def _track(value, stats: dict) -> None:
    if value is None:
        return
    if stats["min"] is None or value < stats["min"]:
        stats["min"] = value
    if stats["max"] is None or value > stats["max"]:
        stats["max"] = value


def _iso(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).isoformat()
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _manifest(stats: dict, db_count: int, schema, when: datetime, partial: bool) -> dict:
    return {
        "row_count": stats["n"],
        "db_count": db_count,
        "mismatch": stats["n"] != db_count,
        "partial_by_retention": partial,
        "min_timestamp": _iso(stats["min"]),
        "max_timestamp": _iso(stats["max"]),
        "columns": schema_columns(schema),
        "extracted_at": when.astimezone(timezone.utc).isoformat(),
        "tool_version": __version__,
    }


def extract_partition(
    conn,
    table,
    day: date,
    store,
    *,
    overwrite: bool = False,
    batch_size: int = BATCH_ROWS,
    extracted_at: datetime | None = None,
    partial_by_retention: bool = False,
    progress: Heartbeat | None = None,
) -> PartitionResult:
    """Extract one Chicago day, or one full snapshot labeled with ``day``.

    A partition that already has ``_manifest.json`` is left alone unless
    ``overwrite`` is set. Rows are fetched with a named server-side cursor and
    written batch by batch. ``count(*)`` for the same predicate is compared
    with the number of rows written.
    """

    try:
        manifest_path = manifest_rel(table.schema, table.table, day)
        data_path = parquet_rel(table.schema, table.table, day)
        if store.exists(manifest_path) and not overwrite:
            return PartitionResult(table.key, day, "skipped", None)

        (select_sql, select_params), (count_sql, count_params) = queries_for(table, day)
        when = extracted_at or datetime.now(timezone.utc)
        stats = {"n": 0, "min": None, "max": None}

        with conn.transaction():
            with conn.cursor(name="lft_extract") as cur:
                if hasattr(cur, "itersize"):
                    cur.itersize = batch_size
                cur.execute(select_sql, select_params)
                if not cur.description:
                    raise RuntimeError(f"{table.key} returned no column description")
                schema = arrow_schema(cur.description)
                names = column_names(cur.description)
                column_index = names.index(table.column) if table.column else None

                def batches():
                    while True:
                        rows = cur.fetchmany(batch_size)
                        if not rows:
                            break
                        stats["n"] += len(rows)
                        if column_index is not None:
                            for row in rows:
                                _track(row[column_index], stats)
                        if progress is not None:
                            progress.note_batch(day, len(rows))
                        yield rows_to_batch(rows, schema)
                    if stats["n"] == 0:
                        yield empty_batch(schema)

                store.write_parquet(data_path, schema, batches())

            with conn.cursor() as cur:
                cur.execute(count_sql, count_params)
                counted = cur.fetchone()
        db_count = int(counted[0]) if counted and counted[0] is not None else 0
        manifest = _manifest(stats, db_count, schema, when, partial_by_retention)
        store.write_json(manifest_path, manifest)
        return PartitionResult(table.key, day, "wrote", manifest)
    finally:
        if progress is not None:
            progress.finish()


def _grouped_counts(rows) -> dict[date, int]:
    counts: dict[date, int] = {}
    for row in rows:
        day_value = row[0]
        if isinstance(day_value, datetime):
            day_value = day_value.date()
        counts[day_value] = int(row[1])
    return counts


def extract_range(
    conn,
    items,
    store,
    *,
    overwrite: bool = False,
    batch_size: int = BATCH_ROWS,
    extracted_at: datetime | None = None,
    progress: Heartbeat | None = None,
) -> tuple[list[PartitionResult], int]:
    """One SELECT for a contiguous span of an unindexed day table.

    Each row is routed by the Chicago date of the partition column. Writers
    open on the first row of a day and publish when the scan finishes. Days
    that already have a manifest are not rewritten unless ``overwrite`` is
    set. If every planned day already has one, no SQL runs. A NULL partition
    value is counted and not written.
    """

    try:
        return _extract_range(
            conn,
            items,
            store,
            overwrite=overwrite,
            batch_size=batch_size,
            extracted_at=extracted_at,
            progress=progress,
        )
    finally:
        if progress is not None:
            progress.finish()


def _extract_range(
    conn,
    items,
    store,
    *,
    overwrite: bool,
    batch_size: int,
    extracted_at: datetime | None,
    progress: Heartbeat | None,
) -> tuple[list[PartitionResult], int]:
    if not items:
        return [], 0
    table = items[0].table
    pending = []
    skipped: dict[date, PartitionResult] = {}
    for item in items:
        path = manifest_rel(item.table.schema, item.table.table, item.day)
        if store.exists(path) and not overwrite:
            skipped[item.day] = PartitionResult(table.key, item.day, "skipped", None)
        else:
            pending.append(item)
    if not pending:
        return [skipped[item.day] for item in items], 0

    start_bound, _end_ignored = predicate_bounds(table.column_kind, items[0].day)
    _start_ignored, end_bound = predicate_bounds(table.column_kind, items[-1].day)
    select_sql, count_sql = range_queries(table)
    params = (start_bound, end_bound)
    when = extracted_at or datetime.now(timezone.utc)
    pending_by_day = {item.day: item for item in pending}
    buffers = {item.day: [] for item in pending}
    stats = {item.day: {"n": 0, "min": None, "max": None} for item in pending}
    sinks = {}
    published: set[date] = set()
    nulls = 0

    def flush(day: date, schema) -> None:
        rows = buffers[day]
        if not rows:
            return
        if day not in sinks:
            sinks[day] = store.open_parquet_sink(parquet_rel(table.schema, table.table, day), schema)
        sinks[day].write_batch(rows_to_batch(rows, schema))
        buffers[day] = []

    try:
        with conn.transaction():
            with conn.cursor(name="lft_extract") as cur:
                if hasattr(cur, "itersize"):
                    cur.itersize = batch_size
                cur.execute(select_sql, params)
                if not cur.description:
                    raise RuntimeError(f"{table.key} returned no column description")
                schema = arrow_schema(cur.description)
                names = column_names(cur.description)
                column_index = names.index(table.column)
                last_day = None
                days_seen: set[date] = set()
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    for row in rows:
                        value = row[column_index]
                        if value is None:
                            nulls += 1
                            continue
                        day = chicago_date(value, table.column_kind)
                        if day not in pending_by_day:
                            continue
                        _track(value, stats[day])
                        stats[day]["n"] += 1
                        last_day = day
                        days_seen.add(day)
                        buffers[day].append(row)
                        if len(buffers[day]) >= batch_size:
                            flush(day, schema)
                    if progress is not None:
                        progress.note_batch(last_day, len(rows), days_with_rows=len(days_seen))
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                grouped = cur.fetchall()
        db_counts = _grouped_counts(grouped)
        for day in pending_by_day:
            flush(day, schema)
        wrote: dict[date, PartitionResult] = {}
        for item in pending:
            day = item.day
            if day not in sinks:
                sinks[day] = store.open_parquet_sink(parquet_rel(table.schema, table.table, day), schema)
                sinks[day].write_batch(empty_batch(schema))
            sinks[day].publish()
            published.add(day)
            db_count = db_counts.get(day, 0)
            manifest = _manifest(stats[day], db_count, schema, when, item.partial_by_retention)
            store.write_json(manifest_rel(table.schema, table.table, day), manifest)
            wrote[day] = PartitionResult(table.key, day, "wrote", manifest)
        ordered = [wrote[item.day] if item.day in wrote else skipped[item.day] for item in items]
        return ordered, nulls
    except Exception:
        for day, sink in sinks.items():
            if day not in published:
                sink.abort()
        raise


def run_plan(
    conn,
    plan,
    store,
    *,
    overwrite: bool = False,
    pause_seconds: float = DEFAULT_PAUSE_SECONDS,
    batch_size: int = BATCH_ROWS,
    sleep=time.sleep,
    extracted_at: datetime | None = None,
    quiet: bool = False,
    clock=time.monotonic,
) -> list[PartitionResult]:
    """Run a plan. Pause after a query, before the next query, to rest the database.

    Each table is handed to ``run_table``'s executor. Skipped days are one total.
    """

    from lft_pipelines.execute import run_groups

    started = clock()
    results = run_groups(
        conn,
        plan,
        store,
        overwrite=overwrite,
        pause_seconds=pause_seconds,
        quiet=quiet,
        batch_size=batch_size,
        extracted_at=extracted_at,
        sleep=sleep,
        clock=clock,
    )
    print(summary_line(results, clock() - started), flush=True)
    return results


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m lft_pipelines.extract",
        description="Read retained history into day-partitioned Parquet.",
    )
    parser.add_argument("--tables", required=True, help="all, or schema.table names separated by commas")
    parser.add_argument("--start", required=True, help="First Chicago date, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="As-of date, exclusive for day tables (YYYY-MM-DD)")
    parser.add_argument("--out", required=True, help="Local directory or s3://bucket/prefix")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--pause-seconds", type=float, default=DEFAULT_PAUSE_SECONDS)
    parser.add_argument("--dry-run", action="store_true", help="Print the plan and do not query")
    parser.add_argument(
        "--at-risk",
        action="store_true",
        help="Drop days retention has already deleted, and run the shortest windows first",
    )
    parser.add_argument("--quiet", action="store_true", help="Turn off stderr heartbeats")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        tables = resolve_tables(args.tables)
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end)
        today = datetime.now(CHICAGO).date()
        plan = build_plan(tables, start, end, at_risk=args.at_risk, today=today)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.dry_run:
        print(format_plan(args.out, plan))
        return 0

    from dotenv import load_dotenv

    from lft_pipelines.execute import run_table

    load_dotenv()
    ordered: list = []
    seen: set[str] = set()
    for item in plan:
        if item.table.key not in seen:
            seen.add(item.table.key)
            ordered.append(item.table)
    store = open_store(args.out)
    conn = connect()
    started = time.monotonic()
    try:
        results = []
        prior = False
        skipped = 0
        for table in ordered:
            part = run_table(
                conn,
                table,
                store,
                start=start,
                end=end,
                today=today,
                overwrite=args.overwrite,
                pause_seconds=args.pause_seconds,
                at_risk=args.at_risk,
                quiet=args.quiet,
                prior_query=prior,
                announce_skips=False,
            )
            prior = prior or any(result.status == "wrote" for result in part)
            skipped += sum(1 for result in part if result.status == "skipped")
            results.extend(part)
        if skipped:
            print(f"skipped {skipped} already-written partitions", flush=True)
        print(summary_line(results, time.monotonic() - started), flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
