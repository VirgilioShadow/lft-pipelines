"""Tables this extract knows how to read.

Names and prune columns come from the ingest Prisma schema and the retention
jobs. A table is listed here only. The CLI cannot introduce a new relation.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    """One source relation and the rule used to partition it.

    ``retention_days`` is the rolling window after which ingest deletes rows.
    ``None`` means the table is not pruned. ``column`` is the prune column
    when one exists, which is also the partition column. ``column_kind`` is
    ``timestamptz`` or ``date``. Snapshot tables have neither.
    """

    schema: str
    table: str
    partition: str
    column: str | None
    column_kind: str | None
    retention_days: int | None
    # A day is not settled when day >= today - settle_days. 0 refuses today.
    # 1 refuses today and yesterday (buckets rebuilt for the latest service date).
    settle_days: int
    # per_day runs one SELECT per Chicago day. range runs one SELECT for the
    # whole planned span. Positions.recorded_at has no index, so per-day
    # queries would each scan the table.
    scan_mode: str
    # Plain name for Discord only. Queries and paths keep ``schema.table``.
    label: str

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.table}"


def _day(
    schema: str,
    table: str,
    column: str,
    kind: str,
    retention: int | None,
    label: str,
    settle_days: int = 0,
    scan_mode: str = "per_day",
) -> TableSpec:
    return TableSpec(schema, table, "day", column, kind, retention, settle_days, scan_mode, label)


def _snapshot(schema: str, table: str, label: str) -> TableSpec:
    return TableSpec(schema, table, "snapshot", None, None, None, 0, "per_day", label)


# Order is the default plan order. --at-risk reorders by retention_days.
TABLES: tuple[TableSpec, ...] = (
    # Pruned in scheduleMaintenanceJobs.ts on the named timestamp.
    _day("metra", "TripUpdateSnapshot", "polled_at", "timestamptz", 120, "trip updates"),
    # recorded_at is not indexed (source_id, updatedAt, trip_id are).
    _day("metra", "Positions", "recorded_at", "timestamptz", 30, "train positions", scan_mode="range"),
    _day("cta", "CtaTripUpdateSnapshot", "polled_at", "timestamptz", 120, "CTA trip updates"),
    _day("cta", "CtaPositionSnapshot", "polled_at", "timestamptz", 30, "CTA positions"),
    _day("metra", "DelayForecast", "predicted_at", "timestamptz", 180, "delay forecasts"),
    # StopDelayForecast is deleted by service_date after 2 days
    # (forecast job and the ingest retention table). Not in the 2:45–3:40 jobs.
    _day("metra", "StopDelayForecast", "service_date", "date", 2, "stop delay forecasts"),
    # Kept. Still partitioned by the timestamp or service date they already have.
    _day("metra", "SpeedObservation", "recorded_at", "timestamptz", None, "speed observations"),
    _day("weather", "WeatherObservation", "observed_at", "timestamptz", None, "weather observations"),
    # Rebuilt for the latest service date, so yesterday is not settled either.
    _day("metra", "OnTimeByTodDailyBucket", "service_date", "date", None, "on-time buckets", settle_days=1),
    _day("metra", "HourlySpeedBinDailyBucket", "service_date", "date", None, "hourly speed bins", settle_days=1),
    # Upserted every poll, so polled_at is not a stable partition. One full copy per run.
    _snapshot("metra", "ServiceAlert", "service alerts"),
    # Static GTFS. One full copy per run, labeled with the run's as-of date.
    # CalendarDates.date is a service exception, not a retention clock.
    _snapshot("metra", "Trips", "schedule (trips)"),
    _snapshot("metra", "StopTimes", "schedule (stop times)"),
    _snapshot("metra", "Stops", "schedule (stops)"),
    _snapshot("metra", "Routes", "schedule (routes)"),
    _snapshot("metra", "Shapes", "schedule (shapes)"),
    _snapshot("metra", "Calendars", "schedule (calendars)"),
    _snapshot("metra", "CalendarDates", "schedule (calendar dates)"),
)

_BY_KEY = {table.key: table for table in TABLES}


def resolve_tables(spec: str) -> tuple[TableSpec, ...]:
    """Parse ``all`` or a comma-separated list of ``schema.table`` names."""

    text = spec.strip()
    if text == "all":
        return TABLES
    wanted = [part.strip() for part in text.split(",") if part.strip()]
    if not wanted:
        raise ValueError("no tables given")
    missing = [name for name in wanted if name not in _BY_KEY]
    if missing:
        known = ", ".join(table.key for table in TABLES)
        raise ValueError(f"unknown table {missing[0]}. Known tables: {known}")
    return tuple(_BY_KEY[name] for name in wanted)
