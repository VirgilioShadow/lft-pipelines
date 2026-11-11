"""Check one table's manifests for a lookback window.

Pure: no Airflow import and no database. A failure is a bad or missing
manifest for a settled day still inside retention. A warning is a size,
timestamp, or schema change that a person should look at.
"""

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from statistics import median

from lft_pipelines.bounds import chicago_date
from lft_pipelines.registry import TableSpec


@dataclass(frozen=True)
class Finding:
    """One fail or warn for a table and day."""

    level: str
    code: str
    day: date
    message: str


@dataclass
class ValidationResult:
    """Findings for one table. ``ok`` is false when any finding is a fail."""

    table: str
    findings: list[Finding] = field(default_factory=list)
    fresh_days: set[date] | None = None

    @property
    def ok(self) -> bool:
        """True when nothing failed. Warnings do not fail the night."""

        return not any(item.level == "fail" for item in self.findings)

    @property
    def warnings(self) -> list[Finding]:
        """Warning findings only. The task log prints these."""

        return [item for item in self.findings if item.level == "warn"]

    @property
    def digest_warnings(self) -> list[Finding]:
        """Warnings for partitions written this run.

        ``fresh_days is None`` means the caller is not a nightly run, so every
        warning is included. An empty set posts nothing.
        """

        if self.fresh_days is None:
            return self.warnings
        return [item for item in self.warnings if item.day in self.fresh_days]


def validate_table(
    table: TableSpec,
    today: date,
    manifests: dict[date, dict],
    *,
    start: date,
    end: date,
    fresh_days: set[date] | None = None,
) -> ValidationResult:
    """Validate settled, in-retention days in ``[start, end)``.

    ``today`` is the Chicago date of the run. ``manifests`` may also hold the
    snapshot labeled with ``today``. A day is in retention when the table is
    not pruned, or ``day >= today - retention_days``. ``fresh_days`` are the
    partitions this run wrote. Hard failures still cover the whole window.
    Only the digest is limited to ``fresh_days``.
    """

    result = ValidationResult(table.key, fresh_days=fresh_days)
    if table.partition == "snapshot":
        _check_snapshot(table, today, manifests, result)
        return result
    day = start
    while day < end:
        if _needs_manifest(table, day, today):
            payload = manifests.get(day)
            if payload is None:
                result.findings.append(Finding("fail", "missing", day, f"{table.key} {day.isoformat()} has no manifest"))
            else:
                _check_payload(table, day, payload, manifests, result)
        day += timedelta(days=1)
    return result


def format_validation(result: ValidationResult) -> str:
    """One line per finding, then a short status line."""

    lines = [f"{item.level.upper()} {item.code} {item.day.isoformat()} {item.message}" for item in result.findings]
    state = "ok" if result.ok else "fail"
    lines.append(f"{result.table} {state} warnings={len(result.warnings)}")
    return "\n".join(lines)


def _needs_manifest(table: TableSpec, day: date, today: date) -> bool:
    if day >= today - timedelta(days=table.settle_days):
        return False
    if table.retention_days is None:
        return True
    return day >= today - timedelta(days=table.retention_days)


def _check_snapshot(table: TableSpec, today: date, manifests: dict[date, dict], result: ValidationResult) -> None:
    payload = manifests.get(today)
    if payload is None:
        result.findings.append(Finding("fail", "missing", today, f"{table.key} snapshot {today.isoformat()} has no manifest"))
        return
    if payload.get("mismatch"):
        result.findings.append(Finding("fail", "mismatch", today, f"{table.key} snapshot mismatch"))
    previous = _previous_manifest(manifests, today)
    if previous is not None:
        _columns(table, today, payload, previous, result)


def _check_payload(table: TableSpec, day: date, payload: dict, manifests: dict[date, dict], result: ValidationResult) -> None:
    if payload.get("mismatch"):
        result.findings.append(Finding("fail", "mismatch", day, f"{table.key} {day.isoformat()} row_count != db_count"))
    _bounds(table, day, payload, result)
    _size(table, day, payload, manifests, result)
    previous = _previous_manifest(manifests, day)
    if previous is not None:
        _columns(table, day, payload, previous, result)


def _bounds(table: TableSpec, day: date, payload: dict, result: ValidationResult) -> None:
    for label in ("min_timestamp", "max_timestamp"):
        value = payload.get(label)
        if value is None or not table.column_kind:
            continue
        if not _inside_day(value, table.column_kind, day):
            result.findings.append(
                Finding("warn", "outside_day", day, f"{_subject(table, day)}: a timestamp falls outside that Chicago day")
            )
            return


def _inside_day(value, column_kind: str, day: date) -> bool:
    if column_kind == "date":
        return str(value)[:10] == day.isoformat()
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return chicago_date(parsed, "timestamptz") == day


def _size(table: TableSpec, day: date, payload: dict, manifests: dict[date, dict], result: ValidationResult) -> None:
    if not table.row_band:
        return
    prior = _same_type_counts(day, manifests)
    if sum(1 for count in prior if count > 0) < 3:
        return
    middle = float(median(prior))
    if middle <= 0:
        return
    rows = payload.get("row_count") or 0
    low = 0.5 * middle
    high = 2 * middle
    if rows < low or rows > high:
        result.findings.append(Finding("warn", "row_band", day, _row_band_message(table, day, rows, middle)))


def _same_type_counts(day: date, manifests: dict[date, dict]) -> list[float]:
    """Row counts from up to the last four earlier days of the same type.

    Saturday and Sunday are one type. Monday through Friday are the other.
    Metra holidays are not split out; a holiday weekday uses the weekday median.
    """

    weekend = _is_weekend(day)
    keys = [key for key in manifests if key < day and _is_weekend(key) == weekend]
    keys.sort()
    return [float(manifests[key].get("row_count") or 0) for key in keys[-4:]]


def _is_weekend(day: date) -> bool:
    """True for Saturday and Sunday on the Chicago calendar date."""

    return day.weekday() >= 5


def _row_band_message(table: TableSpec, day: date, rows: float, middle: float) -> str:
    """How far this day sits from the same-type median, in plain language."""

    subject = _subject(table, day)
    kind = "weekend" if _is_weekend(day) else "weekday"
    if middle <= 0:
        return f"{subject}: row count is outside the recent range"
    if rows < middle:
        percent = round((1 - rows / middle) * 100)
        return f"{subject}: {percent}% fewer rows than the {kind} median"
    percent = round((rows / middle - 1) * 100)
    return f"{subject}: {percent}% more rows than the {kind} median"


def _columns(table: TableSpec, day: date, payload: dict, previous: dict, result: ValidationResult) -> None:
    if payload.get("columns") != previous.get("columns"):
        result.findings.append(
            Finding("warn", "columns", day, f"{_subject(table, day)}: the columns changed from the previous copy")
        )


def _subject(table: TableSpec, day: date) -> str:
    """``train positions, Nov 9`` for a warning bullet."""

    return f"{table.label}, {_MONTHS[day.month - 1]} {day.day}"


_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def fresh_days_from_extracts(extracted: list) -> dict[str, set[date]]:
    """Map each table to the Chicago dates this run wrote.

    A mapped task that failed has no return value and contributes nothing.
    Its missing manifests still fail the night.
    """

    found: dict[str, set[date]] = {}
    for item in extracted or []:
        if not isinstance(item, dict):
            continue
        key = item.get("table")
        if not key:
            continue
        days = found.setdefault(str(key), set())
        for raw in item.get("written") or []:
            days.add(date.fromisoformat(str(raw)[:10]))
    return found


def require_night_ok(failed_names: list[str]) -> None:
    """Raise when any table failed. A partition from an earlier night still counts."""

    if failed_names:
        names = ", ".join(failed_names)
        raise RuntimeError(f"archive check failed for {names}")


def _previous_manifest(manifests: dict[date, dict], day: date) -> dict | None:
    earlier = [key for key in manifests if key < day]
    if not earlier:
        return None
    return manifests[max(earlier)]
