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

    @property
    def ok(self) -> bool:
        """True when nothing failed. Warnings do not fail the night."""

        return not any(item.level == "fail" for item in self.findings)

    @property
    def warnings(self) -> list[Finding]:
        """Warning findings only."""

        return [item for item in self.findings if item.level == "warn"]


def validate_table(
    table: TableSpec,
    today: date,
    manifests: dict[date, dict],
    *,
    start: date,
    end: date,
) -> ValidationResult:
    """Validate settled, in-retention days in ``[start, end)``.

    ``today`` is the Chicago date of the run. ``manifests`` may also hold the
    snapshot labeled with ``today``. A day is in retention when the table is
    not pruned, or ``day >= today - retention_days``.
    """

    result = ValidationResult(table.key)
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
        _columns(table.key, today, payload, previous, result)


def _check_payload(table: TableSpec, day: date, payload: dict, manifests: dict[date, dict], result: ValidationResult) -> None:
    if payload.get("mismatch"):
        result.findings.append(Finding("fail", "mismatch", day, f"{table.key} {day.isoformat()} row_count != db_count"))
    _bounds(table, day, payload, result)
    _size(table.key, day, payload, manifests, result)
    previous = _previous_manifest(manifests, day)
    if previous is not None:
        _columns(table.key, day, payload, previous, result)


def _bounds(table: TableSpec, day: date, payload: dict, result: ValidationResult) -> None:
    for label in ("min_timestamp", "max_timestamp"):
        value = payload.get(label)
        if value is None or not table.column_kind:
            continue
        if not _inside_day(value, table.column_kind, day):
            result.findings.append(
                Finding("warn", "outside_day", day, f"{table.key} {label} {value} is outside {day.isoformat()}")
            )


def _inside_day(value, column_kind: str, day: date) -> bool:
    if column_kind == "date":
        return str(value)[:10] == day.isoformat()
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return chicago_date(parsed, "timestamptz") == day


def _size(table: str, day: date, payload: dict, manifests: dict[date, dict], result: ValidationResult) -> None:
    prior = [manifests[key].get("row_count") or 0 for key in sorted(manifests) if key < day][-7:]
    if len(prior) < 3:
        return
    middle = float(median(prior))
    rows = payload.get("row_count") or 0
    low = 0.5 * middle
    high = 2 * middle
    if rows < low or rows > high:
        result.findings.append(
            Finding("warn", "row_band", day, f"{table} rows={rows} outside [{low:.0f}, {high:.0f}]")
        )


def _columns(table: str, day: date, payload: dict, previous: dict, result: ValidationResult) -> None:
    if payload.get("columns") != previous.get("columns"):
        result.findings.append(Finding("warn", "columns", day, f"{table} columns differ from the previous manifest"))


def _previous_manifest(manifests: dict[date, dict], day: date) -> dict | None:
    earlier = [key for key in manifests if key < day]
    if not earlier:
        return None
    return manifests[max(earlier)]
