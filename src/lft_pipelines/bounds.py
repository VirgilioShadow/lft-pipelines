"""Chicago calendar days as UTC half-open ranges.

The partition date is the America/Chicago calendar date. The SQL predicate
compares the raw column to those bounds. It does not wrap the column in a
function, so a plain btree index on the column can be used.
"""

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

CHICAGO = ZoneInfo("America/Chicago")


def chicago_day_bounds(day: date) -> tuple[datetime, datetime]:
    """Return ``[start, end)`` in UTC for one Chicago calendar date.

    The spring-forward date is 23 hours. The fall-back date is 25 hours.
    """

    start_local = datetime.combine(day, time.min, tzinfo=CHICAGO)
    end_local = datetime.combine(day + timedelta(days=1), time.min, tzinfo=CHICAGO)
    return (
        start_local.astimezone(timezone.utc),
        end_local.astimezone(timezone.utc),
    )


def chicago_date(value, column_kind: str) -> date:
    """Chicago calendar date of one partition-column value."""

    if column_kind == "date":
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        raise TypeError(f"expected a date, got {type(value).__name__}")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(CHICAGO).date()
    raise TypeError(f"expected a timestamp, got {type(value).__name__}")


def predicate_bounds(column_kind: str, day: date) -> tuple[object, object]:
    """Bounds passed to ``column >= %s AND column < %s``.

    A ``date`` column is already a Chicago service date, so the bounds are
    that date and the next date. A ``timestamptz`` column uses the UTC
    instants of the Chicago midnights.
    """

    if column_kind == "date":
        return day, day + timedelta(days=1)
    if column_kind == "timestamptz":
        return chicago_day_bounds(day)
    raise ValueError(f"unsupported column kind {column_kind}")
