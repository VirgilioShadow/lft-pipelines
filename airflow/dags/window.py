"""Chicago date of an Airflow logical date, and the 7-day extract window.

Stdlib only, so the scheduler can import this without the extract venv.
``end`` is exclusive and equal to the Chicago calendar date of the run.
"""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CHICAGO = ZoneInfo("America/Chicago")


def chicago_run_date(logical: datetime | None, *, fallback: datetime | None = None) -> date:
    """Calendar date in America/Chicago for this run.

    A manual trigger in Airflow 3 leaves ``logical`` empty. ``fallback`` is
    ``dag_run.run_after`` in that case. When both are empty, the clock is used.
    """

    moment = logical if logical is not None else fallback
    if moment is None:
        moment = datetime.now(CHICAGO)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=CHICAGO)
    return moment.astimezone(CHICAGO).date()


def lookback_window(today: date) -> tuple[date, date]:
    """``[today - 7 days, today)``. Missed nights in that span are extracted again."""

    return today - timedelta(days=7), today
