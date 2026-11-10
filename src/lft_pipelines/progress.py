"""Progress lines for a long extract.

Plan lines and the run summary go to stdout. Heartbeats go to stderr so a
terminal can overwrite one line without mixing into the manifest log. A log
(cron, Airflow) is not a TTY, so those heartbeats are full lines. ``--quiet``
turns heartbeats off. Plan lines and the summary stay.
"""

import sys
import time
from datetime import date

TTY_INTERVAL_SECONDS = 5.0
LOG_INTERVAL_SECONDS = 30.0


class Heartbeat:
    """A throttled stderr line for one partition or one range scan.

    ``note_batch`` is called once per fetched batch. It reads the clock once
    per call, not once per row.
    """

    def __init__(
        self,
        table: str,
        *,
        stream=None,
        clock=time.monotonic,
        tty: bool | None = None,
        enabled: bool = True,
        range_mode: bool = False,
    ):
        self.table = table
        self.stream = sys.stderr if stream is None else stream
        self.clock = clock
        self.enabled = enabled
        self.range_mode = range_mode
        if tty is None:
            tty = bool(getattr(self.stream, "isatty", lambda: False)())
        self.tty = tty
        self.interval = TTY_INTERVAL_SECONDS if tty else LOG_INTERVAL_SECONDS
        self.started = clock()
        self.last_emit = self.started
        self.rows = 0
        self.day: date | None = None
        self.days_with_rows = 0
        self._open = False

    def note_batch(self, day: date | None, fetched: int, *, days_with_rows: int | None = None) -> None:
        """Count one fetched batch and print if the interval has elapsed."""

        self.rows += fetched
        if day is not None:
            self.day = day
        if days_with_rows is not None:
            self.days_with_rows = days_with_rows
        if not self.enabled:
            return
        now = self.clock()
        if now - self.last_emit < self.interval:
            return
        self.last_emit = now
        self._write(now)

    def finish(self) -> None:
        """End an overwritten terminal line so the next log line is on its own row."""

        if self._open:
            self.stream.write("\n")
            self.stream.flush()
            self._open = False

    def _write(self, now: float) -> None:
        elapsed = now - self.started
        rate = self.rows / elapsed if elapsed else 0.0
        day = self.day.isoformat() if self.day is not None else "-"
        text = (
            f"{self.table} day={day} rows={self.rows} "
            f"rows_per_sec={rate:.0f} elapsed={elapsed:.0f}s"
        )
        if self.range_mode:
            text += f" days_with_rows={self.days_with_rows}"
        if self.tty:
            # \x1b[K clears the rest of the row so a shorter update does not leave old text.
            self.stream.write("\r" + text + "\x1b[K")
            self._open = True
        else:
            self.stream.write(text + "\n")
            self._open = False
        self.stream.flush()


def plan_line(index: int, end: int, total: int, table: str, first: date, last: date, *, scanning: bool) -> str:
    """The stdout line printed before a partition or a range scan."""

    if end == index:
        return f"[{index}/{total}] {table} date={first.isoformat()}"
    text = f"[{index}–{end}/{total}] {table} {first.isoformat()}..{last.isoformat()}"
    if scanning:
        return text + " (one scan)"
    return text


def result_line(result, elapsed: float) -> str:
    """The existing wrote, skipped, or not-settled line, plus elapsed seconds."""

    if result.status == "wrote":
        manifest = result.manifest or {}
        mismatch = bool(manifest.get("mismatch"))
        partial = bool(manifest.get("partial_by_retention"))
        rows = manifest.get("row_count") or 0
        return (
            f"wrote {result.table} date={result.day.isoformat()} rows={rows} "
            f"mismatch={str(mismatch).lower()} partial_by_retention={str(partial).lower()} "
            f"elapsed={elapsed:.1f}s"
        )
    if result.status == "not_settled":
        return f"not settled {result.table} date={result.day.isoformat()} elapsed={elapsed:.1f}s"
    return f"skipped {result.table} date={result.day.isoformat()} elapsed={elapsed:.1f}s"


def summary_line(results, elapsed: float) -> str:
    """One line of totals for the whole run."""

    written = skipped = unsettled = rows = mismatches = partials = 0
    for result in results:
        if result.status == "wrote":
            written += 1
            manifest = result.manifest or {}
            rows += int(manifest.get("row_count") or 0)
            if manifest.get("mismatch"):
                mismatches += 1
            if manifest.get("partial_by_retention"):
                partials += 1
        elif result.status == "skipped":
            skipped += 1
        elif result.status == "not_settled":
            unsettled += 1
    return (
        f"summary written={written} skipped={skipped} not_settled={unsettled} "
        f"rows={rows} mismatches={mismatches} partial_by_retention={partials} "
        f"elapsed={elapsed:.1f}s"
    )
