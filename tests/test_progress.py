"""Progress lines: throttle, log interval, --quiet, and the run summary."""

import io
from datetime import date
from types import SimpleNamespace

from lft_pipelines.extract import extract_partition, parse_args, run_plan
from lft_pipelines.plan import WorkItem
from lft_pipelines.progress import Heartbeat
from lft_pipelines.registry import resolve_tables
from lft_pipelines.storage import open_store
from tests.fakes import FakeConn
from tests.test_extract import DAY, WHEN, _description, _rows


def _clock(values):
    stream = iter(values)

    def clock():
        return next(stream)

    return clock, stream


def test_heartbeat_is_silent_before_five_seconds_and_prints_once(tmp_path):
    clock, leftover = _clock([0.0, 0.0, 4.9, 5.0])
    stream = io.StringIO()
    beat = Heartbeat("metra.Positions", stream=stream, clock=clock, tty=True)
    table = resolve_tables("metra.Positions")[0]
    extract_partition(
        FakeConn(_rows(5), _description(), count=5),
        table,
        DAY,
        open_store(str(tmp_path)),
        batch_size=2,
        extracted_at=WHEN,
        progress=beat,
    )
    text = stream.getvalue()
    assert text.count("rows=") == 1
    assert text.startswith("\r")
    assert "day=2026-01-15" in text
    assert "elapsed=5s" in text
    assert text.endswith("\n")
    assert "\r" not in text[1:]
    try:
        next(leftover)
    except StopIteration:
        return
    raise AssertionError("clock was read more than once per batch")


def test_log_heartbeat_waits_thirty_seconds_and_is_a_full_line():
    clock, _leftover = _clock([0.0, 29.9, 30.0])
    stream = io.StringIO()
    beat = Heartbeat("metra.Positions", stream=stream, clock=clock, tty=False, range_mode=True)
    beat.note_batch(date(2026, 3, 8), 10, days_with_rows=1)
    assert stream.getvalue() == ""
    beat.note_batch(date(2026, 3, 9), 10, days_with_rows=2)
    text = stream.getvalue()
    assert "\r" not in text
    assert text == (
        "metra.Positions day=2026-03-09 rows=20 rows_per_sec=1 "
        "elapsed=30s days_with_rows=2\n"
    )


def test_quiet_suppresses_heartbeats(tmp_path, capsys):
    clock, _leftover = _clock([0.0, 0.0, 0.0, 100.0, 100.0, 100.0])
    table = resolve_tables("metra.Positions")[0]
    plan = (WorkItem(table, DAY, "extract", False),)
    run_plan(
        FakeConn(_rows(1), _description(), count=1, group_rows=[(DAY, 1)]),
        plan,
        open_store(str(tmp_path)),
        pause_seconds=0,
        extracted_at=WHEN,
        quiet=True,
        clock=clock,
    )
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "[1/1] metra.Positions date=2026-01-15" in captured.out
    assert "summary written=1" in captured.out
    args = parse_args(
        ["--tables", "metra.Positions", "--start", "2026-01-15", "--end", "2026-01-16", "--out", "/tmp/out", "--quiet"]
    )
    assert args.quiet is True


def test_summary_counts_written_skipped_unsettled_mismatch_and_partial(tmp_path, capsys):
    stop = resolve_tables("metra.StopDelayForecast")[0]
    plan = (
        WorkItem(stop, date(2026, 11, 7), "extract", True),
        WorkItem(stop, date(2026, 11, 8), "extract", False),
        WorkItem(stop, date(2026, 11, 9), "not_settled", False),
    )
    store = open_store(str(tmp_path))
    store.write_json("metra/StopDelayForecast/date=2026-11-08/_manifest.json", {"sentinel": True})
    description = [SimpleNamespace(name="service_date", type_code=1082)]
    run_plan(
        FakeConn([(date(2026, 11, 7),)], description, count=4),
        plan,
        store,
        pause_seconds=0,
        extracted_at=WHEN,
    )
    out = capsys.readouterr().out
    assert "[1/3] metra.StopDelayForecast date=2026-11-07" in out
    assert "mismatch=true partial_by_retention=true" in out
    assert "[2/3] metra.StopDelayForecast date=2026-11-08" not in out
    assert "skipped 1 already-written partitions" in out
    assert "skipped metra.StopDelayForecast date=2026-11-08" not in out
    assert "not settled metra.StopDelayForecast date=2026-11-09" in out
    assert "summary written=1 skipped=1 not_settled=1 rows=1 mismatches=1 partial_by_retention=1 " in out
