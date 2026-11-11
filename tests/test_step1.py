"""Step 1: one-table runs, validation, compare, lookback dates, alert text."""

import ast
import builtins
import json
import sys
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from types import SimpleNamespace

from lft_pipelines.compare import compare_manifests, main as compare_main
from lft_pipelines.execute import run_table
from lft_pipelines.extract import run_plan
from lft_pipelines.plan import WorkItem, build_plan
from lft_pipelines.registry import resolve_tables
from lft_pipelines.storage import open_store
from lft_pipelines.alerts import warning_message
from lft_pipelines.validate import validate_table

from tests.fakes import FakeConn

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))
from notify import callback_fields, failure_message  # noqa: E402
from tables import ordered_keys  # noqa: E402
from window import chicago_run_date, lookback_window  # noqa: E402

WHEN = datetime(2026, 11, 9, 6, 0, tzinfo=timezone.utc)
SECRET = "do-not-leak-this-token"


def _desc():
    return [SimpleNamespace(name="service_date", type_code=1082)]


def test_run_table_matches_run_plan(tmp_path):
    table = resolve_tables("metra.StopDelayForecast")[0]
    today = date(2026, 11, 10)
    start, end = date(2026, 11, 8), date(2026, 11, 10)
    plan = build_plan((table,), start, end, at_risk=True, today=today)
    rows = [(date(2026, 11, 8),)]

    def run(store):
        return FakeConn(rows, _desc(), count=1)

    left = run_plan(run(None), plan, open_store(str(tmp_path / "a")), pause_seconds=0, extracted_at=WHEN, quiet=True)
    right = run_table(
        run(None),
        table,
        open_store(str(tmp_path / "b")),
        start=start,
        end=end,
        today=today,
        pause_seconds=0,
        extracted_at=WHEN,
        quiet=True,
        announce_skips=False,
    )
    assert [(item.day, item.status) for item in left] == [(item.day, item.status) for item in right]


def test_skipped_days_are_one_total(tmp_path, capsys):
    table = resolve_tables("metra.StopDelayForecast")[0]
    store = open_store(str(tmp_path))
    store.write_json("metra/StopDelayForecast/date=2026-11-08/_manifest.json", {"sentinel": True})
    plan = (WorkItem(table, date(2026, 11, 8), "extract", False),)
    run_plan(FakeConn([], _desc(), count=0), plan, store, pause_seconds=0, extracted_at=WHEN)
    out = capsys.readouterr().out
    assert out.count("skipped 1 already-written partitions") == 1
    assert "date=2026-11-08" not in out.split("summary")[0]


def _manifest(rows, day, columns=None, min_ts=None, max_ts=None, mismatch=False):
    return {
        "row_count": rows,
        "db_count": rows if not mismatch else rows + 1,
        "mismatch": mismatch,
        "min_timestamp": min_ts if min_ts is not None else f"{day.isoformat()}T18:00:00+00:00",
        "max_timestamp": max_ts if max_ts is not None else f"{day.isoformat()}T20:00:00+00:00",
        "columns": columns if columns is not None else [{"name": "id", "type": "int64"}],
        "extracted_at": "2026-11-10T06:00:00+00:00",
        "tool_version": "0.1.0",
    }


def test_validate_fail_and_warn_and_short_history():
    table = resolve_tables("metra.Positions")[0]
    today = date(2026, 11, 10)
    start, end = date(2026, 11, 3), today
    days = [start + timedelta(days=offset) for offset in range(7)]
    manifests = {day: _manifest(10, day) for day in days[:2]}
    short = validate_table(table, today, manifests, start=start, end=end)
    assert any(item.code == "missing" for item in short.findings)
    assert not any(item.code == "row_band" for item in short.findings)

    full = {day: _manifest(10, day) for day in days}
    full[days[3]] = _manifest(10, days[3], mismatch=True)
    full[days[4]] = _manifest(80, days[4])
    full[days[5]] = _manifest(
        10,
        days[5],
        min_ts="2026-11-01T00:00:00+00:00",
        columns=[{"name": "other", "type": "int64"}],
    )
    result = validate_table(table, today, full, start=start, end=end)
    codes = {item.code for item in result.findings}
    assert "mismatch" in codes
    assert "row_band" in codes
    assert "outside_day" in codes
    assert "columns" in codes
    assert result.ok is False
    row_band = next(item.message for item in result.findings if item.code == "row_band")
    assert row_band == "train positions, Nov 7: 700% more rows than the 7-day median"


def test_compare_equal_different_and_one_sided(tmp_path, capsys):
    day = date(2026, 11, 8)
    left = open_store(str(tmp_path / "a"))
    right = open_store(str(tmp_path / "b"))
    payload = _manifest(4, day)
    other = dict(payload)
    other["extracted_at"] = "2026-11-11T00:00:00+00:00"
    other["tool_version"] = "other"
    rel = "metra/StopDelayForecast/date=2026-11-08/_manifest.json"
    left.write_json(rel, payload)
    right.write_json(rel, other)
    assert compare_manifests(payload, other) == []
    code = compare_main(["--a", str(tmp_path / "a"), "--b", str(tmp_path / "b"), "--start", "2026-11-08", "--end", "2026-11-09", "--tables", "metra.StopDelayForecast"])
    assert code == 0
    right.write_json(rel, _manifest(9, day))
    code = compare_main(["--a", str(tmp_path / "a"), "--b", str(tmp_path / "b"), "--start", "2026-11-08", "--end", "2026-11-09", "--tables", "metra.StopDelayForecast"])
    assert code == 1
    assert "row_count" in capsys.readouterr().out
    only = tmp_path / "c"
    code = compare_main(["--a", str(tmp_path / "a"), "--b", str(only), "--start", "2026-11-08", "--end", "2026-11-09", "--tables", "metra.StopDelayForecast"])
    assert code == 1
    assert "only-left" in capsys.readouterr().out


def test_lookback_around_dst():
    fall = datetime(2026, 11, 1, 5, 30, tzinfo=timezone.utc)
    today = chicago_run_date(fall)
    assert today == date(2026, 11, 1)
    assert lookback_window(today) == (date(2026, 10, 25), date(2026, 11, 1))
    spring = datetime(2027, 3, 14, 6, 30, tzinfo=timezone.utc)
    today = chicago_run_date(spring)
    assert today == date(2027, 3, 14)
    assert lookback_window(today) == (date(2027, 3, 7), date(2027, 3, 14))


def test_failure_message_hides_env_and_truncates(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", SECRET)
    monkeypatch.setenv("DATABASE_URL", SECRET)
    monkeypatch.setenv("SPACES_SECRET", SECRET)
    headline = "🔴 Nightly archive failed: train positions (metra.Positions), Nov 11"
    text = failure_message(
        {
            "dag_id": "archive_raw_daily",
            "task_id": "extract_table",
            "try_number": 3,
            "table": "metra.Positions",
            "day": "2026-11-11",
            "today": "2026-11-11",
            "exception_line": "connection to the database was refused",
            "log_lines": [f"\x1b[31m2026-11-11T06:31:04Z row {index} " + ("x" * 400) for index in range(8)],
        }
    )
    assert SECRET not in text
    assert len(text) <= 1800
    assert text.splitlines()[0] == headline
    assert "failed 3 times" in text
    assert "connection to the database was refused" in text
    assert "safe until Dec 11" in text
    assert "nothing tonight; if this repeats tomorrow, tell Claude" in text
    assert "\x1b" not in text
    assert "2026-11-11T06:31:04Z" not in text
    detail = text.split("```")[1].strip().splitlines()
    assert 0 < len(detail) <= 5
    assert json.dumps({"content": text})


def test_alert_risk_dates_warnings_and_secret(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", SECRET)
    monkeypatch.setenv("ARCHIVE_URL", SECRET)
    digest = warning_message(
        date(2026, 11, 11),
        ["train positions, Nov 9: 41% fewer rows than the 7-day median"],
    )
    assert digest.splitlines()[0] == "🟡 Archive check: 1 warning for Nov 11"
    assert "- train positions, Nov 9: 41% fewer rows than the 7-day median" in digest
    assert SECRET not in digest

    kept = failure_message(
        {
            "task_id": "extract_table",
            "try_number": 1,
            "table": "metra.SpeedObservation",
            "day": "2026-11-11",
            "today": "2026-11-11",
        }
    )
    assert kept.splitlines()[0] == "🔴 Nightly archive failed: speed observations (metra.SpeedObservation), Nov 11"
    assert "Risk: none, the database keeps this table" in kept

    boundary = failure_message(
        {
            "task_id": "extract_table",
            "try_number": 1,
            "table": "metra.StopDelayForecast",
            "day": "2026-01-31",
            "today": "2026-01-31",
        }
    )
    assert "safe until Feb 2" in boundary

    lost = failure_message(
        {
            "task_id": "extract_table",
            "try_number": 2,
            "table": "metra.Positions",
            "day": "2026-10-01",
            "today": "2026-11-11",
            "exception_line": "timeout",
        }
    )
    assert "ended Oct 31" in lost
    assert "lost unless it was already archived" in lost
    assert SECRET not in lost

    check = failure_message(
        {
            "task_id": "validate",
            "try_number": 3,
            "day": "2026-11-11",
            "today": "2026-11-11",
            "exception_line": "archive check failed for train positions",
        }
    )
    assert check.splitlines()[0] == "🔴 Archive check failed, Nov 11"
    assert "failed 3 times" in check
    assert "check the archive before cutover; tell Claude" in check


def test_compare_skips_partial_retention(tmp_path, capsys):
    day = date(2026, 11, 8)
    left = open_store(str(tmp_path / "a"))
    right = open_store(str(tmp_path / "b"))
    payload = _manifest(4, day)
    payload["partial_by_retention"] = True
    other = _manifest(9, day)
    other["partial_by_retention"] = True
    rel = "metra/StopDelayForecast/date=2026-11-08/_manifest.json"
    left.write_json(rel, payload)
    right.write_json(rel, other)
    code = compare_main(
        ["--a", str(tmp_path / "a"), "--b", str(tmp_path / "b"), "--start", "2026-11-08", "--end", "2026-11-09", "--tables", "metra.StopDelayForecast"]
    )
    out = capsys.readouterr().out
    assert code == 0
    assert "skipped_partial=1" in out
    assert "row_count" not in out.split("summary")[0]


def test_chicago_run_date_falls_back_when_logical_date_is_missing():
    spring = datetime(2027, 3, 14, 6, 30, tzinfo=timezone.utc)
    assert chicago_run_date(None, fallback=spring) == date(2027, 3, 14)
    assert chicago_run_date(None) == datetime.now(ZoneInfo("America/Chicago")).date()


def test_mapped_failure_uses_map_index_for_the_table():
    class TaskInstance:
        map_index = 0
        task_id = "extract_table"
        try_number = 1

    fields = callback_fields({"dag_id": "archive_raw_daily", "task_instance": TaskInstance()})
    assert fields["table"] == ordered_keys()[0]
    assert fields["table"] != "-"
    named = callback_fields({"table": "metra.Positions", "task_instance": TaskInstance()})
    assert named["table"] == "metra.Positions"


def test_external_python_bodies_import_every_name_they_use():
    tree = ast.parse((ROOT / "airflow" / "dags" / "archive_raw_daily.py").read_text())
    bodies = {}

    class Walk(ast.NodeVisitor):
        def visit_FunctionDef(self, node):
            if node.name in {"extract_table", "validate"}:
                bodies[node.name] = node
            self.generic_visit(node)

    Walk().visit(tree)
    assert set(bodies) == {"extract_table", "validate"}
    for name, fn in bodies.items():
        missing = _unbound(fn)
        assert not missing, f"{name} uses {sorted(missing)} without importing them"


def _unbound(fn: ast.FunctionDef) -> set[str]:
    """Names loaded in a function that it neither imports nor defines."""

    imported: set[str] = set()
    defined = {arg.arg for arg in fn.args.args}
    used: set[str] = set()

    class Names(ast.NodeVisitor):
        def visit_Import(self, node):
            for alias in node.names:
                imported.add((alias.asname or alias.name).split(".")[0])

        def visit_ImportFrom(self, node):
            for alias in node.names:
                imported.add(alias.asname or alias.name)

        def visit_FunctionDef(self, node):
            defined.add(node.name)

        def visit_Name(self, node):
            if isinstance(node.ctx, ast.Store):
                defined.add(node.id)
            elif isinstance(node.ctx, ast.Load):
                used.add(node.id)

    for stmt in fn.body:
        Names().visit(stmt)
    return used - defined - imported - set(dir(builtins))
