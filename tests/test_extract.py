import io
import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pyarrow.parquet as pq

from lft_pipelines.bounds import chicago_day_bounds
from lft_pipelines.extract import extract_partition, main, run_plan
from lft_pipelines.plan import build_plan, format_plan
from lft_pipelines.registry import TABLES, resolve_tables
from lft_pipelines.sql import ensure_select, queries_for
from lft_pipelines.storage import MemoryFilesystem, open_store
from tests.fakes import FakeConn

DAY = date(2026, 1, 15)
WHEN = datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc)


def _description():
    return [
        SimpleNamespace(name="id", type_code=20),
        SimpleNamespace(name="recorded_at", type_code=1184),
        SimpleNamespace(name="label", type_code=25),
    ]


def _rows(count=5):
    stamp = datetime(2026, 1, 15, 18, 0, tzinfo=timezone.utc)
    return [(index, stamp, f"row-{index}") for index in range(count)]


def _positions():
    return resolve_tables("metra.Positions")[0]


def test_chicago_bounds_across_both_dst_changes():
    spring_start, spring_end = chicago_day_bounds(date(2026, 3, 8))
    assert spring_start == datetime(2026, 3, 8, 6, 0, tzinfo=timezone.utc)
    assert spring_end == datetime(2026, 3, 9, 5, 0, tzinfo=timezone.utc)
    assert spring_end - spring_start == timedelta(hours=23)

    fall_start, fall_end = chicago_day_bounds(date(2026, 11, 1))
    assert fall_start == datetime(2026, 11, 1, 5, 0, tzinfo=timezone.utc)
    assert fall_end == datetime(2026, 11, 2, 6, 0, tzinfo=timezone.utc)
    assert fall_end - fall_start == timedelta(hours=25)

    winter_start, winter_end = chicago_day_bounds(date(2026, 1, 15))
    assert winter_start == datetime(2026, 1, 15, 6, 0, tzinfo=timezone.utc)
    assert winter_end - winter_start == timedelta(hours=24)

    summer_start, summer_end = chicago_day_bounds(date(2026, 7, 15))
    assert summer_start == datetime(2026, 7, 15, 5, 0, tzinfo=timezone.utc)
    assert summer_end - summer_start == timedelta(hours=24)


def test_queries_are_selects_with_a_bare_range():
    for table in TABLES:
        (select_sql, select_params), (count_sql, count_params) = queries_for(table, DAY)
        ensure_select(select_sql)
        ensure_select(count_sql)
        assert "AT TIME ZONE" not in select_sql
        assert "date_trunc" not in select_sql
        if table.partition == "snapshot":
            assert "WHERE" not in select_sql
            assert select_params == ()
            continue
        column = f'"{table.column}"'
        assert f"WHERE {column} >= %s AND {column} < %s" in select_sql
        assert select_params == count_params
        if table.column_kind == "date":
            assert select_params == (DAY, DAY + timedelta(days=1))
        else:
            assert select_params[0].tzinfo is not None
            assert select_params[0] < select_params[1]


def test_sql_guard_rejects_writes():
    for statement in (
        "DELETE FROM metra.\"Positions\"",
        "UPDATE metra.\"Positions\" SET id = 1",
        "INSERT INTO metra.\"Positions\" VALUES (1)",
        "WITH gone AS (DELETE FROM metra.\"Positions\" RETURNING 1) SELECT 1",
        "SELECT 1; DELETE FROM metra.\"Positions\"",
    ):
        try:
            ensure_select(statement)
        except ValueError:
            continue
        raise AssertionError(statement)


def test_partition_path_and_manifest_roundtrip(tmp_path):
    table = _positions()
    store = open_store(str(tmp_path))
    conn = FakeConn(_rows(), _description(), count=5)
    result = extract_partition(conn, table, DAY, store, batch_size=2, extracted_at=WHEN)

    parquet = tmp_path / "metra/Positions/date=2026-01-15/part-0.parquet"
    manifest_path = tmp_path / "metra/Positions/date=2026-01-15/_manifest.json"
    assert parquet.is_file()
    assert not parquet.with_name("part-0.parquet.partial").exists()
    assert pq.read_table(parquet).num_rows == 5
    manifest = json.loads(manifest_path.read_text())
    assert result.manifest == manifest
    assert manifest["row_count"] == 5
    assert manifest["db_count"] == 5
    assert manifest["mismatch"] is False
    assert manifest["min_timestamp"] == "2026-01-15T18:00:00+00:00"
    assert manifest["max_timestamp"] == "2026-01-15T18:00:00+00:00"
    assert manifest["extracted_at"] == WHEN.isoformat()
    assert manifest["tool_version"] == "0.1.0"
    assert {"name": "recorded_at", "type": "timestamp[us, tz=UTC]"} in manifest["columns"]
    assert conn.fetch_sizes == [2, 2, 2, 2]
    assert conn.cursor_names[0] == "lft_extract"
    assert all(sql.lower().startswith("select") for sql, _params in conn.statements)


def test_mismatch_is_recorded(tmp_path):
    table = _positions()
    store = open_store(str(tmp_path))
    conn = FakeConn(_rows(2), _description(), count=9)
    result = extract_partition(conn, table, DAY, store, batch_size=10, extracted_at=WHEN)
    assert result.manifest["row_count"] == 2
    assert result.manifest["db_count"] == 9
    assert result.manifest["mismatch"] is True


def test_skip_and_overwrite(tmp_path):
    table = _positions()
    store = open_store(str(tmp_path))
    first = FakeConn(_rows(1), _description(), count=1)
    extract_partition(first, table, DAY, store, extracted_at=WHEN)
    second = FakeConn(_rows(4), _description(), count=4)
    skipped = extract_partition(second, table, DAY, store, extracted_at=WHEN)
    assert skipped.status == "skipped"
    assert second.statements == []
    third = FakeConn(_rows(4), _description(), count=4)
    written = extract_partition(third, table, DAY, store, overwrite=True, extracted_at=WHEN)
    assert written.status == "wrote"
    assert written.manifest["row_count"] == 4
    assert pq.read_table(tmp_path / "metra/Positions/date=2026-01-15/part-0.parquet").num_rows == 4


def test_s3_writes_partial_then_copies(tmp_path):
    del tmp_path
    table = _positions()
    filesystem = MemoryFilesystem()
    store = open_store("s3://archive-bucket/history", filesystem=filesystem)
    conn = FakeConn(_rows(1), _description(), count=1)
    extract_partition(conn, table, DAY, store, extracted_at=WHEN)
    final = "archive-bucket/history/metra/Positions/date=2026-01-15/part-0.parquet"
    partial = final + ".partial"
    assert ("copy", partial, final) in filesystem.operations
    copy_at = filesystem.operations.index(("copy", partial, final))
    delete_at = filesystem.operations.index(("delete", partial))
    assert copy_at < delete_at
    assert partial not in filesystem.data
    table_out = pq.read_table(io.BytesIO(filesystem.data[final]))
    assert table_out.num_rows == 1
    manifest_key = "archive-bucket/history/metra/Positions/date=2026-01-15/_manifest.json"
    manifest = json.loads(filesystem.data[manifest_key])
    assert manifest["mismatch"] is False


def test_at_risk_orders_short_retention_and_oldest_days_first():
    tables = resolve_tables(
        "metra.StopDelayForecast,metra.Positions,metra.TripUpdateSnapshot,metra.Trips"
    )
    today = date(2026, 1, 4)
    plan = build_plan(tables, date(2026, 1, 1), date(2026, 1, 4), at_risk=True, today=today)
    keys = [(item.table.key, item.day.isoformat()) for item in plan]
    assert keys == [
        ("metra.StopDelayForecast", "2026-01-02"),
        ("metra.StopDelayForecast", "2026-01-03"),
        ("metra.Positions", "2026-01-01"),
        ("metra.Positions", "2026-01-02"),
        ("metra.Positions", "2026-01-03"),
        ("metra.TripUpdateSnapshot", "2026-01-01"),
        ("metra.TripUpdateSnapshot", "2026-01-02"),
        ("metra.TripUpdateSnapshot", "2026-01-03"),
        ("metra.Trips", "2026-01-04"),
    ]


def test_dry_run_prints_the_plan_and_does_not_query(tmp_path, capsys, monkeypatch):
    def explode(*_args, **_kwargs):
        raise AssertionError("dry-run must not connect")

    monkeypatch.setattr("lft_pipelines.extract.connect", explode)
    out = tmp_path / "not-created"
    code = main(
        [
            "--tables",
            "all",
            "--start",
            "2026-11-01",
            "--end",
            "2026-11-04",
            "--out",
            str(out),
            "--dry-run",
        ]
    )
    assert code == 0
    assert not out.exists()
    text = capsys.readouterr().out
    assert text.startswith("partitions=38 dry-run")
    assert "metra.ServiceAlert\tretention=none\tpartition=snapshot" in text
    assert "metra.Positions" in text
    assert "date=2026-11-01" in text
    assert "date=2026-11-03" in text
    assert "metra.Trips" in text
    assert "date=2026-11-04" in text
    assert "s3://" not in text
    plan = build_plan(TABLES, date(2026, 11, 1), date(2026, 11, 4), at_risk=False, today=date(2026, 11, 9))
    assert format_plan(str(out), plan) == text.rstrip("\n")


def test_settle_guard_lists_today_and_the_bucket_lag():
    today = date(2026, 11, 9)
    positions = resolve_tables("metra.Positions")
    plan = build_plan(positions, date(2026, 11, 8), date(2026, 11, 10), at_risk=False, today=today)
    assert [(item.day, item.status) for item in plan] == [
        (date(2026, 11, 8), "extract"),
        (date(2026, 11, 9), "not_settled"),
    ]
    assert "date=2026-11-09\tnot settled" in format_plan("/tmp/archive", plan)

    buckets = resolve_tables("metra.OnTimeByTodDailyBucket,metra.HourlySpeedBinDailyBucket")
    assert [table.settle_days for table in buckets] == [1, 1]
    plan = build_plan(buckets, date(2026, 11, 7), date(2026, 11, 10), at_risk=False, today=today)
    statuses = {(item.table.table, item.day): item.status for item in plan}
    for name in ("OnTimeByTodDailyBucket", "HourlySpeedBinDailyBucket"):
        assert statuses[(name, date(2026, 11, 7))] == "extract"
        assert statuses[(name, date(2026, 11, 8))] == "not_settled"
        assert statuses[(name, date(2026, 11, 9))] == "not_settled"
        assert f"{name}\tretention=none\tpartition=day" in format_plan("/tmp/archive", plan)
    assert format_plan("/tmp/archive", plan).count("not settled") == 4


def test_retention_boundary_day_stays_and_is_flagged(tmp_path):
    today = date(2026, 11, 9)
    stop = resolve_tables("metra.StopDelayForecast")
    plan = build_plan(stop, date(2026, 11, 7), date(2026, 11, 9), at_risk=True, today=today)
    assert [(item.day, item.status, item.partial_by_retention) for item in plan] == [
        (date(2026, 11, 7), "extract", True),
        (date(2026, 11, 8), "extract", False),
    ]
    text = format_plan(str(tmp_path), plan)
    assert "date=2026-11-07\textract\tpartial_by_retention=true" in text
    description = [SimpleNamespace(name="service_date", type_code=1082)]
    store = open_store(str(tmp_path))
    run_plan(
        FakeConn([], description, count=0),
        plan,
        store,
        pause_seconds=0,
        extracted_at=WHEN,
    )
    boundary = json.loads((tmp_path / "metra/StopDelayForecast/date=2026-11-07/_manifest.json").read_text())
    later = json.loads((tmp_path / "metra/StopDelayForecast/date=2026-11-08/_manifest.json").read_text())
    assert boundary["partial_by_retention"] is True
    assert later["partial_by_retention"] is False

    positions = resolve_tables("metra.Positions")
    edge = build_plan(positions, date(2026, 10, 10), date(2026, 10, 11), at_risk=False, today=today)
    assert edge[0].partial_by_retention is True
    gps = tmp_path / "gps"
    run_plan(
        FakeConn([], _description(), count=0, group_rows=[]),
        edge,
        open_store(str(gps)),
        pause_seconds=0,
        extracted_at=WHEN,
    )
    gps_manifest = json.loads((gps / "metra/Positions/date=2026-10-10/_manifest.json").read_text())
    assert gps_manifest["partial_by_retention"] is True


def test_range_scan_routes_a_dst_day_and_skips_existing(tmp_path, capsys):
    today = date(2026, 3, 20)
    plan = build_plan(
        resolve_tables("metra.Positions"),
        date(2026, 3, 8),
        date(2026, 3, 10),
        at_risk=False,
        today=today,
    )
    assert [item.day for item in plan] == [date(2026, 3, 8), date(2026, 3, 9)]
    rows = [
        (1, datetime(2026, 3, 8, 6, 30, tzinfo=timezone.utc), "march-8"),
        (2, datetime(2026, 3, 9, 4, 30, tzinfo=timezone.utc), "still-march-8"),
        (3, datetime(2026, 3, 9, 5, 30, tzinfo=timezone.utc), "march-9"),
        (4, None, "null-row"),
    ]
    grouped = [(date(2026, 3, 8), 2), (date(2026, 3, 9), 1)]
    conn = FakeConn(rows, _description(), count=0, group_rows=grouped)
    run_plan(conn, plan, open_store(str(tmp_path)), pause_seconds=0, batch_size=2, extracted_at=WHEN)
    day8 = pq.read_table(tmp_path / "metra/Positions/date=2026-03-08/part-0.parquet")
    day9 = pq.read_table(tmp_path / "metra/Positions/date=2026-03-09/part-0.parquet")
    assert day8.column("label").to_pylist() == ["march-8", "still-march-8"]
    assert day9.column("label").to_pylist() == ["march-9"]
    assert "null-row" not in day8.column("label").to_pylist()
    out = capsys.readouterr().out
    assert "null_partition_rows=1 table=metra.Positions" in out
    assert "[1–2/2] metra.Positions 2026-03-08..2026-03-09 (one scan)" in out
    data_sql = next(sql for sql, _params in conn.statements if "group by" not in sql.lower())
    count_sql = next(sql for sql, _params in conn.statements if "group by" in sql.lower())
    assert "AT TIME ZONE" not in data_sql
    assert "AT TIME ZONE 'America/Chicago'" in count_sql
    assert conn.cursor_names[0] == "lft_extract"
    assert conn.cursor_names.count("lft_extract") == 1
    manifest = json.loads((tmp_path / "metra/Positions/date=2026-03-08/_manifest.json").read_text())
    assert manifest["row_count"] == 2
    assert manifest["db_count"] == 2
    assert manifest["partial_by_retention"] is False

    again = FakeConn(rows, _description(), count=0, group_rows=grouped)
    run_plan(again, plan, open_store(str(tmp_path)), pause_seconds=0, extracted_at=WHEN)
    assert again.statements == []

    fresh = tmp_path / "second"
    store = open_store(str(fresh))
    store.write_json("metra/Positions/date=2026-03-08/_manifest.json", {"sentinel": True})
    third = FakeConn(rows, _description(), count=0, group_rows=grouped)
    run_plan(third, plan, store, pause_seconds=0, batch_size=2, extracted_at=WHEN)
    kept = json.loads((fresh / "metra/Positions/date=2026-03-08/_manifest.json").read_text())
    assert kept == {"sentinel": True}
    assert not (fresh / "metra/Positions/date=2026-03-08/part-0.parquet").exists()
    assert pq.read_table(fresh / "metra/Positions/date=2026-03-09/part-0.parquet").num_rows == 1
    assert len(third.statements) == 2


def test_service_alert_is_planned_as_a_snapshot():
    table = resolve_tables("metra.ServiceAlert")[0]
    assert table.partition == "snapshot"
    plan = build_plan((table,), date(2026, 11, 1), date(2026, 11, 4), at_risk=False, today=date(2026, 11, 9))
    assert [(item.day, item.status) for item in plan] == [(date(2026, 11, 4), "extract")]
    (select_sql, params), _count = queries_for(table, plan[0].day)
    assert "WHERE" not in select_sql
    assert params == ()
    assert "partition=snapshot" in format_plan("/tmp/archive", plan)
