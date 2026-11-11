"""Row-band warnings: same day type, once per new partition, no zero median."""

from datetime import date

import pytest

from lft_pipelines.registry import resolve_tables
from lft_pipelines.validate import (
    format_validation,
    fresh_days_from_extracts,
    require_night_ok,
    validate_table,
)

TODAY = date(2026, 11, 11)
START = date(2026, 11, 4)
END = date(2026, 11, 11)


def _manifest(rows, day, columns=None):
    return {
        "row_count": rows,
        "db_count": rows,
        "mismatch": False,
        "min_timestamp": f"{day.isoformat()}T18:00:00+00:00",
        "max_timestamp": f"{day.isoformat()}T20:00:00+00:00",
        "columns": columns if columns is not None else [{"name": "id", "type": "int64"}],
        "extracted_at": "2026-11-10T06:00:00+00:00",
        "tool_version": "0.1.0",
    }


def _fill(spec: dict[date, int], columns=None) -> dict[date, dict]:
    return {day: _manifest(rows, day, columns) for day, rows in spec.items()}


def test_weekend_median_ignores_a_weekday_baseline():
    table = resolve_tables("metra.Positions")[0]
    weekdays = [
        date(2026, 10, 28),
        date(2026, 10, 29),
        date(2026, 10, 30),
        date(2026, 11, 3),
        date(2026, 11, 4),
        date(2026, 11, 5),
        date(2026, 11, 6),
        date(2026, 11, 9),
        date(2026, 11, 10),
    ]
    weekends = [
        date(2026, 10, 24),
        date(2026, 10, 25),
        date(2026, 10, 31),
        date(2026, 11, 1),
        date(2026, 11, 7),
        date(2026, 11, 8),
    ]
    counts = {day: 100 for day in weekdays}
    counts.update({day: 40 for day in weekends})
    quiet = validate_table(table, TODAY, _fill(counts), start=START, end=END)
    assert not any(item.code == "row_band" for item in quiet.findings)

    counts[date(2026, 11, 8)] = 10
    sunday = validate_table(
        table, TODAY, _fill(counts), start=START, end=END, fresh_days={date(2026, 11, 8)}
    )
    assert sunday.digest_warnings[0].message == "train positions, Nov 8: 75% fewer rows than the weekend median"

    counts[date(2026, 11, 6)] = 40
    friday = validate_table(table, TODAY, _fill(counts), start=START, end=END)
    friday_band = next(item.message for item in friday.findings if item.day == date(2026, 11, 6) and item.code == "row_band")
    assert friday_band == "train positions, Nov 6: 60% fewer rows than the weekday median"


def test_previous_run_is_logged_and_left_out_of_the_digest(capsys):
    table = resolve_tables("metra.Positions")[0]
    counts = {date(2026, 11, day): 100 for day in (3, 4, 5, 6, 9, 10)}
    counts[date(2026, 11, 7)] = 40
    counts[date(2026, 11, 8)] = 40
    counts[date(2026, 11, 9)] = 10
    result = validate_table(
        table,
        TODAY,
        _fill(counts),
        start=date(2026, 11, 3),
        end=END,
        fresh_days={date(2026, 11, 10)},
    )
    print(format_validation(result))
    log = capsys.readouterr().out
    assert "train positions, Nov 9: 90% fewer rows than the weekday median" in log
    assert result.digest_warnings == []
    assert result.ok


def test_zero_median_and_irregular_producer_skip_row_band():
    positions = resolve_tables("metra.Positions")[0]
    outage = {date(2026, 11, day): 0 for day in (3, 4, 5, 6)}
    outage[date(2026, 11, 9)] = 100
    recovered = validate_table(positions, TODAY, _fill(outage), start=date(2026, 11, 3), end=END)
    assert not any(item.code == "row_band" for item in recovered.findings)

    forecasts = resolve_tables("metra.DelayForecast")[0]
    stop = resolve_tables("metra.StopDelayForecast")[0]
    assert forecasts.row_band is False
    assert stop.row_band is False
    assert positions.row_band is True
    swung = {date(2026, 11, day): 100 for day in (3, 4, 5)}
    swung[date(2026, 11, 6)] = 300
    manifests = _fill(swung)
    manifests[date(2026, 11, 6)] = _manifest(300, date(2026, 11, 6), columns=[{"name": "other", "type": "int64"}])
    result = validate_table(forecasts, TODAY, manifests, start=START, end=END)
    assert not any(item.code == "row_band" for item in result.findings)
    assert any(item.code == "columns" for item in result.findings)


def test_old_missing_partition_still_fails_the_night():
    table = resolve_tables("metra.Positions")[0]
    counts = {date(2026, 11, day): 100 for day in (5, 6, 7, 8, 9, 10)}
    result = validate_table(
        table, TODAY, _fill(counts), start=START, end=END, fresh_days={date(2026, 11, 10)}
    )
    assert any(item.code == "missing" and item.day == date(2026, 11, 4) for item in result.findings)
    assert result.digest_warnings == []
    with pytest.raises(RuntimeError, match="archive check failed for train positions"):
        require_night_ok([table.label] if not result.ok else [])


def test_extract_written_days_ignore_a_failed_task():
    found = fresh_days_from_extracts(
        [None, {"table": "metra.Positions", "written": ["2026-11-10"]}, {"wrote": 0}]
    )
    assert found == {"metra.Positions": {date(2026, 11, 10)}}


def test_nov_4_11_false_alarms_post_nothing():
    """The eight warnings from the first nightly digest, rebuilt as manifests.

    Sun Nov 8 and Sat Nov 7 were low against a weekday baseline: positions
    3900 vs 10000 (61%), trip updates 4300 (57%), hourly bins 4600 (54%).
    CTA trip updates then had a 100-row day after zeros. DelayForecast on
    Nov 6 was 300 against 100 (200% more). This run wrote only Nov 10.
    """

    weekdays = [
        date(2026, 10, 28),
        date(2026, 10, 29),
        date(2026, 10, 30),
        date(2026, 11, 3),
        date(2026, 11, 4),
        date(2026, 11, 5),
        date(2026, 11, 6),
        date(2026, 11, 9),
        date(2026, 11, 10),
    ]
    weekends = [
        date(2026, 10, 24),
        date(2026, 10, 25),
        date(2026, 10, 31),
        date(2026, 11, 1),
        date(2026, 11, 7),
        date(2026, 11, 8),
    ]
    fresh = {date(2026, 11, 10)}
    bullets = []
    cases = [
        ("metra.Positions", 10000, 3900),
        ("metra.TripUpdateSnapshot", 10000, 4300),
        ("metra.HourlySpeedBinDailyBucket", 10000, 4600),
    ]
    for key, weekday_rows, weekend_rows in cases:
        counts = {day: weekday_rows for day in weekdays}
        counts.update({day: weekend_rows for day in weekends})
        result = validate_table(
            resolve_tables(key)[0], TODAY, _fill(counts), start=START, end=END, fresh_days=fresh
        )
        assert not any(item.code == "row_band" for item in result.findings)
        bullets.extend(item.message for item in result.digest_warnings)

    cta = {date(2026, 11, day): 0 for day in (3, 4, 5, 6, 7, 8)}
    cta[date(2026, 11, 9)] = 100
    cta[date(2026, 11, 10)] = 100
    cta_result = validate_table(
        resolve_tables("cta.CtaTripUpdateSnapshot")[0],
        TODAY,
        _fill(cta),
        start=date(2026, 11, 3),
        end=END,
        fresh_days=fresh,
    )
    assert not any(item.code == "row_band" for item in cta_result.findings)
    bullets.extend(item.message for item in cta_result.digest_warnings)

    forecast_counts = {date(2026, 11, day): 100 for day in (3, 4, 5, 7, 8, 9, 10)}
    forecast_counts[date(2026, 11, 6)] = 300
    forecast = validate_table(
        resolve_tables("metra.DelayForecast")[0],
        TODAY,
        _fill(forecast_counts),
        start=date(2026, 11, 3),
        end=END,
        fresh_days=fresh,
    )
    assert not any(item.code == "row_band" for item in forecast.findings)
    bullets.extend(item.message for item in forecast.digest_warnings)
    assert bullets == []
