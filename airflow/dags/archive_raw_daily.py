"""Nightly archive extract. One table at a time, then validate, then a heartbeat.

The scheduler imports only the registry (no pinned driver). Extract and
validate run in /opt/lft-venv so Airflow's constraints never move those pins.
"""

import os
import sys
import urllib.request
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, "/src/src")

from airflow.sdk import dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

from notify import on_failure
from tables import ordered_keys
from window import chicago_run_date, lookback_window

PYTHON = "/opt/lft-venv/bin/python"


@dag(
    dag_id="archive_raw_daily",
    schedule=CronTriggerTimetable("30 0 * * *", timezone="America/Chicago"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=90),
        "on_failure_callback": on_failure,
    },
)
def archive_raw_daily():
    """Extract a 7-day lookback ending on the Chicago date of this run."""

    @task
    def plan_run() -> list[dict]:
        """One spec per table. The dates are the same; the pool runs them one by one."""

        ctx = get_current_context()
        dag_run = ctx.get("dag_run")
        today = chicago_run_date(ctx.get("logical_date"), fallback=getattr(dag_run, "run_after", None))
        start, end = lookback_window(today)
        return [
            {
                "table_key": key,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "today": today.isoformat(),
            }
            for key in ordered_keys()
        ]

    @task.external_python(
        python=PYTHON,
        pool="lft_db",
        max_active_tis_per_dagrun=1,
        expect_airflow=False,
    )
    def extract_table(spec: dict) -> dict:
        """Extract one table. Returns counts only, not row payloads.

        Imports stay inside this function. Only its source is sent to the venv.
        """

        import os
        from datetime import date

        from lft_pipelines.execute import run_table
        from lft_pipelines.extract import connect
        from lft_pipelines.registry import resolve_tables
        from lft_pipelines.storage import open_store

        table = resolve_tables(spec["table_key"])[0]
        store = open_store(os.environ["ARCHIVE_URL"])
        conn = connect()
        try:
            results = run_table(
                conn,
                table,
                store,
                start=date.fromisoformat(spec["start"]),
                end=date.fromisoformat(spec["end"]),
                today=date.fromisoformat(spec["today"]),
                pause_seconds=2,
                at_risk=True,
                quiet=True,
            )
        finally:
            conn.close()
        return {
            "table": table.key,
            "wrote": sum(1 for item in results if item.status == "wrote"),
            "written": [item.day.isoformat() for item in results if item.status == "wrote"],
            "skipped": sum(1 for item in results if item.status == "skipped"),
            "not_settled": sum(1 for item in results if item.status == "not_settled"),
            "mismatches": sum(1 for item in results if item.manifest and item.manifest.get("mismatch")),
        }

    @task.external_python(python=PYTHON, trigger_rule="all_done", expect_airflow=False)
    def validate(specs: list[dict], extracted: list[dict]) -> dict:
        """Fail when a settled manifest is missing or mismatched. Warn once per new partition.

        Imports stay inside this function. Only its source is sent to the venv.
        """

        import os
        from datetime import date

        from lft_pipelines.alerts import post_discord, warning_message
        from lft_pipelines.registry import resolve_tables
        from lft_pipelines.storage import open_store
        from lft_pipelines.validate import format_validation, fresh_days_from_extracts, require_night_ok, validate_table

        if not specs:
            raise RuntimeError("no table specs")
        today = date.fromisoformat(specs[0]["today"])
        start = date.fromisoformat(specs[0]["start"])
        end = date.fromisoformat(specs[0]["end"])
        store = open_store(os.environ["ARCHIVE_URL"])
        fresh = fresh_days_from_extracts(extracted)
        warnings = []
        failed_names = []
        for spec in specs:
            table = resolve_tables(spec["table_key"])[0]
            found = dict(store.list_manifests(table.schema, table.table, start, end))
            if table.partition == "snapshot":
                payload = store.read_json(
                    f"{table.schema}/{table.table}/date={today.isoformat()}/_manifest.json"
                )
                if payload is not None:
                    found[today] = payload
            result = validate_table(table, today, found, start=start, end=end, fresh_days=fresh.get(table.key, set()))
            print(format_validation(result))
            if not result.ok:
                failed_names.append(table.label)
            warnings.extend(item.message for item in result.digest_warnings)
        url = os.environ.get("ALERT_WEBHOOK_URL") or ""
        if warnings and url:
            post_discord(url, warning_message(today, warnings))
        require_night_ok(failed_names)
        return {"warnings": len(warnings)}

    @task
    def heartbeat(checked: dict) -> None:
        """Ping only after a successful validate. No-op when the URL is unset."""

        del checked
        url = os.environ.get("HEALTHCHECK_URL") or ""
        if not url:
            print("heartbeat: HEALTHCHECK_URL unset")
            return
        urllib.request.urlopen(url, timeout=10).read()

    specs = plan_run()
    extracted = extract_table.expand(spec=specs)
    checked = validate(specs, extracted)
    beat = heartbeat(checked)
    beat.set_upstream(extracted)


archive_raw_daily()
