"""Failure callback for the archive DAG.

The text is built by ``lft_pipelines.alerts``. This module reads
``ALERT_WEBHOOK_URL`` only, and only to choose where to post. No other
environment value is copied into the message.
"""

import os

from lft_pipelines.alerts import failure_message, post_discord

__all__ = ["callback_fields", "failure_message", "on_failure", "post_discord"]

LOG_READ = 40


def on_failure(context) -> None:
    """Airflow failure callback. No-op when ``ALERT_WEBHOOK_URL`` is unset.

    Airflow calls this only after the task's retries are exhausted.
    """

    url = os.environ.get("ALERT_WEBHOOK_URL") or ""
    if not url:
        return
    post_discord(url, failure_message(callback_fields(context)))


def callback_fields(context) -> dict:
    """The fields the alert is allowed to contain. Environment values are not copied."""

    task_instance = context.get("task_instance")
    spec = context.get("spec") if isinstance(context.get("spec"), dict) else {}
    dag = context.get("dag")
    day = spec.get("today") or context.get("day") or "-"
    return {
        "dag_id": getattr(dag, "dag_id", None) or context.get("dag_id"),
        "task_id": getattr(task_instance, "task_id", None) or context.get("task_id"),
        "try_number": getattr(task_instance, "try_number", None) or context.get("try_number"),
        "table": _table_name(context, spec, task_instance),
        "day": day,
        "today": context.get("today") or spec.get("today") or day,
        "exception_line": _exception_line(context, task_instance),
        "log_lines": context.get("log_lines") or _log_tail(task_instance),
    }


def _table_name(context, spec: dict, task_instance) -> str:
    """Table for the alert. Mapped extract tasks use their map index."""

    named = spec.get("table_key") or context.get("table")
    if named:
        return named
    index = getattr(task_instance, "map_index", None)
    if index is None:
        index = context.get("map_index")
    if not isinstance(index, int) or index < 0:
        return "-"
    from tables import ordered_keys

    keys = ordered_keys()
    if index >= len(keys):
        return "-"
    return keys[index]


def _exception_line(context, task_instance) -> str:
    """Last non-empty line of the exception. The traceback stays in the log."""

    exc = context.get("exception")
    if exc is None and task_instance is not None:
        exc = getattr(task_instance, "exception", None)
    if exc is None:
        return ""
    lines = [line.strip() for line in str(exc).splitlines() if line.strip()]
    return lines[-1] if lines else ""


def _log_tail(task_instance) -> list[str]:
    """Recent log lines. The formatter keeps only a few cleaned ones."""

    try_number = getattr(task_instance, "try_number", None)
    if task_instance is None or not hasattr(task_instance, "log_filepath"):
        return []
    path = task_instance.log_filepath
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return handle.read().splitlines()[-LOG_READ:]
    except OSError:
        return [f"log unreadable try={try_number}"]
