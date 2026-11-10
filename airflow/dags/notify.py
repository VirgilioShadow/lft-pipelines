"""Failure and warning posts for the archive DAG.

Stdlib only. The message names the dag, task, try, table and day, plus the
last log lines. It never reads the process environment, so a secret in the
environment cannot land in Discord.
"""

import json
import os
import urllib.request

MAX_CHARS = 1800
LOG_LINES = 15


def failure_message(context: dict) -> str:
    """Text for one task failure. ``context`` is a plain dict, not an Airflow object."""

    table = context.get("table") or "-"
    day = context.get("day") or "-"
    lines = context.get("log_lines") or []
    tail = "\n".join(str(line) for line in lines[-LOG_LINES:])
    text = (
        f"dag={context.get('dag_id')} task={context.get('task_id')} "
        f"try={context.get('try_number')} table={table} day={day}\n{tail}"
    )
    if len(text) > MAX_CHARS:
        text = text[: MAX_CHARS - 3] + "..."
    return text


def post_discord(url: str, text: str) -> None:
    """POST ``{"content": text}``. No-op when ``url`` is empty."""

    if not url:
        return
    body = json.dumps({"content": text[:MAX_CHARS]}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def on_failure(context) -> None:
    """Airflow failure callback. No-op when ``ALERT_WEBHOOK_URL`` is unset."""

    url = os.environ.get("ALERT_WEBHOOK_URL") or ""
    if not url:
        return
    post_discord(url, failure_message(callback_fields(context)))


def callback_fields(context) -> dict:
    """The fields the alert is allowed to contain. Environment values are not copied."""

    task_instance = context.get("task_instance")
    spec = context.get("spec") if isinstance(context.get("spec"), dict) else {}
    dag = context.get("dag")
    return {
        "dag_id": getattr(dag, "dag_id", None) or context.get("dag_id"),
        "task_id": getattr(task_instance, "task_id", None) or context.get("task_id"),
        "try_number": getattr(task_instance, "try_number", None) or context.get("try_number"),
        "table": _table_name(context, spec, task_instance),
        "day": spec.get("today") or context.get("day") or "-",
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


def _log_tail(task_instance) -> list[str]:
    try_number = getattr(task_instance, "try_number", None)
    if task_instance is None or not hasattr(task_instance, "log_filepath"):
        exception = getattr(task_instance, "exception", None) if task_instance else None
        return [] if exception is None else [str(exception)]
    path = task_instance.log_filepath
    try:
        with open(path, encoding="utf-8", errors="replace") as handle:
            return handle.read().splitlines()[-LOG_LINES:]
    except OSError:
        return [f"log unreadable try={try_number}"]
