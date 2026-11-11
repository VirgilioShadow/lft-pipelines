"""Discord text for archive failures and the nightly warning digest.

Stdlib only. Message builders do not read the process environment. The
caller passes the webhook URL in when it is time to post.
"""

import json
import re
import urllib.request
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

MAX_CHARS = 1800
DETAIL_LINES = 5
USER_AGENT = "lft-pipelines-alerts/1.0 (+https://github.com/VirgilioShadow/lft-pipelines)"
_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_BRACKET_STAMP = re.compile(r"^\[[^\]]*20\d{2}[^\]]*\]\s*")
_ISO_STAMP = re.compile(r"^20\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?\s*")

SINGLE_FAILURE_ACTION = "nothing tonight; if this repeats tomorrow, tell Claude"
VALIDATION_ACTION = "check the archive before cutover; tell Claude"


def friendly_date(day: date) -> str:
    """Month and day without a leading zero, such as ``Nov 11``."""

    return f"{_MONTHS[day.month - 1]} {day.day}"


def failure_message(context: dict) -> str:
    """Readable failure post. ``context`` is a plain dict, not an Airflow object.

    ``day`` is the Chicago date in the headline. ``today`` decides whether that
    day is already past retention. When ``today`` is omitted, the clock is
    America/Chicago.
    """

    task_id = str(context.get("task_id") or "")
    table_key = str(context.get("table") or "")
    day = _parse_day(context.get("day"))
    today = _parse_day(context.get("today")) or _chicago_today()
    tries = _tries(context.get("try_number"))
    when = friendly_date(day) if day else "unknown date"
    headline = _failure_headline(task_id, table_key, when)
    happened = _happened(task_id, table_key, tries, str(context.get("exception_line") or "").strip())
    risk = _risk(task_id, table_key, day, today)
    action = VALIDATION_ACTION if task_id == "validate" else SINGLE_FAILURE_ACTION
    footer = [
        f"What happened: {happened}",
        f"Risk: {risk}",
        f"What to do: {action}",
    ]
    details = _detail_lines(context.get("log_lines") or [])
    if details == [str(context.get("exception_line") or "").strip()]:
        details = []
    return _assemble(headline, [], footer, details)


def warning_message(run_day: date | str, bullets: list[str]) -> str:
    """One nightly digest. ``bullets`` are already plain sentences from validate."""

    day = run_day if isinstance(run_day, date) else _parse_day(run_day)
    when = friendly_date(day) if day else str(run_day)
    count = len(bullets)
    noun = "warning" if count == 1 else "warnings"
    headline = f"🟡 Archive check: {count} {noun} for {when}"
    middle = [f"- {item}" for item in bullets]
    footer = [
        f"What happened: The nightly archive check found {count} {noun}.",
        "Risk: none, a warning does not remove rows.",
        f"What to do: {VALIDATION_ACTION}",
    ]
    return _assemble(headline, middle, footer, [])


def post_discord(url: str, text: str) -> None:
    """POST ``{"content": text}``. No-op when ``url`` is empty."""

    if not url:
        return
    body = json.dumps({"content": text[:MAX_CHARS]}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        # Discord's edge returns 403 to the default "Python-urllib" agent.
        headers={"content-type": "application/json", "user-agent": USER_AGENT},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def clean_log_line(line: str) -> str:
    """Drop ANSI color codes and a leading timestamp. The rest of the line stays."""

    text = _ANSI.sub("", str(line)).strip()
    previous = None
    while text and text != previous:
        previous = text
        text = _ISO_STAMP.sub("", _BRACKET_STAMP.sub("", text)).strip()
    return text


def _failure_headline(task_id: str, table_key: str, when: str) -> str:
    """First line. A phone notification has to make sense with nothing under it."""

    if task_id == "validate":
        return f"🔴 Archive check failed, {when}"
    if table_key and table_key != "-":
        return f"🔴 Nightly archive failed: {_label(table_key)} ({table_key}), {when}"
    return f"🔴 Nightly archive failed, {when}"


def _happened(task_id: str, table_key: str, tries: int | None, exception_line: str) -> str:
    """One sentence, then the last exception line when there is one."""

    times = f"failed {tries} times" if tries else "failed after its retries"
    if task_id == "validate":
        sentence = f"The archive check {times}."
    elif table_key and table_key != "-":
        sentence = f"The nightly extract of {_label(table_key)} {times}."
    else:
        sentence = f"The nightly archive {times}."
    if exception_line:
        return f"{sentence} Last error: {exception_line}"
    return sentence


def _risk(task_id: str, table_key: str, day: date | None, today: date) -> str:
    """Retention sentence. Validation has no single table, so it says so."""

    if task_id == "validate":
        return "the check covers every table; this alert did not delete rows"
    table = _spec(table_key)
    if table is None or table.retention_days is None or day is None:
        if table is None and table_key not in {"", "-"}:
            return "unknown table, so retention was not checked"
        return "none, the database keeps this table"
    safe = day + timedelta(days=table.retention_days)
    if safe < today:
        return (
            f"the {_label(table_key)} rows for {friendly_date(day)} are already past "
            f"the {table.retention_days}-day keep window (ended {friendly_date(safe)}). "
            "That data is lost unless it was already archived."
        )
    return f"safe until {friendly_date(safe)}; tonight's run retries it"


def _label(table_key: str) -> str:
    """Friendly name. Unknown keys fall back to the key itself."""

    table = _spec(table_key)
    return table.label if table is not None else table_key


def _spec(table_key: str):
    """Registry row for one ``schema.table`` key, or None."""

    if not table_key or table_key == "-":
        return None
    from lft_pipelines.registry import resolve_tables

    try:
        found = resolve_tables(table_key)
    except ValueError:
        return None
    if len(found) != 1:
        return None
    return found[0]


def _detail_lines(lines: list) -> list[str]:
    """Last few cleaned log lines. Blank lines are dropped."""

    cleaned = [clean_log_line(line) for line in lines]
    kept = [line for line in cleaned if line]
    return kept[-DETAIL_LINES:]


def _assemble(headline: str, middle: list[str], footer: list[str], details: list[str]) -> str:
    """Join the parts and stay under ``MAX_CHARS``. Details go first, then bullets.

    The headline is never shortened unless it alone exceeds the cap.
    """

    def render(mids: list[str], dets: list[str]) -> str:
        parts = [headline, *mids, *footer]
        if dets:
            parts.append("```")
            parts.extend(dets)
            parts.append("```")
        return "\n".join(parts)

    text = render(middle, details)
    if len(text) <= MAX_CHARS:
        return text
    dets = list(details)
    while dets and len(render(middle, dets)) > MAX_CHARS:
        dets.pop()
    if len(render(middle, dets)) <= MAX_CHARS:
        return render(middle, dets)
    mids = list(middle)
    while mids and len(render(mids, [])) > MAX_CHARS:
        mids.pop()
    if len(render(mids, [])) <= MAX_CHARS:
        return render(mids, [])
    room = MAX_CHARS - len(headline) - 1
    if room <= 0:
        return headline[:MAX_CHARS]
    return headline + "\n" + "\n".join(footer)[:room]


def _parse_day(value) -> date | None:
    """ISO date, or None when the field is missing or not a date."""

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str) or value in {"", "-"}:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _tries(value) -> int | None:
    """Attempt count when it is a positive integer. Strings of digits count."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isdigit() and int(value) > 0:
        return int(value)
    return None


def _chicago_today() -> date:
    """Today's calendar date in America/Chicago."""

    return datetime.now(ZoneInfo("America/Chicago")).date()
