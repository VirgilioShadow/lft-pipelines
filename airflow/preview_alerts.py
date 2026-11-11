"""Print three sample Discord alerts. ``--post`` sends them with a ``[TEST]`` prefix.

Run from a checkout, on the Droplet or a laptop. The default path does not
touch the network. ``--post`` reads ``ALERT_WEBHOOK_URL`` and nothing else.
"""

import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from lft_pipelines.alerts import failure_message, post_discord, warning_message  # noqa: E402


def sample_messages() -> list[str]:
    """A pruned-table failure, an unpruned-table failure, and one warning digest."""

    log = ["\x1b[31m[2026-11-11T06:31:04Z] connection to the database was refused\x1b[0m"]
    pruned = failure_message(
        {
            "task_id": "extract_table",
            "try_number": 3,
            "table": "metra.Positions",
            "day": "2026-11-11",
            "today": "2026-11-11",
            "exception_line": "connection to the database was refused",
            "log_lines": log,
        }
    )
    kept = failure_message(
        {
            "task_id": "extract_table",
            "try_number": 3,
            "table": "metra.SpeedObservation",
            "day": "2026-11-11",
            "today": "2026-11-11",
            "exception_line": "connection to the database was refused",
            "log_lines": log,
        }
    )
    warnings = warning_message(
        date(2026, 11, 11),
        [
            "train positions, Nov 9: 41% fewer rows than the 7-day median",
            "service alerts, Nov 11: the columns changed from the previous copy",
        ],
    )
    return [pruned, kept, warnings]


def main(argv: list[str]) -> int:
    """Print the samples. Post them only when ``--post`` is set and the URL is present."""

    messages = sample_messages()
    for text in messages:
        print(text)
        print()
    if "--post" not in argv:
        return 0
    url = os.environ.get("ALERT_WEBHOOK_URL") or ""
    if not url:
        print("ALERT_WEBHOOK_URL is unset; nothing posted", file=sys.stderr)
        return 1
    for text in messages:
        post_discord(url, f"[TEST] {text}")
    print("posted 3 test messages")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
