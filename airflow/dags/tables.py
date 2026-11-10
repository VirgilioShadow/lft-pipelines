"""Registry order for the archive DAG.

The scheduler imports this. It pulls in the registry only, not the database
driver. ``/src/src`` is the copy of this repo inside the Airflow image.
"""

import sys

sys.path.insert(0, "/src/src")

from lft_pipelines.registry import TABLES


def ordered_keys() -> list[str]:
    """Table names, shortest retention first, snapshots after day tables."""

    def rank(table) -> tuple:
        retention = table.retention_days if table.retention_days is not None else 10**9
        snapshot = 1 if table.partition == "snapshot" else 0
        return (retention, snapshot, TABLES.index(table))

    return [table.key for table in sorted(TABLES, key=rank)]
