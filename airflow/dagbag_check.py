"""Import the archive DAG inside the Airflow image. No database and no secrets."""

import os
import sys

from cryptography.fernet import Fernet

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__EXECUTOR", "LocalExecutor")
os.environ.setdefault("AIRFLOW__CORE__FERNET_KEY", Fernet.generate_key().decode())
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:////tmp/airflow-dagbag.db")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags")

from airflow.dag_processing.dagbag import DagBag
from airflow.timetables.trigger import CronTriggerTimetable

sys.path.insert(0, "/src/src")
bag = DagBag(dag_folder="/opt/airflow/dags")
if bag.import_errors:
    for name, error in bag.import_errors.items():
        print(f"import error {name}: {error}")
    raise SystemExit(1)

dag = bag.dags["archive_raw_daily"]
expected = {"plan_run", "extract_table", "validate", "heartbeat"}
missing = expected - set(dag.task_dict)
if missing:
    print(f"missing tasks {sorted(missing)} have {sorted(dag.task_dict)}")
    raise SystemExit(1)
if dag.task_dict["extract_table"].pool != "lft_db":
    print(f"pool {dag.task_dict['extract_table'].pool}")
    raise SystemExit(1)
if not isinstance(dag.timetable, CronTriggerTimetable):
    print(type(dag.timetable))
    raise SystemExit(1)
print("dagbag ok", sorted(dag.task_dict))
