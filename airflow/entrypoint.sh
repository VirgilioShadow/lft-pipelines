#!/bin/bash
# Metadata URL and the SimpleAuth password file. Values stay in the env file.
set -euo pipefail
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@postgres/airflow"
if [ -z "${AIRFLOW_ADMIN_PASSWORD:-}" ]; then
  echo "AIRFLOW_ADMIN_PASSWORD is required" >&2
  exit 1
fi
umask 077
python -c 'import json, os; open("/opt/airflow/simple_auth_passwords.json", "w").write(json.dumps({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}) + "\n")'
exec /entrypoint "$@"
