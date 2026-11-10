# Airflow 3.3.2 (released 2026-09-17). 3.4.0 shipped 2026-11-06, inside the
# two-week window, so it is not the pin. The extract venv is separate:
# Airflow's constraints are not applied to it.
FROM apache/airflow:3.3.2

USER root
COPY pyproject.toml README.md /src/
COPY src /src/src
RUN python -m venv /opt/lft-venv \
    && /opt/lft-venv/bin/pip install --no-cache-dir /src \
    && chown -R airflow:root /opt/lft-venv /src
COPY airflow/dags /opt/airflow/dags
COPY airflow/dagbag_check.py /opt/airflow/dagbag_check.py
COPY airflow/entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod 755 /opt/airflow/entrypoint.sh \
    && chown -R airflow:root /opt/airflow/dags /opt/airflow/dagbag_check.py /opt/airflow/entrypoint.sh
USER airflow
