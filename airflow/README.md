# Airflow on the extract Droplet

Nightly archive extract. The systemd timer stays the production path until the shadow run matches. This file has names and commands only. Secret values stay in the env file on the Droplet.

## One-time setup

Create the env file as root, mode 600:

```sh
install -m 600 /dev/null /etc/lft-pipelines/airflow.env
```

Copy the names from `airflow/airflow.env.example`. Fill them on the Droplet. Do not commit the filled file.

- `DATABASE_URL` and `STATEMENT_TIMEOUT_MS` are the read-only extract role.
- `SPACES_ENDPOINT`, `SPACES_KEY`, `SPACES_SECRET` are the archive bucket.
- `ARCHIVE_URL` is the prefix the DAG writes. Start with `s3://lft-archive-airflow/airflow-shadow`.
- `ALERT_WEBHOOK_URL` is the pipelines Discord webhook. Empty means no alert.
- `HEALTHCHECK_URL` is the healthchecks.io ping. Empty means the heartbeat logs and returns.
- `AIRFLOW__CORE__FERNET_KEY` from `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`.
- `AIRFLOW__API_AUTH__JWT_SECRET` from `openssl rand -hex 32`. This is `[api_auth] jwt_secret` in Airflow 3.3.2. The scheduler and the API must share it.
- `AIRFLOW_ADMIN_PASSWORD` is the UI password for user `admin`. Generate it with `openssl rand -hex 24`. The entrypoint writes it into SimpleAuth's password file. The role is ADMIN. There is no all-admins mode.
- `POSTGRES_PASSWORD` is the metadata database. It must be URL-safe. Generate it with `openssl rand -hex 24`.

Postgres receives only `POSTGRES_USER`, `POSTGRES_DB`, and `POSTGRES_PASSWORD`. Compose fills that password, and the Airflow metadata URL, from the shell, so start with `--env-file` (below). The other secrets stay on the Airflow services. `AIRFLOW_ENV_FILE` must point at the same file, because that is what the Airflow services load.

## Alert preview

From the repo root, print three sample messages (a pruned-table failure, a kept-table failure, and the nightly warnings). This does not post:

```sh
python3 airflow/preview_alerts.py
```

On the Droplet, post those samples with a `[TEST]` prefix. Only the webhook variable is passed to the script:

```sh
cd /opt/lft-pipelines
ALERT_WEBHOOK_URL="$(grep '^ALERT_WEBHOOK_URL=' /etc/lft-pipelines/airflow.env | cut -d= -f2-)" python3 airflow/preview_alerts.py --post
```

The UI listens on `127.0.0.1:8080` on the Droplet. Open it through a tunnel:

```sh
ssh -L 8080:127.0.0.1:8080 root@<droplet>
```

Then browse `http://127.0.0.1:8080`. Log in as `admin` with `AIRFLOW_ADMIN_PASSWORD`.

## Build and start

On the Droplet, from a checkout of the reviewed commit. Pull first so `compare` is in the extract image, then rebuild that image before the morning check below.

```sh
git checkout <sha>
docker build -t lft-pipelines:latest .
cd airflow
docker compose --env-file /etc/lft-pipelines/airflow.env build
docker compose --env-file /etc/lft-pipelines/airflow.env up -d
```

New DAGs start paused. Unpause it once the services are up:

```sh
docker compose --env-file /etc/lft-pipelines/airflow.env exec scheduler airflow dags unpause archive_raw_daily
```

`airflow-init` migrates the metadata database and creates the `lft_db` pool with one slot. The API, scheduler, and dag processor restart unless stopped. Long-running caps are postgres 512 MB, API 512 MB, scheduler 1 GB, and dag processor 384 MB. Postgres is the metadata database, not the transit database.

Task logs are on the `airflow-logs` volume, mounted at `/opt/airflow/logs` on the scheduler, API, and dag processor. Once a week:

```sh
docker compose --env-file /etc/lft-pipelines/airflow.env exec scheduler find /opt/airflow/logs -type f -mtime +30 -delete
```

The image is Apache Airflow 3.3.2. Extract code runs in `/opt/lft-venv`, which is installed from this repo's pins and does not use Airflow's constraints. Auth is SimpleAuthManager (`admin:ADMIN`).

## Shadow, then cutover

For the three shadow nights, move the systemd extract to 00:05 so it does not overlap the 00:30 DAG on the 1 GB database:

```sh
sed -i 's/00:30:00/00:05:00/' /etc/systemd/system/lft-extract.timer && systemctl daemon-reload && systemctl list-timers lft-extract.timer
```

Leave `ARCHIVE_URL` on `s3://lft-archive-airflow/airflow-shadow`. Each morning, on the Droplet, compare the last three Chicago days. Partitions marked `partial_by_retention` are skipped.

```sh
docker run --rm --env-file /etc/lft-pipelines/extract.env --entrypoint python lft-pipelines:latest -m lft_pipelines.compare \
  --a s3://lft-archive-airflow/archive \
  --b s3://lft-archive-airflow/airflow-shadow \
  --start <chicago-today-minus-3> \
  --end <chicago-today> \
  --tables all
```

Exit 0 means the manifests agree. After three clean mornings, cut over:

1. Set `ARCHIVE_URL` to `s3://lft-archive-airflow/archive`.
2. `docker compose --env-file /etc/lft-pipelines/airflow.env up -d` from the `airflow` directory.
3. `systemctl disable --now lft-extract.timer`
4. Delete the `airflow-shadow` prefix after the next successful Airflow night.

To roll back, enable the timer again, put `OnCalendar` back to `00:30:00` if you changed it, and set `ARCHIVE_URL` back to the shadow prefix.
