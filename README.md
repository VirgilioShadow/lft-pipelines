# lft-pipelines

Read-only extracts of Lakefront Rails transit history. The live database keeps a short window of GPS positions, trip updates, forecasts, alerts, and weather. This tool copies each retained day to Parquet so the history survives the nightly prune.

It is the extract step of a data pipeline. The functions are importable. A later step wraps them in an Apache Airflow DAG on a DigitalOcean Droplet and writes the same layout to Spaces. Nothing here trains a model, and nothing here writes to the database.

## What the data is

The source is a Postgres database with Metra and CTA tables. High-churn tables are pruned on a schedule:

| Table | Partition column | Kept |
| --- | --- | --- |
| `metra.Positions`, `cta.CtaPositionSnapshot` | timestamp | 30 days |
| `metra.TripUpdateSnapshot`, `cta.CtaTripUpdateSnapshot` | `polled_at` | 120 days |
| `metra.DelayForecast` | `predicted_at` | 180 days |
| `metra.StopDelayForecast` | `service_date` | 2 days |
| `metra.SpeedObservation`, `weather.WeatherObservation` | timestamp | not pruned |
| `metra.OnTimeByTodDailyBucket`, `metra.HourlySpeedBinDailyBucket` | `service_date` | not pruned |
| `metra.ServiceAlert` and GTFS `Trips`, `StopTimes`, `Stops`, `Routes`, `Shapes`, `Calendars`, `CalendarDates` | none | one snapshot per run |

A day partition is the America/Chicago calendar date of that column. The query uses a half-open range `[start, end)` in UTC, with no function wrapped around the column, so the database can use an index. `metra.Positions.recorded_at` has no index (the indexes are `source_id`, `updatedAt`, and `trip_id`), so that table is read in one range scan instead of one scan per day. A btree on `Positions(recorded_at)` is the suggested index for LFT-087, because the nightly prune scans the same column. Static GTFS tables, and `metra.ServiceAlert`, are copied whole and labeled with the run's as-of date (`--end`). Service alerts are upserted on every poll, so `polled_at` is not a stable day key.

## Correctness rules

- A day partition is extracted only after it has settled. The tool never plans `day >= today` in Chicago. `metra.HourlySpeedBinDailyBucket` and `metra.OnTimeByTodDailyBucket` are deleted and rebuilt for the latest service date, so they also skip `day >= today - 1` (`settle_days` on the table). A refused day stays on the dry-run as `not settled`.
- The day `today - retention_days` is only partly pruned (the 03:00–03:40 Chicago jobs). It is still extracted. Its manifest sets `partial_by_retention` to true. Every other manifest sets that flag to false. The dry-run shows the flag.
- `metra.Positions` uses one `SELECT` for the whole planned span, with a named cursor. Each row is routed to that Chicago date's Parquet file. Days that already have a manifest are skipped unless `--overwrite` is set; if every planned day has one, the scan does not run. `count(*)` for the span is one `GROUP BY` of the Chicago date (`AT TIME ZONE 'America/Chicago'` appears only in that count). A row whose partition column is NULL is counted in the run output and is not written.
- `metra.ServiceAlert` is a snapshot: one full copy per run, labeled with `--end`.

## Why archive

The managed database is 1 GB of RAM and about 20 GB of disk, and the disk is near full. Retention stays as it is: pausing the prune would fill the disk in a few weeks. The archive is the copy that makes a shorter raw window safe later. The oldest days in the shortest windows are the ones the next prune will delete, so those are extracted first.

## Layout

```text
<out>/<schema>/<table>/date=YYYY-MM-DD/part-0.parquet
<out>/<schema>/<table>/date=YYYY-MM-DD/_manifest.json
```

`<out>` is a local directory or `s3://<bucket>/<prefix>`. Parquet is zstd. The file is written to a temp name and published with a rename (local) or a copy-and-delete (Spaces). The manifest records the row count, the min and max of the partition column, the column names and Arrow types, `partial_by_retention`, `extracted_at`, and the tool version. After the write, `count(*)` for the same range is compared with the rows written. A difference sets `mismatch` to true.

If the manifest is already there, the partition is skipped. `--overwrite` replaces it.

## How to run

Python 3.11 or newer. From this directory:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
cp .env.example .env
```

Put the database URL in `.env` under the name `DATABASE_URL`. Do not commit that file.

`--end` is exclusive for day tables and is the label on snapshot tables. `--dry-run` prints the plan and does not connect.

```sh
python -m lft_pipelines.extract --tables all --start 2026-11-01 --end 2026-11-04 --out "$HOME/lft-archive" --dry-run
```

`--at-risk` drops days a pruned table can no longer hold (`today - retention_days` in Chicago) and runs the shortest retention window first, oldest day first. The 2-day stop forecasts come before the 30-day position tables, which come before the 120-day trip updates. A pause between days (default 2 seconds, `--pause-seconds`) keeps the small database from serving two large days back to back.

The first real run, with `--end` set to the Chicago date of the run (2026-11-09), is the GPS tables that the next prune will delete. The second run is everything else. Days already written, including the GPS days, are skipped because their manifests exist. `Positions` is one scan for whatever days of that table are still missing.

```sh
python -m lft_pipelines.extract --tables metra.Positions,cta.CtaPositionSnapshot --at-risk --start 2026-10-10 --end 2026-11-09 --out "$HOME/lft-archive" --pause-seconds 5
python -m lft_pipelines.extract --tables all --at-risk --start 2026-07-12 --end 2026-11-09 --out "$HOME/lft-archive" --pause-seconds 5
```

## Why the versions are pinned

The old training jobs installed SQLAlchemy without an exact pin. SQLAlchemy 2.1 changed PostgreSQL driver handling and those jobs failed on every run. This project does not use SQLAlchemy, pandas, or torch. It uses psycopg 3 only, and every dependency in `pyproject.toml` is pinned to an exact version, including `psycopg-binary`, so a later release cannot change the driver under a scheduled job.

Each session sets `default_transaction_read_only`, `statement_timeout` (default 300000 ms, override with `STATEMENT_TIMEOUT_MS`), and `application_name=lft-pipelines-extract` as connection parameters. The cursor only receives `SELECT`. Rows come from a named server-side cursor in batches, written straight to a Parquet writer, so a large day does not sit in memory.

## Tests

```sh
pip install -e ".[dev]"
pytest
```

The tests use a fake cursor. They do not need a database or Spaces credentials. GitHub Actions runs that same `pytest` and nothing else.

## Read-only database user

The Mac can use an existing URL until a separate role exists. The role the Droplet should use is login plus `SELECT` only:

```sql
CREATE ROLE lft_archive_read WITH LOGIN PASSWORD 'set-outside-git';
GRANT CONNECT ON DATABASE trains TO lft_archive_read;
GRANT USAGE ON SCHEMA metra, cta, weather TO lft_archive_read;
GRANT SELECT ON ALL TABLES IN SCHEMA metra, cta, weather TO lft_archive_read;
ALTER ROLE lft_archive_read SET default_transaction_read_only = on;
```

Creating that role, and adding the Droplet's address as a trusted source, needs an explicit approval. The password never goes in this repo.

## How step 1 wraps this in Airflow

An Airflow DAG is the workflow graph. A task is one unit of work with its own retry and log. Step 1 calls `build_plan` and `extract_partition` from a TaskFlow task, one Chicago day at a time, so a timeout retries that day instead of the whole history. `DATABASE_URL` and the Spaces keys become Airflow Connections (stored credentials, not files in git). The schedule runs after the Chicago prune window has passed for the day being saved, and before the next prune deletes the oldest retained day. The manifest skip makes a backfill, a rerun of a missed date, safe to repeat.

## Deployment target

Step 1 builds this. The sketch:

- A DigitalOcean Droplet, 4–8 GB, in the same VPC as the database, runs Apache Airflow with Docker Compose.
- This image is the worker. It is a slim Python 3.11 image, the pinned dependencies, and a non-root user. The entrypoint is `python -m lft_pipelines.extract`.
- The DAG writes to DigitalOcean Spaces with `--out s3://<bucket>/<prefix>`. PyArrow's `S3FileSystem` uses `SPACES_ENDPOINT`, `SPACES_KEY`, and `SPACES_SECRET`. There is no boto3. The layout, the temp-key then copy-and-delete publish, and the manifests are the same as on disk.
- The Droplet's address is one trusted source on the database. The role is the read-only user above.

The Droplet, the bucket, the firewall change, and the database role are not created by this repository.
