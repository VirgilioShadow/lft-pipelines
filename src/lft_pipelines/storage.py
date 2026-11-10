"""Local directories and DigitalOcean Spaces, with the same layout.

A partition is written to a temp name and then renamed (local) or copied and
deleted (object storage). Readers never see a half-written Parquet file.
"""

import io
import json
import os
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlparse

import pyarrow.parquet as pq


def partition_dir(schema: str, table: str, day) -> str:
    return f"{schema}/{table}/date={day.isoformat()}"


def parquet_rel(schema: str, table: str, day) -> str:
    return f"{partition_dir(schema, table, day)}/part-0.parquet"


def manifest_rel(schema: str, table: str, day) -> str:
    return f"{partition_dir(schema, table, day)}/_manifest.json"


class LocalStore:
    """A directory on disk. ``os.replace`` is the atomic publish step."""

    def __init__(self, root: Path):
        self.root = Path(root)

    def exists(self, rel: str) -> bool:
        return self._target(rel).is_file()

    def _target(self, rel: str) -> Path:
        path = (self.root / rel).resolve()
        root = self.root.resolve()
        if path != root and root not in path.parents:
            raise ValueError("refusing a path that leaves the output directory")
        return path

    def write_parquet(self, rel: str, schema, batches) -> None:
        path = self._target(rel)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".partial")
        try:
            with pq.ParquetWriter(str(tmp), schema, compression="zstd") as writer:
                for batch in batches:
                    writer.write_batch(batch)
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()

    def write_json(self, rel: str, payload: dict) -> None:
        path = self._target(rel)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".partial")
        try:
            tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            os.replace(tmp, path)
        finally:
            if tmp.exists():
                tmp.unlink()

    def open_parquet_sink(self, rel: str, schema):
        """A writer that stays open across batches, then publishes once."""

        return LocalParquetSink(self._target(rel), schema)

    def read_json(self, rel: str) -> dict | None:
        """Read one JSON object, or None when the file is absent."""

        path = self._target(rel)
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def list_manifests(self, schema: str, table: str, start: date, end: date) -> list[tuple[date, dict]]:
        """Manifests for ``[start, end)``, oldest day first. Missing days are omitted."""

        return _manifests_between(self, schema, table, start, end)


class LocalParquetSink:
    """Parquet on a ``.partial`` path. ``publish`` renames it into place."""

    def __init__(self, path: Path, schema):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.tmp = path.with_name(path.name + ".partial")
        self._writer = pq.ParquetWriter(str(self.tmp), schema, compression="zstd")
        self._closed = False
        self._published = False

    def write_batch(self, batch) -> None:
        self._writer.write_batch(batch)

    def publish(self) -> None:
        if not self._closed:
            self._writer.close()
            self._closed = True
        os.replace(self.tmp, self.path)
        self._published = True

    def abort(self) -> None:
        if not self._closed:
            self._writer.close()
            self._closed = True
        if not self._published and self.tmp.exists():
            self.tmp.unlink()


class S3Store:
    """Spaces or any S3 API, via PyArrow. No boto3.

    Object storage has no rename. The file is written to a ``.partial`` key,
    copied onto the final key, and the partial key is deleted.
    """

    def __init__(self, filesystem, bucket: str, prefix: str):
        self.filesystem = filesystem
        self.bucket = bucket
        self.prefix = prefix.strip("/")

    def _key(self, rel: str) -> str:
        if self.prefix:
            return f"{self.bucket}/{self.prefix}/{rel}"
        return f"{self.bucket}/{rel}"

    def exists(self, rel: str) -> bool:
        from pyarrow.fs import FileType

        info = self.filesystem.get_file_info(self._key(rel))
        return info.type == FileType.File

    def write_parquet(self, rel: str, schema, batches) -> None:
        final = self._key(rel)
        tmp = final + ".partial"
        with self.filesystem.open_output_stream(tmp) as sink:
            with pq.ParquetWriter(sink, schema, compression="zstd") as writer:
                for batch in batches:
                    writer.write_batch(batch)
        self.filesystem.copy_file(tmp, final)
        self.filesystem.delete_file(tmp)

    def open_parquet_sink(self, rel: str, schema):
        """A writer that stays open across batches, then copies the object into place."""

        return S3ParquetSink(self.filesystem, self._key(rel), schema)

    def write_json(self, rel: str, payload: dict) -> None:
        final = self._key(rel)
        tmp = final + ".partial"
        body = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
        with self.filesystem.open_output_stream(tmp) as sink:
            sink.write(body)
        self.filesystem.copy_file(tmp, final)
        self.filesystem.delete_file(tmp)

    def read_json(self, rel: str) -> dict | None:
        """Read one JSON object, or None when the key is absent."""

        key = self._key(rel)
        from pyarrow.fs import FileType

        info = self.filesystem.get_file_info(key)
        if info.type != FileType.File:
            return None
        with self.filesystem.open_input_stream(key) as source:
            return json.loads(source.read().decode("utf-8"))

    def list_manifests(self, schema: str, table: str, start: date, end: date) -> list[tuple[date, dict]]:
        """Manifests for ``[start, end)``, oldest day first. Missing days are omitted."""

        return _manifests_between(self, schema, table, start, end)


class S3ParquetSink:
    """Parquet on a ``.partial`` key. ``publish`` copies it onto the final key."""

    def __init__(self, filesystem, final: str, schema):
        self.filesystem = filesystem
        self.final = final
        self.tmp = final + ".partial"
        self._sink = filesystem.open_output_stream(self.tmp)
        self._writer = pq.ParquetWriter(self._sink, schema, compression="zstd")
        self._closed = False
        self._published = False

    def write_batch(self, batch) -> None:
        self._writer.write_batch(batch)

    def publish(self) -> None:
        if not self._closed:
            self._writer.close()
            self._sink.close()
            self._closed = True
        self.filesystem.copy_file(self.tmp, self.final)
        self.filesystem.delete_file(self.tmp)
        self._published = True

    def abort(self) -> None:
        if not self._closed:
            try:
                self._writer.close()
            finally:
                self._sink.close()
            self._closed = True
        if not self._published:
            self.filesystem.delete_file(self.tmp)


class MemoryObjectFile(io.BytesIO):
    """File-like sink that keeps bytes on a dict when closed. Tests only."""

    def __init__(self, store: dict, key: str):
        super().__init__()
        self._store = store
        self._key = key

    def close(self) -> None:
        self._store[self._key] = self.getvalue()
        super().close()


class MemoryFilesystem:
    """Stand-in for ``pyarrow.fs.S3FileSystem`` used by tests."""

    def __init__(self):
        self.data: dict[str, bytes] = {}
        self.operations: list[tuple] = []

    def open_output_stream(self, key: str) -> MemoryObjectFile:
        self.operations.append(("open", key))
        return MemoryObjectFile(self.data, key)

    def copy_file(self, src: str, dest: str) -> None:
        self.operations.append(("copy", src, dest))
        self.data[dest] = self.data[src]

    def delete_file(self, key: str) -> None:
        self.operations.append(("delete", key))
        self.data.pop(key, None)

    def get_file_info(self, key: str):
        from pyarrow.fs import FileInfo, FileType

        if key in self.data:
            return FileInfo(key, type=FileType.File, size=len(self.data[key]))
        return FileInfo(key, type=FileType.NotFound)

    def open_input_stream(self, key: str):
        """Bytes of one stored object. Tests only."""

        if key not in self.data:
            raise FileNotFoundError(key)
        return io.BytesIO(self.data[key])


def _manifests_between(store, schema: str, table: str, start: date, end: date) -> list[tuple[date, dict]]:
    """Read ``_manifest.json`` for each day in ``[start, end)`` that has one."""

    found = []
    day = start
    while day < end:
        payload = store.read_json(manifest_rel(schema, table, day))
        if payload is not None:
            found.append((day, payload))
        day += timedelta(days=1)
    return found


def spaces_filesystem(env: dict):
    """Build an S3 filesystem from ``SPACES_ENDPOINT``, ``SPACES_KEY``, ``SPACES_SECRET``.

    Values are read from the environment and passed to PyArrow. They are not
    logged. ``endpoint_override`` is the Spaces host (the scheme is stripped).
    """

    missing = [name for name in ("SPACES_ENDPOINT", "SPACES_KEY", "SPACES_SECRET") if not env.get(name)]
    if missing:
        raise RuntimeError(f"missing {', '.join(missing)}")
    parsed = urlparse(env["SPACES_ENDPOINT"] if "://" in env["SPACES_ENDPOINT"] else f"https://{env['SPACES_ENDPOINT']}")
    host = parsed.netloc or parsed.path
    host = host.rstrip("/")
    region = host.split(".")[0] if host.endswith("digitaloceanspaces.com") else None
    from pyarrow.fs import S3FileSystem

    kwargs = {
        "access_key": env["SPACES_KEY"],
        "secret_key": env["SPACES_SECRET"],
        "endpoint_override": host,
        "scheme": parsed.scheme or "https",
        "force_virtual_addressing": True,
    }
    if region:
        kwargs["region"] = region
    return S3FileSystem(**kwargs)


def open_store(out: str, *, env: dict | None = None, filesystem=None):
    """Open a local directory or an ``s3://bucket/prefix`` location."""

    if out.startswith("s3://"):
        parsed = urlparse(out)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        if not bucket:
            raise ValueError("s3 URL needs a bucket")
        fs = filesystem if filesystem is not None else spaces_filesystem(env if env is not None else os.environ)
        return S3Store(fs, bucket, prefix)
    return LocalStore(Path(out))

