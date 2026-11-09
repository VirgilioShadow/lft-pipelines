"""Connection double. It records SQL and never opens a socket."""

from contextlib import nullcontext
from types import SimpleNamespace


class FakeCursor:
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name
        self.description = None
        self.itersize = None
        self._rows: list = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        from lft_pipelines.sql import ensure_select

        ensure_select(sql)
        self.conn.statements.append((sql, params))
        lower = sql.lower()
        if "group by" in lower:
            self._rows = list(self.conn.group_rows)
            self.description = [
                SimpleNamespace(name="chicago_day", type_code=1082),
                SimpleNamespace(name="count", type_code=20),
            ]
        elif lower.startswith("select count"):
            self._rows = [(self.conn.count,)]
            self.description = [SimpleNamespace(name="count", type_code=20)]
        else:
            self._rows = list(self.conn.rows)
            self.description = list(self.conn.description)

    def fetchmany(self, size):
        self.conn.fetch_sizes.append(size)
        chunk = self._rows[:size]
        self._rows = self._rows[size:]
        return chunk

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows.pop(0)

    def fetchall(self):
        rows = list(self._rows)
        self._rows = []
        return rows


class FakeConn:
    def __init__(self, rows, description, count, group_rows=None):
        self.rows = rows
        self.description = description
        self.count = count
        self.group_rows = [] if group_rows is None else group_rows
        self.statements: list = []
        self.fetch_sizes: list = []
        self.cursor_names: list = []

    def cursor(self, name=None):
        self.cursor_names.append(name)
        return FakeCursor(self, name)

    def transaction(self):
        return nullcontext()

    def close(self):
        return None
