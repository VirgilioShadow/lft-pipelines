"""Turn cursor rows into Arrow batches without holding the whole day.

Postgres type OIDs are mapped once from the cursor description. Each
``fetchmany`` batch becomes one RecordBatch and can be discarded.
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import UUID

import pyarrow as pa

# Common Postgres OIDs. Anything else is stored as a string so a new column
# type cannot crash a long extract.
_OID_ARROW = {
    16: pa.bool_(),
    17: pa.binary(),
    20: pa.int64(),
    21: pa.int16(),
    23: pa.int32(),
    25: pa.string(),
    700: pa.float32(),
    701: pa.float64(),
    1042: pa.string(),
    1043: pa.string(),
    1082: pa.date32(),
    1083: pa.string(),
    1114: pa.timestamp("us"),
    1184: pa.timestamp("us", tz="UTC"),
    1700: pa.string(),
    2950: pa.string(),
    114: pa.string(),
    3802: pa.string(),
    1009: pa.list_(pa.string()),
    1015: pa.list_(pa.string()),
}


def arrow_schema(description) -> pa.Schema:
    """Build a schema from a DB-API ``cursor.description``."""

    fields = []
    for column in description:
        type_code = getattr(column, "type_code", None)
        if type_code is None and len(column) > 1:
            type_code = column[1]
        name = column.name if hasattr(column, "name") else column[0]
        fields.append(pa.field(name, _OID_ARROW.get(type_code, pa.string())))
    return pa.schema(fields)


def column_names(description) -> tuple[str, ...]:
    return tuple(column.name if hasattr(column, "name") else column[0] for column in description)


def _prepare(value, arrow_type: pa.DataType):
    if value is None:
        return None
    if pa.types.is_timestamp(arrow_type) and isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=timezone.utc)
        return value
    if pa.types.is_string(arrow_type):
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        if isinstance(value, (Decimal, UUID, date, datetime)):
            return str(value) if not isinstance(value, datetime) else value.isoformat()
        return str(value)
    return value


def rows_to_batch(rows: list[tuple], schema: pa.Schema) -> pa.RecordBatch:
    prepared = [
        tuple(_prepare(value, field.type) for value, field in zip(row, schema, strict=True))
        for row in rows
    ]
    columns = list(zip(*prepared)) if prepared else [() for _ in schema]
    arrays = [pa.array(column, type=field.type) for column, field in zip(columns, schema, strict=True)]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def empty_batch(schema: pa.Schema) -> pa.RecordBatch:
    arrays = [pa.array([], type=field.type) for field in schema]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def schema_columns(schema: pa.Schema) -> list[dict[str, str]]:
    return [{"name": field.name, "type": str(field.type)} for field in schema]
