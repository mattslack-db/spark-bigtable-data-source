"""
Stateful processor that consumes bigtable_changes and reconstructs the full row record.

Uses row_key as the grouping key. State is a MapState keyed by (column_family,
column_qualifier); the value is the latest cell value (bytes) for that column. On each
new change, state is updated and one output row is emitted with row_key and the full
record: a map from "column_family:column_qualifier" to the latest value.

Column granularity
------------------
State (and the reconstructed record) is per column — a distinct (family, qualifier)
pair — so two qualifiers in the same family coexist and a DELETE_COLUMN removes only the
targeted column. Output record keys are the string ``f"{family}:{qualifier}"``. Bigtable
column-family names cannot contain ``":"``, so the first ``":"`` unambiguously separates
family from qualifier when the record is read back (e.g. for initial state). Column
qualifiers are assumed to be UTF-8 for the string record key; non-UTF-8 qualifiers are
decoded with replacement and will not round-trip exactly.

Example usage with the bigtable_changes stream::

    from pyspark.sql import SparkSession
    from bigtable_stateful_processor import BigtableReconstructProcessor, RECONSTRUCTED_RECORD_SCHEMA

    spark = SparkSession.builder.getOrCreate()
    changes = (
        spark.readStream.format("bigtable_changes")
        .option("project_id", "...")
        .option("instance_id", "...")
        .option("table_id", "...")
        .load()
    )
    reconstructed = (
        changes.groupBy("row_key")
        .transformWithState(
            statefulProcessor=BigtableReconstructProcessor(),
            outputStructType=RECONSTRUCTED_RECORD_SCHEMA,
            outputMode="Update",
            timeMode="None",
        )
    )
    reconstructed.writeStream.outputMode("update").format("console").start()

With initial state from a Delta table (e.g. a previous run's output); pass GroupedData::

    initial_state = spark.read.table("catalog.schema.bt_reconstructed").groupBy("row_key")
    reconstructed = (
        changes.groupBy("row_key")
        .transformWithState(
            statefulProcessor=BigtableReconstructProcessor(),
            outputStructType=RECONSTRUCTED_RECORD_SCHEMA,
            outputMode="Update",
            timeMode="None",
            initialState=initial_state,
        )
    )
"""

from typing import Any, Iterator, List, Tuple

from pyspark.sql import Row
from pyspark.sql.streaming.stateful_processor import (
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
)
from pyspark.sql.types import BinaryType, StringType, StructType

from bigtable_data_source.mutation_types import MutationType

# Separator between column family and qualifier in the reconstructed record's map keys.
# Bigtable family names cannot contain ":", so the first ":" splits family from qualifier.
_RECORD_KEY_SEPARATOR = ":"

# MapState: key = (column_family: string, column_qualifier: binary), value = latest value (bytes)
_MAP_KEY_SCHEMA = (
    StructType()
    .add("column_family", StringType())
    .add("column_qualifier", BinaryType())
)
_MAP_VALUE_SCHEMA = StructType().add("value", BinaryType())


class BigtableReconstructProcessor(StatefulProcessor):
    """
    Reconstructs the full Bigtable row from change stream events.

    State: MapState from (column_family, column_qualifier) to latest value (binary).
    When any new change arrives for a row_key, state is updated and an output row is
    emitted with row_key and the full record (all columns' latest values), keyed by
    "column_family:column_qualifier".
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self._handle = handle
        self._cells = handle.getMapState(
            "cells",
            userKeySchema=_MAP_KEY_SCHEMA,
            valueSchema=_MAP_VALUE_SCHEMA,
        )

    def handleInitialState(
        self,
        key: Any,
        rows: Iterator[Row],
        timerValues: TimerValues,
    ) -> None:
        """
        Load state from an initial state batch (e.g. a Delta table with row_key + record).
        Each row must have a 'record' column: a map of "column_family:column_qualifier"
        (str) -> value (bytes). If multiple rows exist for the same key, they are applied
        in order (last wins per column).
        """
        for row in rows:
            record = _extract_record(row)
            if record is None:
                continue
            for record_key, val in record.items():
                family, qualifier = _split_record_key(_as_str(record_key))
                value = val if val is not None else b""
                self._cells.updateValue((family, qualifier), (value,))

    def handleInputRows(
        self,
        key: Any,
        rows: Iterator[Row],
        timerValues: TimerValues,
    ) -> Iterator[Row]:
        row_key = _extract_row_key(key)
        for row in rows:
            family = _as_str(row.column_family)
            qualifier = row.column_qualifier if row.column_qualifier is not None else b""
            map_key = (family, qualifier)
            mutation_type = (row.mutation_type or "").strip().upper()
            if mutation_type == MutationType.SET_CELL.value:
                value = row.value if row.value is not None else b""
                self._cells.updateValue(map_key, (value,))
            elif mutation_type == MutationType.DELETE_ROW.value:
                self._cells.clear()
            elif mutation_type == MutationType.DELETE_COLUMN.value:
                if self._cells.containsKey(map_key):
                    self._cells.removeKey(map_key)
            elif mutation_type == MutationType.DELETE_FAMILY.value:
                for family_key in _keys_in_family(self._cells, family):
                    self._cells.removeKey(family_key)
            # else: ignore unknown mutation type

        record = _build_record_from_state(self._cells)
        yield Row(row_key=row_key, record=record)

    def close(self) -> None:
        pass


def _as_str(value: Any) -> str:
    """Decode a column family/qualifier/record key to str, tolerating bytes or None."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    return str(value)


def _compose_record_key(family: str, qualifier: bytes) -> str:
    """Build the output map key "family:qualifier".

    The record is a map<string, binary>, so the qualifier must become a string. Decode
    it strictly as UTF-8 and fail fast on non-UTF-8 bytes rather than lossily replacing
    them: a lossy decode could map two distinct binary qualifiers to the same key
    (silent collision/corruption) and would not round-trip back to the original bytes.
    """
    if qualifier is None:
        qualifier = b""
    try:
        qual_str = bytes(qualifier).decode("utf-8")
    except UnicodeDecodeError as e:
        raise ValueError(
            f"Column qualifier {bytes(qualifier)!r} in family {family!r} is not valid "
            "UTF-8. BigtableReconstructProcessor represents record keys as "
            "'column_family:column_qualifier' strings and requires UTF-8 qualifiers."
        ) from e
    return f"{family}{_RECORD_KEY_SEPARATOR}{qual_str}"


def _split_record_key(record_key: str) -> Tuple[str, bytes]:
    """Inverse of _compose_record_key: split on the first ":" into (family, qualifier bytes).

    A record key with no separator is treated as a family with an empty qualifier.
    """
    family, separator, qualifier = record_key.partition(_RECORD_KEY_SEPARATOR)
    if not separator:
        return family, b""
    return family, qualifier.encode("utf-8")


def _extract_record(row: Any) -> dict | None:
    """Extract a record dict from an initial state row.

    PySpark's transformWithState passes initial state rows as either:
      - Row objects with a 'record' attribute (unit tests, some PySpark versions)
      - Raw column values: the record column arrives as a list of (key, value)
        tuples or a dict (PySpark 4.x streaming workers)
    """
    # Row object with named field
    record = getattr(row, "record", None)
    if record is not None:
        return dict(record) if not isinstance(record, dict) else record
    # Already a dict (e.g. MapType deserialized directly)
    if isinstance(row, dict):
        return row
    # List of (key, value) tuples — PySpark MapType deserialization
    if isinstance(row, list) and row and isinstance(row[0], (tuple, list)):
        return dict(row)
    return None


def _extract_row_key(key: Any) -> bytes:
    if isinstance(key, bytes):
        return key
    if isinstance(key, (tuple, list)):
        if not key:
            raise TypeError("Cannot extract row_key from an empty grouping key")
        return key[0]
    if hasattr(key, "row_key"):
        return key.row_key
    if hasattr(key, "__getitem__"):
        try:
            return key[0]
        except (IndexError, KeyError, TypeError) as e:
            raise TypeError(
                f"Cannot extract row_key from grouping key of type {type(key).__name__}"
            ) from e
    raise TypeError(
        f"Cannot extract row_key from grouping key of type {type(key).__name__}"
    )


def _keys_in_family(cells, family: str) -> List[tuple]:
    """Return the state keys belonging to a column family.

    Materialised into a list so callers can remove entries without mutating the
    MapState while its iterator is live.
    """
    return [
        map_key_tuple
        for map_key_tuple, _ in cells.iterator()
        if map_key_tuple and map_key_tuple[0] == family
    ]


def _build_record_from_state(cells) -> dict:
    """Build record map ("family:qualifier" -> value bytes) from MapState."""
    record = {}
    for map_key_tuple, value_tuple in cells.iterator():
        family = map_key_tuple[0] if map_key_tuple else ""
        qualifier = (
            map_key_tuple[1] if map_key_tuple and len(map_key_tuple) > 1 else b""
        )
        value = value_tuple[0] if value_tuple else b""
        record[_compose_record_key(family, qualifier)] = value
    return record
