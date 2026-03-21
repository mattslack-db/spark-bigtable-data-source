"""
Stateful processor that consumes bigtable_changes and reconstructs the full row record.

Uses row_key as the grouping key. State is a MapState: key = column_family (string),
value = latest value (bytes) for that family. On each new change, state is updated and
one output row is emitted with row_key and the full record (map of column_family -> value).

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

from typing import Any, Iterator

from pyspark.sql import Row
from pyspark.sql.streaming.stateful_processor import (
    StatefulProcessor,
    StatefulProcessorHandle,
    TimerValues,
)
from pyspark.sql.types import BinaryType, StringType, StructType

from bigtable_data_source.mutation_types import MutationType

# MapState: key = column_family (single string), value = latest value (bytes)
_MAP_KEY_SCHEMA = StructType().add("column_family", StringType())
_MAP_VALUE_SCHEMA = StructType().add("value", BinaryType())


class BigtableReconstructProcessor(StatefulProcessor):
    """
    Reconstructs the full Bigtable row from change stream events.

    State: MapState from column_family (string) to latest value (binary).
    When any new change arrives for a row_key, state is updated and an output row
    is emitted with row_key and the full record (all column families' latest values).
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
        Each row must have a 'record' column: a map of column_family (str) -> value (bytes).
        If multiple rows exist for the same key, they are applied in order (last wins per key).
        """
        row_key = _extract_row_key(key)
        count = 0
        for row in rows:
            record = _extract_record(row)
            if record is None:
                continue
            for cf, val in record.items():
                if not isinstance(cf, str):
                    cf = cf.decode("utf-8") if cf else ""
                value = val if val is not None else b""
                self._cells.updateValue((cf,), (value,))
                count += 1

    def handleInputRows(
        self,
        key: Any,
        rows: Iterator[Row],
        timerValues: TimerValues,
    ) -> Iterator[Row]:
        row_key = _extract_row_key(key)
        pre_state = _build_record_from_state(self._cells)
        for row in rows:
            cf = row.column_family
            if not isinstance(cf, str):
                cf = cf.decode("utf-8") if cf else ""
            map_key = (cf,)
            mutation_type = (row.mutation_type or "").strip().upper()
            if mutation_type == MutationType.SET_CELL.value:
                value = row.value if row.value is not None else b""
                self._cells.updateValue(map_key, (value,))
            elif mutation_type == MutationType.DELETE_ROW.value:
                self._cells.clear()
            elif mutation_type in (MutationType.DELETE_COLUMN.value, MutationType.DELETE_FAMILY.value):
                if self._cells.containsKey(map_key):
                    self._cells.removeKey(map_key)
            # else: ignore unknown mutation type

        record = _build_record_from_state(self._cells)
        yield Row(row_key=row_key, record=record)

    def close(self) -> None:
        pass


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
    if isinstance(key, (tuple, list)) and len(key) >= 1:
        return key[0]
    if hasattr(key, "row_key"):
        return key.row_key
    if hasattr(key, "__getitem__"):
        return key[0]
    return key


def _build_record_from_state(cells) -> dict:
    """Build record map (column_family -> value bytes) from MapState."""
    record = {}
    for map_key_tuple, value_tuple in cells.iterator():
        cf = map_key_tuple[0] if map_key_tuple else ""
        val = value_tuple[0] if value_tuple else b""
        record[cf] = val
    return record
