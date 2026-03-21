"""Unit tests for Bigtable stateful processor (transformWithState)."""

from unittest.mock import MagicMock
from pyspark.sql import Row


def test_extract_row_key_bytes():
    """_extract_row_key returns bytes when key is bytes."""
    from bigtable_stateful_processor.processor import _extract_row_key

    assert _extract_row_key(b"row-1") == b"row-1"


def test_extract_row_key_tuple():
    """_extract_row_key returns first element when key is tuple."""
    from bigtable_stateful_processor.processor import _extract_row_key

    assert _extract_row_key((b"row-1",)) == b"row-1"
    assert _extract_row_key((b"row-1", "other")) == b"row-1"


def test_extract_row_key_list():
    """_extract_row_key returns first element when key is list."""
    from bigtable_stateful_processor.processor import _extract_row_key

    assert _extract_row_key([b"row-1"]) == b"row-1"


def test_extract_row_key_row_like():
    """_extract_row_key returns row_key attribute when present."""
    from bigtable_stateful_processor.processor import _extract_row_key

    key = Row(row_key=b"row-1")
    assert _extract_row_key(key) == b"row-1"


def test_extract_row_key_getitem():
    """_extract_row_key returns key[0] when key supports __getitem__."""
    from bigtable_stateful_processor.processor import _extract_row_key

    class KeyLike:
        def __getitem__(self, i):
            return b"row-1" if i == 0 else None

    assert _extract_row_key(KeyLike()) == b"row-1"


def test_build_record_from_state_empty():
    """_build_record_from_state returns empty dict when state iterator is empty."""
    from bigtable_stateful_processor.processor import _build_record_from_state

    mock_cells = MagicMock()
    mock_cells.iterator.return_value = iter([])
    assert _build_record_from_state(mock_cells) == {}


def test_build_record_from_state_single_entry():
    """_build_record_from_state returns map of one column family to value."""
    from bigtable_stateful_processor.processor import _build_record_from_state

    mock_cells = MagicMock()
    mock_cells.iterator.return_value = iter([(("cf1",), (b"value1",))])
    assert _build_record_from_state(mock_cells) == {"cf1": b"value1"}


def test_build_record_from_state_multiple_entries():
    """_build_record_from_state returns map of all column families to values."""
    from bigtable_stateful_processor.processor import _build_record_from_state

    mock_cells = MagicMock()
    mock_cells.iterator.return_value = iter([
        (("cf1",), (b"v1",)),
        (("cf2",), (b"v2",)),
    ])
    assert _build_record_from_state(mock_cells) == {"cf1": b"v1", "cf2": b"v2"}


def test_reconstructed_record_schema():
    """RECONSTRUCTED_RECORD_SCHEMA has row_key and record (MapType)."""
    from bigtable_stateful_processor import RECONSTRUCTED_RECORD_SCHEMA
    from pyspark.sql.types import BinaryType, MapType, StringType, StructType

    assert isinstance(RECONSTRUCTED_RECORD_SCHEMA, StructType)
    assert RECONSTRUCTED_RECORD_SCHEMA.fieldNames() == ["row_key", "record"]
    assert RECONSTRUCTED_RECORD_SCHEMA["row_key"].dataType == BinaryType()
    assert isinstance(RECONSTRUCTED_RECORD_SCHEMA["record"].dataType, MapType)
    assert RECONSTRUCTED_RECORD_SCHEMA["record"].dataType.keyType == StringType()
    assert RECONSTRUCTED_RECORD_SCHEMA["record"].dataType.valueType == BinaryType()


def test_processor_init():
    """Processor init creates MapState via handle.getMapState."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    mock_handle = MagicMock()
    mock_map_state = MagicMock()
    mock_handle.getMapState.return_value = mock_map_state

    processor = BigtableReconstructProcessor()
    processor.init(mock_handle)

    assert processor._handle is mock_handle
    assert processor._cells is mock_map_state
    mock_handle.getMapState.assert_called_once()
    call_args, call_kw = mock_handle.getMapState.call_args
    assert call_args[0] == "cells"
    assert "userKeySchema" in call_kw
    assert "valueSchema" in call_kw


def test_handle_input_rows_set_cell_emits_full_record():
    """handleInputRows with one SET_CELL updates state and emits row with record."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(
        row_key=b"r1",
        column_family="cf1",
        column_qualifier=b"col1",
        value=b"value1",
        mutation_type="SET_CELL",
        commit_timestamp=None,
        partition_start_key=b"",
        partition_end_key=b"",
        low_watermark=None,
    )
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert len(out) == 1
    assert out[0].row_key == b"r1"
    assert out[0].record == {"cf1": b"value1"}
    assert mock_cells._state == {("cf1",): (b"value1",)}


def test_handle_input_rows_two_families_emits_combined_record():
    """handleInputRows with two SET_CELLs (different families) emits record with both."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    rows = [
        Row(row_key=b"r1", column_family="cf1", column_qualifier=b"q1", value=b"v1",
            mutation_type="SET_CELL", commit_timestamp=None, partition_start_key=b"",
        partition_end_key=b"", low_watermark=None),
        Row(row_key=b"r1", column_family="cf2", column_qualifier=b"q2", value=b"v2",
            mutation_type="SET_CELL", commit_timestamp=None, partition_start_key=b"",
        partition_end_key=b"", low_watermark=None),
    ]
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter(rows), timer))

    assert len(out) == 1
    assert out[0].record == {"cf1": b"v1", "cf2": b"v2"}
    assert mock_cells._state == {("cf1",): (b"v1",), ("cf2",): (b"v2",)}


def test_handle_input_rows_delete_family_removes_entry():
    """handleInputRows with DELETE_FAMILY removes that column family from state."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    mock_cells.updateValue(("cf1",), (b"v1",))
    mock_cells.updateValue(("cf2",), (b"v2",))
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1", column_family="cf1", column_qualifier=b"q1", value=b"",
              mutation_type="DELETE_FAMILY", commit_timestamp=None, partition_start_key=b"",
        partition_end_key=b"", low_watermark=None)
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert len(out) == 1
    assert out[0].record == {"cf2": b"v2"}
    assert mock_cells._state == {("cf2",): (b"v2",)}


def test_handle_input_rows_delete_row_clears_state():
    """handleInputRows with DELETE_ROW clears state and emits empty record."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    mock_cells.updateValue(("cf1",), (b"v1",))
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1", column_family="cf1", column_qualifier=b"q1", value=b"",
              mutation_type="DELETE_ROW", commit_timestamp=None, partition_start_key=b"",
        partition_end_key=b"", low_watermark=None)
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert len(out) == 1
    assert out[0].record == {}
    assert mock_cells._state == {}


def test_handle_input_rows_unknown_mutation_type_ignored():
    """handleInputRows ignores unknown mutation type but still emits current state."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    mock_cells.updateValue(("cf1",), (b"v1",))
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1", column_family="cf1", column_qualifier=b"q1", value=b"",
              mutation_type="UNKNOWN", commit_timestamp=None, partition_start_key=b"",
        partition_end_key=b"", low_watermark=None)
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert len(out) == 1
    assert out[0].record == {"cf1": b"v1"}
    assert mock_cells._state == {("cf1",): (b"v1",)}


def test_handle_input_rows_column_family_bytes_decoded():
    """handleInputRows decodes column_family from bytes to string."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1", column_family=b"cf1", column_qualifier=b"q1", value=b"v1",
              mutation_type="SET_CELL", commit_timestamp=None, partition_start_key=b"",
        partition_end_key=b"", low_watermark=None)
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert out[0].record == {"cf1": b"v1"}


def test_processor_close_noop():
    """Processor close() does not raise."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    processor.close()


def test_handle_initial_state_populates_cells():
    """handleInitialState with one row populates MapState from record."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1", record={"cf1": b"v1", "cf2": b"v2"})
    timer = MagicMock()
    processor.handleInitialState(b"r1", iter([row]), timer)

    assert mock_cells._state == {("cf1",): (b"v1",), ("cf2",): (b"v2",)}


def test_handle_initial_state_empty_record():
    """handleInitialState with empty record does not add entries."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1", record={})
    timer = MagicMock()
    processor.handleInitialState(b"r1", iter([row]), timer)

    assert mock_cells._state == {}


def test_handle_initial_state_multiple_rows_last_wins():
    """handleInitialState with multiple rows for same key: last row wins per key."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    rows = [
        Row(row_key=b"r1", record={"cf1": b"first"}),
        Row(row_key=b"r1", record={"cf1": b"second", "cf2": b"v2"}),
    ]
    timer = MagicMock()
    processor.handleInitialState(b"r1", iter(rows), timer)

    assert mock_cells._state == {("cf1",): (b"second",), ("cf2",): (b"v2",)}


def test_handle_initial_state_skips_row_without_record():
    """handleInitialState skips rows that have no record attribute."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1")  # no record
    timer = MagicMock()
    processor.handleInitialState(b"r1", iter([row]), timer)

    assert mock_cells._state == {}


def _make_mock_map_state():
    """In-memory mock MapState for testing."""
    state = {}

    def update_value(key, value):
        state[key] = value

    def contains_key(key):
        return key in state

    def remove_key(key):
        state.pop(key, None)

    def clear():
        state.clear()

    def iterator():
        return iter([(k, v) for k, v in state.items()])

    mock = MagicMock()
    mock._state = state
    mock.updateValue = update_value
    mock.containsKey = contains_key
    mock.removeKey = remove_key
    mock.clear = clear
    mock.iterator = iterator
    return mock
