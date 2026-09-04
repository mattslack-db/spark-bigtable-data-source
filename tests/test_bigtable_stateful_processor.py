"""Unit tests for Bigtable stateful processor (transformWithState).

State is per column: keyed by (column_family, column_qualifier). The reconstructed
record maps "column_family:column_qualifier" -> latest value (bytes).
"""

import pytest
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


def test_extract_row_key_empty_tuple_raises():
    """_extract_row_key raises TypeError on an empty grouping key."""
    from bigtable_stateful_processor.processor import _extract_row_key

    with pytest.raises(TypeError, match="empty grouping key"):
        _extract_row_key(())


def test_extract_row_key_unknown_type_raises():
    """_extract_row_key raises TypeError (not returns unchecked) for unknown types."""
    from bigtable_stateful_processor.processor import _extract_row_key

    with pytest.raises(TypeError, match="Cannot extract row_key"):
        _extract_row_key(object())


# ─── record key composition ─────────────────────────────────────────────────


def test_compose_and_split_record_key_round_trip():
    """_split_record_key inverts _compose_record_key for UTF-8 qualifiers."""
    from bigtable_stateful_processor.processor import (
        _compose_record_key,
        _split_record_key,
    )

    composed = _compose_record_key("cf1", b"col1")
    assert composed == "cf1:col1"
    assert _split_record_key(composed) == ("cf1", b"col1")


def test_split_record_key_empty_qualifier():
    """A composed key with empty qualifier round-trips to empty bytes."""
    from bigtable_stateful_processor.processor import (
        _compose_record_key,
        _split_record_key,
    )

    composed = _compose_record_key("cf1", b"")
    assert composed == "cf1:"
    assert _split_record_key(composed) == ("cf1", b"")


def test_split_record_key_qualifier_with_separator():
    """Qualifiers containing ':' keep everything after the first ':' (family has no ':')."""
    from bigtable_stateful_processor.processor import _split_record_key

    assert _split_record_key("cf1:a:b") == ("cf1", b"a:b")


def test_compose_record_key_non_utf8_qualifier_raises():
    """A non-UTF-8 qualifier fails fast instead of silently corrupting the record key."""
    import pytest
    from bigtable_stateful_processor.processor import _compose_record_key

    with pytest.raises(ValueError, match="not valid.*UTF-8"):
        _compose_record_key("cf1", b"\xff\xfe")


# ─── _build_record_from_state ───────────────────────────────────────────────


def test_build_record_from_state_empty():
    """_build_record_from_state returns empty dict when state iterator is empty."""
    from bigtable_stateful_processor.processor import _build_record_from_state

    mock_cells = MagicMock()
    mock_cells.iterator.return_value = iter([])
    assert _build_record_from_state(mock_cells) == {}


def test_build_record_from_state_single_entry():
    """_build_record_from_state maps one (family, qualifier) to 'family:qualifier'."""
    from bigtable_stateful_processor.processor import _build_record_from_state

    mock_cells = MagicMock()
    mock_cells.iterator.return_value = iter([(("cf1", b"col1"), (b"value1",))])
    assert _build_record_from_state(mock_cells) == {"cf1:col1": b"value1"}


def test_build_record_from_state_multiple_entries():
    """_build_record_from_state returns map of all columns to values."""
    from bigtable_stateful_processor.processor import _build_record_from_state

    mock_cells = MagicMock()
    mock_cells.iterator.return_value = iter([
        (("cf1", b"q1"), (b"v1",)),
        (("cf2", b"q2"), (b"v2",)),
    ])
    assert _build_record_from_state(mock_cells) == {"cf1:q1": b"v1", "cf2:q2": b"v2"}


def test_reconstructed_record_schema():
    """RECONSTRUCTED_RECORD_SCHEMA has row_key and record (MapType[string, binary])."""
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
    assert out[0].record == {"cf1:col1": b"value1"}
    assert mock_cells._state == {("cf1", b"col1"): (b"value1",)}


def test_handle_input_rows_two_qualifiers_same_family_coexist():
    """Two SET_CELLs to different qualifiers in one family do NOT clobber each other."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    rows = [
        _set_cell(b"r1", "cf1", b"col_a", b"v1"),
        _set_cell(b"r1", "cf1", b"col_b", b"v2"),
    ]
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter(rows), timer))

    assert out[0].record == {"cf1:col_a": b"v1", "cf1:col_b": b"v2"}
    assert mock_cells._state == {
        ("cf1", b"col_a"): (b"v1",),
        ("cf1", b"col_b"): (b"v2",),
    }


def test_handle_input_rows_two_families_emits_combined_record():
    """handleInputRows with two SET_CELLs (different families) emits record with both."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    rows = [
        _set_cell(b"r1", "cf1", b"q1", b"v1"),
        _set_cell(b"r1", "cf2", b"q2", b"v2"),
    ]
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter(rows), timer))

    assert len(out) == 1
    assert out[0].record == {"cf1:q1": b"v1", "cf2:q2": b"v2"}
    assert mock_cells._state == {("cf1", b"q1"): (b"v1",), ("cf2", b"q2"): (b"v2",)}


def test_handle_input_rows_delete_column_removes_only_that_column():
    """DELETE_COLUMN removes only the targeted column; siblings in the family survive."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    mock_cells.updateValue(("cf1", b"col_a"), (b"v1",))
    mock_cells.updateValue(("cf1", b"col_b"), (b"v2",))
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = _delete(b"r1", "cf1", b"col_b", "DELETE_COLUMN")
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert out[0].record == {"cf1:col_a": b"v1"}
    assert mock_cells._state == {("cf1", b"col_a"): (b"v1",)}


def test_handle_input_rows_delete_family_removes_all_columns_in_family():
    """DELETE_FAMILY removes every column in that family, leaving other families."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    mock_cells.updateValue(("cf1", b"col_a"), (b"v1",))
    mock_cells.updateValue(("cf1", b"col_b"), (b"v2",))
    mock_cells.updateValue(("cf2", b"col_c"), (b"v3",))
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = _delete(b"r1", "cf1", b"col_a", "DELETE_FAMILY")
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert out[0].record == {"cf2:col_c": b"v3"}
    assert mock_cells._state == {("cf2", b"col_c"): (b"v3",)}


def test_handle_input_rows_delete_row_clears_state():
    """handleInputRows with DELETE_ROW clears state and emits empty record."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    mock_cells.updateValue(("cf1", b"q1"), (b"v1",))
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = _delete(b"r1", "cf1", b"q1", "DELETE_ROW")
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
    mock_cells.updateValue(("cf1", b"q1"), (b"v1",))
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = _delete(b"r1", "cf1", b"q1", "UNKNOWN")
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert len(out) == 1
    assert out[0].record == {"cf1:q1": b"v1"}
    assert mock_cells._state == {("cf1", b"q1"): (b"v1",)}


def test_handle_input_rows_column_family_bytes_decoded():
    """handleInputRows decodes column_family from bytes to string."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = _set_cell(b"r1", b"cf1", b"q1", b"v1")
    timer = MagicMock()
    out = list(processor.handleInputRows(b"r1", iter([row]), timer))

    assert out[0].record == {"cf1:q1": b"v1"}


def test_processor_close_noop():
    """Processor close() does not raise."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    processor.close()


def test_handle_initial_state_populates_cells():
    """handleInitialState with one row populates MapState from a composite-keyed record."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    row = Row(row_key=b"r1", record={"cf1:q1": b"v1", "cf2:q2": b"v2"})
    timer = MagicMock()
    processor.handleInitialState(b"r1", iter([row]), timer)

    assert mock_cells._state == {("cf1", b"q1"): (b"v1",), ("cf2", b"q2"): (b"v2",)}


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
    """handleInitialState with multiple rows for same key: last row wins per column."""
    from bigtable_stateful_processor.processor import BigtableReconstructProcessor

    processor = BigtableReconstructProcessor()
    mock_cells = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = mock_cells

    rows = [
        Row(row_key=b"r1", record={"cf1:q1": b"first"}),
        Row(row_key=b"r1", record={"cf1:q1": b"second", "cf2:q2": b"v2"}),
    ]
    timer = MagicMock()
    processor.handleInitialState(b"r1", iter(rows), timer)

    assert mock_cells._state == {("cf1", b"q1"): (b"second",), ("cf2", b"q2"): (b"v2",)}


def test_handle_initial_state_round_trips_reconstructed_record():
    """A record built by _build_record_from_state reloads into identical state."""
    from bigtable_stateful_processor.processor import (
        BigtableReconstructProcessor,
        _build_record_from_state,
    )

    source = _make_mock_map_state()
    source.updateValue(("cf1", b"col_a"), (b"v1",))
    source.updateValue(("cf1", b"col_b"), (b"v2",))
    source.updateValue(("cf2", b"col_c"), (b"v3",))
    record = _build_record_from_state(source)

    processor = BigtableReconstructProcessor()
    target = _make_mock_map_state()
    processor._handle = MagicMock()
    processor._cells = target
    processor.handleInitialState(b"r1", iter([Row(row_key=b"r1", record=record)]), MagicMock())

    assert target._state == source._state


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


def _set_cell(row_key, column_family, column_qualifier, value):
    return Row(
        row_key=row_key,
        column_family=column_family,
        column_qualifier=column_qualifier,
        value=value,
        mutation_type="SET_CELL",
        commit_timestamp=None,
        partition_start_key=b"",
        partition_end_key=b"",
        low_watermark=None,
    )


def _delete(row_key, column_family, column_qualifier, mutation_type):
    return Row(
        row_key=row_key,
        column_family=column_family,
        column_qualifier=column_qualifier,
        value=b"",
        mutation_type=mutation_type,
        commit_timestamp=None,
        partition_start_key=b"",
        partition_end_key=b"",
        low_watermark=None,
    )


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
