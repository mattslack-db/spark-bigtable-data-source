"""Tests for Bigtable Change Stream reader logic."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch


def test_stream_reader_validates_required_options():
    """Test reader validates required options."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    with pytest.raises(ValueError, match="Missing required options"):
        BigtableStreamReader({})


def test_stream_reader_missing_project_id():
    """Test missing project_id raises ValueError."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    with pytest.raises(ValueError, match="project_id"):
        BigtableStreamReader({"instance_id": "i", "table_id": "t"})


def test_stream_reader_missing_instance_id():
    """Test missing instance_id raises ValueError."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    with pytest.raises(ValueError, match="instance_id"):
        BigtableStreamReader({"project_id": "p", "table_id": "t"})


def test_stream_reader_missing_table_id():
    """Test missing table_id raises ValueError."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    with pytest.raises(ValueError, match="table_id"):
        BigtableStreamReader({"project_id": "p", "instance_id": "i"})


def test_stream_reader_init_with_valid_options(basic_options):
    """Test reader initializes with valid options."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)

    assert reader.project_id == "test-project"
    assert reader.instance_id == "test-instance"
    assert reader.table_id == "test-table"
    assert reader.app_profile == "default"
    assert reader.batch_seconds == 10
    assert reader.max_rows_per_partition == 5000


def test_stream_reader_custom_options():
    """Test reader respects custom option values."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    options = {
        "project_id": "my-project",
        "instance_id": "my-instance",
        "table_id": "my-table",
        "app_profile_id": "custom-profile",
        "batch_duration_seconds": "30",
        "max_rows_per_partition": "1000",
    }

    reader = BigtableStreamReader(options)

    assert reader.app_profile == "custom-profile"
    assert reader.batch_seconds == 30
    assert reader.max_rows_per_partition == 1000


def test_stream_reader_does_not_connect_on_init(basic_options):
    """Test that __init__ does NOT create a Bigtable client."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)

    assert reader._client is None
    assert reader._table is None


def _make_partitions(n):
    """Create n BigtablePartition instances for mocking _fetch_partition_metadata."""
    from bigtable_data_source.partitioning import BigtablePartition

    return [
        BigtablePartition(i, b"" if i == 0 else b"mid", b"mid" if i == 0 else b"", None)
        for i in range(n)
    ]


def test_initial_offset_discovers_partitions(basic_options):
    """Test initialOffset returns one token entry per partition from _fetch_partition_metadata."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)
    three = _make_partitions(3)

    with patch.object(reader, "_fetch_partition_metadata", return_value=three):
        offset = reader.initialOffset()

    assert len(offset) == 3
    assert offset == {"0": None, "1": None, "2": None}


def test_initial_offset_single_partition(basic_options):
    """Test initialOffset with a single partition."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)
    one = _make_partitions(1)

    with patch.object(reader, "_fetch_partition_metadata", return_value=one):
        offset = reader.initialOffset()

    assert len(offset) == 1
    assert offset == {"0": None}


def test_partitions_returns_bigtable_partitions(basic_options):
    """Test partitions() returns BigtablePartition objects with buffered rows."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    two = _make_partitions(2)

    def _side_effect():
        reader._partitions = {i: two[i] for i in range(len(two))}
        reader._tokens = {i: None for i in range(len(two))}
        return two

    with patch.object(reader, "_fetch_partition_metadata", side_effect=_side_effect):
        reader.initialOffset()

    start = {"0": None, "1": None}
    end = {"0": "token-a", "1": "token-b"}

    parts = reader.partitions(start, end)

    assert len(parts) == 2
    for p in parts:
        assert isinstance(p, BigtablePartition)


def test_read_yields_buffered_rows(basic_options):
    """Test read() yields rows from the partition's carried rows (production path)."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    row_dict = {
        "row_key": b"key-1",
        "column_family": "cf1",
        "column_qualifier": b"col1",
        "value": b"val1",
        "mutation_type": "SET_CELL",
        "commit_timestamp": ts,
        "partition_key": "b''-b'row-500'",
        "low_watermark": ts,
    }
    partition = BigtablePartition(0, b"", b"row-500", None, rows=[row_dict])
    result = list(reader.read(partition))

    assert len(result) == 1
    assert result[0] == (
        b"key-1",
        "cf1",
        b"col1",
        b"val1",
        "SET_CELL",
        ts,
        "b''-b'row-500'",
        ts,
    )


def test_read_empty_partition(basic_options):
    """Test read() returns nothing for a partition with no buffered data."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    reader._buffered_rows = {}

    partition = BigtablePartition(5, b"", b"", None)
    result = list(reader.read(partition))

    assert result == []


def test_commit_does_not_raise(basic_options):
    """Test commit() completes without error."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)
    reader.commit({"0": "some-token"})


def test_stop_closes_client(basic_options):
    """Test stop() closes the Bigtable client."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)
    mock_client = MagicMock()
    reader._client = mock_client

    reader.stop()

    mock_client.close.assert_called_once()


def test_stop_noop_when_no_client(basic_options):
    """Test stop() is safe to call when client was never created."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)
    reader.stop()


def test_parse_mutation_set_cell(basic_options):
    """Test _parse_mutation parses SET_CELL mutations."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    ts = datetime(2025, 6, 1, tzinfo=timezone.utc)

    chunk = MagicMock()
    chunk.row_key = b"row-1"
    chunk.set_cell = MagicMock(family_name="cf1", column_qualifier=b"col1", value=b"value1")
    chunk.delete_from_column = None
    chunk.delete_from_family = None
    chunk.delete_from_row = None

    partition = BigtablePartition(0, b"", b"end", None)

    result = reader._parse_mutation(chunk, chunk.row_key, ts, ts, partition)

    assert result is not None
    assert result["row_key"] == b"row-1"
    assert result["column_family"] == "cf1"
    assert result["column_qualifier"] == b"col1"
    assert result["value"] == b"value1"
    assert result["mutation_type"] == "SET_CELL"
    assert result["commit_timestamp"] == ts


def test_parse_mutation_delete_row(basic_options):
    """Test _parse_mutation parses DELETE_ROW mutations."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    ts = datetime(2025, 6, 1, tzinfo=timezone.utc)

    chunk = MagicMock()
    chunk.row_key = b"row-2"
    chunk.set_cell = None
    chunk.delete_from_column = None
    chunk.delete_from_family = None
    chunk.delete_from_row = MagicMock()  # truthy

    partition = BigtablePartition(0, b"", b"end", None)

    result = reader._parse_mutation(chunk, chunk.row_key, ts, ts, partition)

    assert result is not None
    assert result["mutation_type"] == "DELETE_ROW"
    assert result["value"] == b""


def test_parse_mutation_unknown_type(basic_options):
    """Test _parse_mutation returns None for unknown mutation types."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    ts = datetime(2025, 6, 1, tzinfo=timezone.utc)

    chunk = MagicMock()
    chunk.row_key = b"row-3"
    chunk.set_cell = None
    chunk.delete_from_column = None
    chunk.delete_from_family = None
    chunk.delete_from_row = None

    partition = BigtablePartition(0, b"", b"end", None)

    result = reader._parse_mutation(chunk, chunk.row_key, ts, ts, partition)

    assert result is None
