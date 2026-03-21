"""Tests for Bigtable Change Stream reader logic."""

import logging

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


def test_batch_duration_seconds_invalid_raises_value_error(basic_options):
    """batch_duration_seconds must be a positive integer."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    for invalid in ("x", "0", "-1", "1.5", ""):
        options = {**basic_options, "batch_duration_seconds": invalid}
        with pytest.raises(ValueError, match="batch_duration_seconds"):
            BigtableStreamReader(options)


def test_max_rows_per_partition_invalid_raises_value_error(basic_options):
    """max_rows_per_partition must be a positive integer."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    for invalid in ("x", "0", "-1", "1.5", ""):
        options = {**basic_options, "max_rows_per_partition": invalid}
        with pytest.raises(ValueError, match="max_rows_per_partition"):
            BigtableStreamReader(options)


def test_stream_reader_does_not_connect_on_init(basic_options):
    """Test that __init__ does NOT create a Bigtable client."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)

    assert reader._client is None
    assert reader._table is None


def test_credentials_json_logs_security_warning(basic_options, caplog):
    """credentials_json triggers a warning about private key exposure in job config."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    caplog.set_level(logging.WARNING, logger="bigtable_data_source.stream_reader")
    BigtableStreamReader({**basic_options, "credentials_json": "{}"})
    assert any("credentials_json" in r.message for r in caplog.records)
    assert any(
        "Application Default Credentials" in r.message or "ADC" in r.message
        for r in caplog.records
    )


def test_credentials_json_invalid_raises_value_error():
    """Malformed credentials_json must raise, not silently fall back to ADC."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(
        {
            "project_id": "p",
            "instance_id": "i",
            "table_id": "t",
            "credentials_json": "not valid json",
        }
    )
    with pytest.raises(ValueError, match="credentials_json option is invalid"):
        reader._get_client()


def test_credentials_json_not_sa_dict_raises_value_error():
    """Valid JSON but missing service account fields must raise."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    import json

    reader = BigtableStreamReader(
        {
            "project_id": "p",
            "instance_id": "i",
            "table_id": "t",
            "credentials_json": json.dumps({"foo": "bar"}),
        }
    )
    with pytest.raises(ValueError, match="credentials_json option is invalid"):
        reader._get_client()


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
        "partition_start_key": b"",
        "partition_end_key": b"row-500",
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
        b"",
        b"row-500",
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


# ─── start_timestamp option ─────────────────────────────────────────────────


def test_parse_start_timestamp_none():
    """_parse_start_timestamp returns None when value is None."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    assert BigtableStreamReader._parse_start_timestamp(None) is None


def test_parse_start_timestamp_empty_string():
    """_parse_start_timestamp returns None for empty or whitespace string."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    assert BigtableStreamReader._parse_start_timestamp("") is None
    assert BigtableStreamReader._parse_start_timestamp("   ") is None


def test_parse_start_timestamp_iso_utc():
    """_parse_start_timestamp parses ISO 8601 with Z as UTC."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    dt = BigtableStreamReader._parse_start_timestamp("2025-03-01T12:00:00Z")
    assert dt is not None
    assert dt.year == 2025 and dt.month == 3 and dt.day == 1
    assert dt.hour == 12 and dt.minute == 0 and dt.second == 0
    assert dt.tzinfo == timezone.utc


def test_parse_start_timestamp_iso_naive_treated_as_utc():
    """_parse_start_timestamp treats naive ISO datetime as UTC."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    dt = BigtableStreamReader._parse_start_timestamp("2025-03-01T12:00:00")
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.hour == 12


def test_parse_start_timestamp_unix_int():
    """_parse_start_timestamp accepts Unix timestamp (int) in seconds."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    # 2025-03-01 12:00:00 UTC
    unix_ts = 1740823200
    dt = BigtableStreamReader._parse_start_timestamp(unix_ts)
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert int(dt.timestamp()) == unix_ts


def test_parse_start_timestamp_unix_float():
    """_parse_start_timestamp accepts Unix timestamp (float) in seconds."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    dt = BigtableStreamReader._parse_start_timestamp(1740823200.5)
    assert dt is not None
    assert dt.tzinfo == timezone.utc
    assert dt.second == 0
    assert dt.microsecond == 500_000


def test_stream_reader_init_stores_start_timestamp(basic_options):
    """Reader stores _start_timestamp when start_timestamp option is set."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(
        {**basic_options, "start_timestamp": "2025-03-01T00:00:00Z"}
    )
    assert reader._start_timestamp is not None
    assert reader._start_timestamp.year == 2025 and reader._start_timestamp.month == 3


def test_stream_reader_init_no_start_timestamp_by_default(basic_options):
    """Reader has _start_timestamp None when option is not set."""
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)
    assert reader._start_timestamp is None


def test_read_partition_chunk_uses_start_timestamp_when_no_token(basic_options):
    """When no continuation token, _read_partition_chunk sets request start_time from start_timestamp."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    options = {**basic_options, "start_timestamp": "2025-03-01T12:00:00Z"}
    reader = BigtableStreamReader(options)
    reader._tokens = {}  # no token
    reader._raw_partitions = {}
    partition = BigtablePartition(0, b"", b"\xff\xff", None)

    captured_request = None

    def fake_read_change_stream(request=None):
        nonlocal captured_request
        captured_request = request
        # Yield a heartbeat so the loop exits (3 heartbeats with no data)
        hb = MagicMock()
        hb.estimated_low_watermark = None
        hb.continuation_token = MagicMock()
        hb.continuation_token.token = "next-token"
        response = MagicMock()
        response.heartbeat = hb
        response.close_stream = None
        response.data_change = None
        for _ in range(3):
            yield response
        return

    mock_table = MagicMock()
    mock_table.name = "projects/p/instances/i/tables/t"
    mock_table._instance._client.table_data_client.read_change_stream = fake_read_change_stream

    with patch.object(reader, "_get_client", return_value=(MagicMock(), mock_table)):
        rows, new_token = reader._read_partition_chunk(partition)

    assert captured_request is not None
    assert "start_time" in captured_request
    assert reader._start_timestamp is not None
    assert captured_request["start_time"].seconds == int(reader._start_timestamp.timestamp())
    assert captured_request["start_time"].nanos == 0
    assert new_token == "next-token"


def test_read_partition_chunk_uses_now_when_no_start_timestamp(basic_options):
    """When no token and no start_timestamp, request uses current time (we only assert start_time is set)."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    reader._tokens = {}
    reader._raw_partitions = {}
    partition = BigtablePartition(0, b"", b"\xff\xff", None)

    captured_request = None

    def fake_read_change_stream(request=None):
        nonlocal captured_request
        captured_request = request
        hb = MagicMock()
        hb.estimated_low_watermark = None
        hb.continuation_token = MagicMock()
        hb.continuation_token.token = "t"
        response = MagicMock()
        response.heartbeat = hb
        response.close_stream = None
        response.data_change = None
        for _ in range(3):
            yield response
        return

    mock_table = MagicMock()
    mock_table.name = "projects/p/instances/i/tables/t"
    mock_table._instance._client.table_data_client.read_change_stream = fake_read_change_stream

    with patch.object(reader, "_get_client", return_value=(MagicMock(), mock_table)):
        reader._read_partition_chunk(partition)

    assert captured_request is not None
    assert "start_time" in captured_request
    assert captured_request["start_time"].seconds >= 0


def test_read_partition_chunk_uses_continuation_token_when_provided(basic_options):
    """When a continuation token is set for the partition, request uses continuation_tokens and no start_time."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    reader._tokens = {0: "saved-continuation-token"}
    reader._raw_partitions = {}
    partition = BigtablePartition(0, b"", b"\xff\xff", None)

    captured_request = None

    def fake_read_change_stream(request=None):
        nonlocal captured_request
        captured_request = request
        hb = MagicMock()
        hb.estimated_low_watermark = None
        hb.continuation_token = MagicMock()
        hb.continuation_token.token = "next-token"
        response = MagicMock()
        response.heartbeat = hb
        response.close_stream = None
        response.data_change = None
        for _ in range(3):
            yield response
        return

    mock_table = MagicMock()
    mock_table.name = "projects/p/instances/i/tables/t"
    mock_table._instance._client.table_data_client.read_change_stream = fake_read_change_stream

    with patch.object(reader, "_get_client", return_value=(MagicMock(), mock_table)):
        rows, new_token = reader._read_partition_chunk(partition)

    assert captured_request is not None
    assert "continuation_tokens" in captured_request
    tokens_cfg = captured_request["continuation_tokens"]
    assert "tokens" in tokens_cfg
    assert len(tokens_cfg["tokens"]) == 1
    assert tokens_cfg["tokens"][0]["token"] == "saved-continuation-token"
    assert "start_time" not in captured_request
    assert new_token == "next-token"


# --- _to_datetime_utc, latestOffset, _fetch_partition_metadata ---


def test_to_datetime_utc_none():
    from bigtable_data_source.stream_reader import _to_datetime_utc

    assert _to_datetime_utc(None) is None


def test_to_datetime_utc_aware_datetime_via_timestamp():
    from bigtable_data_source.stream_reader import _to_datetime_utc

    dt = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    out = _to_datetime_utc(dt)
    assert out.tzinfo == timezone.utc
    assert abs((out - dt).total_seconds()) < 1e-6


def test_to_datetime_utc_naive_datetime_gets_utc():
    from bigtable_data_source.stream_reader import _to_datetime_utc

    dt = datetime(2025, 1, 1, 12, 0, 0)
    out = _to_datetime_utc(dt)
    assert out.tzinfo == timezone.utc


def test_to_datetime_utc_proto_to_datetime():
    from bigtable_data_source.stream_reader import _to_datetime_utc

    ts = MagicMock()
    aware = datetime(2025, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    ts.ToDatetime = MagicMock(return_value=aware)
    assert _to_datetime_utc(ts) == aware


def test_to_datetime_utc_object_with_timestamp_method():
    from bigtable_data_source.stream_reader import _to_datetime_utc

    class HasTimestamp:
        def timestamp(self):
            return 1704067200.0

    out = _to_datetime_utc(HasTimestamp())
    assert out.year == 2024
    assert out.tzinfo == timezone.utc


def test_latest_offset_self_initializes_when_initial_offset_not_called(basic_options):
    """When latestOffset() is called first (e.g. Spark uses a new reader), it discovers partitions then runs."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    p = BigtablePartition(0, b"a", b"b", None)

    def fake_fetch():
        reader._partitions = {0: p}
        reader._tokens = {0: None}
        reader._raw_partitions = {0: MagicMock()}
        return [p]

    with patch.object(
        reader,
        "_fetch_partition_metadata",
        side_effect=fake_fetch,
    ) as mock_fetch:
        with patch.object(
            reader,
            "_read_partition_chunk",
            return_value=([], "token"),
        ):
            offsets = reader.latestOffset()
    mock_fetch.assert_called_once()
    assert reader._initial_offset_completed is True
    assert offsets == {"0": "token"}


def test_read_partition_chunk_wall_clock_timeout(basic_options):
    """Stalled stream yields no further iterations once wall-clock budget is exceeded."""
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(
        {**basic_options, "read_stream_timeout_seconds": "60"}
    )
    reader._tokens = {0: None}
    reader._raw_partitions = {}
    partition = BigtablePartition(0, b"", b"\xff\xff", None)

    def infinite_heartbeats():
        while True:
            hb = MagicMock()
            hb.estimated_low_watermark = None
            hb.continuation_token = MagicMock()
            hb.continuation_token.token = "tok"
            r = MagicMock()
            r.heartbeat = hb
            r.close_stream = None
            r.data_change = None
            yield r

    mock_table = MagicMock()
    mock_table.name = "projects/p/instances/i/tables/t"
    mock_table._instance._client.table_data_client.read_change_stream = (
        lambda **kw: infinite_heartbeats()
    )
    with patch.object(reader, "_get_client", return_value=(MagicMock(), mock_table)):
        with patch(
            "bigtable_data_source.stream_reader.time.monotonic",
            side_effect=[0.0, 0.0, 1e9],
        ):
            rows, new_token = reader._read_partition_chunk(partition)
    assert rows == []
    assert new_token == "tok"


def test_read_partition_chunk_permission_denied_raises(basic_options):
    from google.api_core.exceptions import PermissionDenied

    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    reader._tokens = {0: None}
    reader._raw_partitions = {}
    partition = BigtablePartition(0, b"", b"\xff\xff", None)
    mock_table = MagicMock()
    mock_table.name = "projects/p/instances/i/tables/t"
    mock_table._instance._client.table_data_client.read_change_stream = MagicMock(
        side_effect=PermissionDenied("denied")
    )
    with patch.object(reader, "_get_client", return_value=(MagicMock(), mock_table)):
        with pytest.raises(PermissionDenied):
            reader._read_partition_chunk(partition)


def test_read_partition_chunk_unauthenticated_raises(basic_options):
    from google.api_core.exceptions import Unauthenticated

    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    reader._tokens = {0: None}
    reader._raw_partitions = {}
    partition = BigtablePartition(0, b"", b"\xff\xff", None)
    mock_table = MagicMock()
    mock_table.name = "projects/p/instances/i/tables/t"
    mock_table._instance._client.table_data_client.read_change_stream = MagicMock(
        side_effect=Unauthenticated("invalid credentials")
    )
    with patch.object(reader, "_get_client", return_value=(MagicMock(), mock_table)):
        with pytest.raises(Unauthenticated):
            reader._read_partition_chunk(partition)


def test_read_stream_timeout_seconds_invalid_raises(basic_options):
    from bigtable_data_source.stream_reader import BigtableStreamReader

    with pytest.raises(ValueError, match="read_stream_timeout_seconds"):
        BigtableStreamReader({**basic_options, "read_stream_timeout_seconds": "0"})


def test_latest_offset_buffers_rows_and_returns_tokens(basic_options):
    from bigtable_data_source.stream_reader import BigtableStreamReader
    from bigtable_data_source.partitioning import BigtablePartition

    reader = BigtableStreamReader(basic_options)
    reader._initial_offset_completed = True
    p = BigtablePartition(0, b"a", b"b", None)
    reader._partitions = {0: p}
    reader._tokens = {0: None}
    reader._raw_partitions = {0: MagicMock()}
    one_row = {
        "row_key": b"rk",
        "column_family": "cf",
        "column_qualifier": b"cq",
        "value": b"v",
        "mutation_type": "SET_CELL",
        "commit_timestamp": datetime.now(timezone.utc),
        "partition_start_key": b"a",
        "partition_end_key": b"b",
        "low_watermark": None,
    }
    with patch.object(
        reader,
        "_read_partition_chunk",
        return_value=([one_row], "continuation-xyz"),
    ):
        offsets = reader.latestOffset()
    assert offsets == {"0": "continuation-xyz"}
    assert reader._buffered_rows[0] == [one_row]
    assert reader._tokens[0] == "continuation-xyz"


def test_fetch_partition_metadata(basic_options):
    from bigtable_data_source.stream_reader import BigtableStreamReader

    reader = BigtableStreamReader(basic_options)
    rr = MagicMock()
    rr.start_key_closed = b"\x00\x01"
    rr.end_key_open = b"\x00\x02"
    part = MagicMock()
    part.row_range = rr
    resp = MagicMock()
    resp.partition = part
    mock_table = MagicMock()
    mock_table.name = "projects/p/instances/i/tables/t"
    dc = MagicMock()
    dc.generate_initial_change_stream_partitions = MagicMock(return_value=iter([resp]))
    mock_table._instance._client.table_data_client = dc
    with patch.object(reader, "_get_client", return_value=(MagicMock(), mock_table)):
        partitions = reader._fetch_partition_metadata()
    assert len(partitions) == 1
    assert partitions[0].partition_index == 0
    assert partitions[0].start_key == b"\x00\x01"
    assert partitions[0].end_key == b"\x00\x02"
    assert 0 in reader._partitions
