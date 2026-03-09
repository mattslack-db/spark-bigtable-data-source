"""Tests for Bigtable Change Stream data source."""

from pyspark.sql.types import StructType


def test_data_source_name():
    """Test that data source name is 'bigtable_changes'."""
    from bigtable_data_source import BigtableChangeStreamDataSource

    assert BigtableChangeStreamDataSource.name() == "bigtable_changes"


def test_data_source_schema_returns_fixed_schema():
    """Test that schema() returns the fixed change stream schema."""
    from bigtable_data_source import BigtableChangeStreamDataSource, CHANGE_STREAM_SCHEMA

    options = {
        "project_id": "test-project",
        "instance_id": "test-instance",
        "table_id": "test-table",
    }

    ds = BigtableChangeStreamDataSource(options)
    schema = ds.schema()

    assert isinstance(schema, StructType)
    assert schema == CHANGE_STREAM_SCHEMA

    field_names = [f.name for f in schema.fields]
    assert "row_key" in field_names
    assert "column_family" in field_names
    assert "column_qualifier" in field_names
    assert "value" in field_names
    assert "mutation_type" in field_names
    assert "commit_timestamp" in field_names
    assert "partition_start_key" in field_names
    assert "partition_end_key" in field_names
    assert "low_watermark" in field_names


def test_data_source_stream_reader_method(basic_options):
    """Test that streamReader() returns a BigtableChangeStreamReader."""
    from bigtable_data_source import BigtableChangeStreamDataSource, BigtableChangeStreamReader

    ds = BigtableChangeStreamDataSource(basic_options)
    reader = ds.streamReader(ds.schema())

    assert isinstance(reader, BigtableChangeStreamReader)
    assert reader.project_id == "test-project"
    assert reader.instance_id == "test-instance"
    assert reader.table_id == "test-table"


def test_data_source_options_passed_to_reader(basic_options):
    """Test that all options are forwarded to the stream reader."""
    from bigtable_data_source import BigtableChangeStreamDataSource

    options = {
        **basic_options,
        "app_profile_id": "my-profile",
        "batch_duration_seconds": "20",
        "max_rows_per_partition": "10000",
    }

    ds = BigtableChangeStreamDataSource(options)
    reader = ds.streamReader(ds.schema())

    assert reader.app_profile == "my-profile"
    assert reader.batch_seconds == 20
    assert reader.max_rows_per_partition == 10000
