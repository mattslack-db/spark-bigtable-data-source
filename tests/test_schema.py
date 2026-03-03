"""Tests for Bigtable Change Stream schema definition."""

from pyspark.sql.types import (
    StructType,
    BinaryType,
    StringType,
    TimestampType,
)


def test_schema_is_struct_type():
    """Test schema is a StructType."""
    from bigtable_data_source.schema import CHANGE_STREAM_SCHEMA

    assert isinstance(CHANGE_STREAM_SCHEMA, StructType)


def test_schema_has_expected_fields():
    """Test schema contains all expected fields."""
    from bigtable_data_source.schema import CHANGE_STREAM_SCHEMA

    field_names = [f.name for f in CHANGE_STREAM_SCHEMA.fields]

    assert field_names == [
        "row_key",
        "column_family",
        "column_qualifier",
        "value",
        "mutation_type",
        "commit_timestamp",
        "partition_key",
        "low_watermark",
    ]


def test_schema_field_types():
    """Test schema field types are correct."""
    from bigtable_data_source.schema import CHANGE_STREAM_SCHEMA

    type_map = {f.name: f.dataType for f in CHANGE_STREAM_SCHEMA.fields}

    assert type_map["row_key"] == BinaryType()
    assert type_map["column_family"] == StringType()
    assert type_map["column_qualifier"] == BinaryType()
    assert type_map["value"] == BinaryType()
    assert type_map["mutation_type"] == StringType()
    assert type_map["commit_timestamp"] == TimestampType()
    assert type_map["partition_key"] == StringType()
    assert type_map["low_watermark"] == TimestampType()


def test_schema_field_count():
    """Test schema has exactly 8 fields."""
    from bigtable_data_source.schema import CHANGE_STREAM_SCHEMA

    assert len(CHANGE_STREAM_SCHEMA.fields) == 8
