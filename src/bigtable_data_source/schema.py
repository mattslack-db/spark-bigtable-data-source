"""Schema definition for Bigtable Change Stream events."""

from pyspark.sql.types import (
    StructType,
    BinaryType,
    StringType,
    TimestampType,
)

CHANGE_STREAM_SCHEMA = (
    StructType()
    .add("row_key", BinaryType())
    .add("column_family", StringType())
    .add("column_qualifier", BinaryType())
    .add("value", BinaryType())
    .add("mutation_type", StringType())
    .add("commit_timestamp", TimestampType())
    .add("partition_start_key", BinaryType())
    .add("partition_end_key", BinaryType())
    .add("low_watermark", TimestampType())
)
