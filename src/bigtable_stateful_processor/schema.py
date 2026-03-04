"""Output schema for the reconstructed record from the stateful processor."""

from pyspark.sql.types import BinaryType, MapType, StringType, StructType

# row_key: bytes, record: map from column_family (string) -> latest value (bytes)
RECONSTRUCTED_RECORD_SCHEMA = (
    StructType()
    .add("row_key", BinaryType())
    .add("record", MapType(StringType(), BinaryType()))
)
