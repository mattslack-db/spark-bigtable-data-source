"""Bigtable Change Stream Data Source implementation."""

from pyspark.sql.datasource import DataSource
from pyspark.sql.types import StructType

from .schema import CHANGE_STREAM_SCHEMA
from .stream_reader import BigtableChangeStreamReader


class BigtableChangeStreamDataSource(DataSource):
    """PySpark Data Source for Google Cloud Bigtable Change Streams."""

    @classmethod
    def name(cls):
        return "bigtable_changes"

    def __init__(self, options):
        self.options = options

    def schema(self) -> StructType:
        return CHANGE_STREAM_SCHEMA

    def streamReader(self, schema: StructType) -> BigtableChangeStreamReader:
        return BigtableChangeStreamReader(self.options)
