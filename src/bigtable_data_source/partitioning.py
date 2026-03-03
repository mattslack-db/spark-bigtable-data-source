"""Partitioning utilities for Bigtable Change Stream tablet partitions."""

from pyspark.sql.datasource import InputPartition


class BigtablePartition(InputPartition):
    """
    Represents a Bigtable tablet partition for change stream reading.

    Each partition corresponds to one tablet's row key range in the
    Bigtable table. The token is a continuation token for resuming
    the change stream from where the last micro-batch left off.
    Optional rows are buffered rows carried to the executor for read().
    """

    def __init__(self, partition_index, start_key, end_key, token=None, rows=None):
        self.partition_index = partition_index
        self.start_key = start_key
        self.end_key = end_key
        self.token = token
        self.rows = list(rows) if rows is not None else []

    def __eq__(self, other):
        if not isinstance(other, BigtablePartition):
            return False
        return (
            self.partition_index == other.partition_index
            and self.start_key == other.start_key
            and self.end_key == other.end_key
            and self.token == other.token
        )

    def __hash__(self):
        return hash((self.partition_index, self.start_key, self.end_key, self.token))

    def __repr__(self):
        return (
            f"BigtablePartition(partition_index={self.partition_index}, "
            f"start_key={self.start_key!r}, end_key={self.end_key!r}, "
            f"token={self.token!r})"
        )
