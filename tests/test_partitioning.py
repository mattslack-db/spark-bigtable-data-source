"""Tests for Bigtable partition utilities."""


def test_bigtable_partition_creation():
    """Test creating a Bigtable partition."""
    from bigtable_data_source.partitioning import BigtablePartition

    partition = BigtablePartition(0, b"", b"row-500", None)

    assert partition.partition_index == 0
    assert partition.start_key == b""
    assert partition.end_key == b"row-500"
    assert partition.token is None
    assert partition.rows == []


def test_bigtable_partition_with_token():
    """Test partition with a continuation token."""
    from bigtable_data_source.partitioning import BigtablePartition

    partition = BigtablePartition(1, b"row-500", b"row-999", "abc123")

    assert partition.token == "abc123"


def test_bigtable_partition_equality():
    """Test partition equality comparison."""
    from bigtable_data_source.partitioning import BigtablePartition

    p1 = BigtablePartition(0, b"", b"row-500", None)
    p2 = BigtablePartition(0, b"", b"row-500", None)
    p3 = BigtablePartition(1, b"row-500", b"", None)

    assert p1 == p2
    assert p1 != p3


def test_bigtable_partition_equality_with_token():
    """Test partitions with different tokens are not equal."""
    from bigtable_data_source.partitioning import BigtablePartition

    p1 = BigtablePartition(0, b"", b"row-500", None)
    p2 = BigtablePartition(0, b"", b"row-500", "token-1")

    assert p1 != p2


def test_bigtable_partition_hash():
    """Test partition can be used in sets/dicts."""
    from bigtable_data_source.partitioning import BigtablePartition

    p1 = BigtablePartition(0, b"", b"row-500", None)
    p2 = BigtablePartition(0, b"", b"row-500", None)
    p3 = BigtablePartition(1, b"row-500", b"", None)

    s = {p1, p2, p3}
    assert len(s) == 2


def test_bigtable_partition_repr():
    """Test partition string representation."""
    from bigtable_data_source.partitioning import BigtablePartition

    partition = BigtablePartition(2, b"abc", b"xyz", "tok-1")

    r = repr(partition)
    assert "partition_index=2" in r
    assert "abc" in r
    assert "xyz" in r
    assert "tok-1" in r


def test_bigtable_partition_not_equal_to_other_type():
    """Test partition is not equal to non-partition objects."""
    from bigtable_data_source.partitioning import BigtablePartition

    partition = BigtablePartition(0, b"", b"", None)
    assert partition != "not a partition"
    assert partition != 42
