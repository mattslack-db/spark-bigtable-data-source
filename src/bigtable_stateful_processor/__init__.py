"""
Stateful processor to reconstruct full Bigtable records from the change stream.

Consumes the bigtable_changes stream, uses row_key as state key, maintains a MapState
with one entry per column family (latest value per family), and emits the full record
on every update.
"""

from .processor import BigtableReconstructProcessor
from .schema import RECONSTRUCTED_RECORD_SCHEMA

__all__ = [
    "BigtableReconstructProcessor",
    "RECONSTRUCTED_RECORD_SCHEMA",
]
