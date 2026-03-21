"""Shared mutation type constants for Bigtable change streams."""

from enum import Enum


class MutationType(str, Enum):
    """Mutation types emitted by the change stream and consumed by stateful processors."""

    SET_CELL = "SET_CELL"
    DELETE_COLUMN = "DELETE_COLUMN"
    DELETE_FAMILY = "DELETE_FAMILY"
    DELETE_ROW = "DELETE_ROW"
