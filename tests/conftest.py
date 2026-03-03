"""Pytest configuration and shared fixtures."""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

# Ensure the package is importable when running pytest from repo root without pip install -e .
_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC = _REPO_ROOT / "src"
if _SRC.exists() and str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


@pytest.fixture(autouse=True)
def _mock_bigtable_for_unit_tests(request):
    """Make google.cloud.bigtable importable for unit tests (not integration) so reader __init__ does not raise."""
    from unittest.mock import MagicMock

    if "integration" in request.keywords:
        yield
        return
    key = "google.cloud.bigtable"
    had = key in sys.modules
    if not had:
        sys.modules[key] = MagicMock()
    try:
        yield
    finally:
        if not had and key in sys.modules:
            del sys.modules[key]


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("bigtable-tests") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def basic_options():
    """Basic connection options for testing."""
    return {
        "project_id": "test-project",
        "instance_id": "test-instance",
        "table_id": "test-table",
    }
