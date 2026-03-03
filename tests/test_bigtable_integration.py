"""
Integration test: write synthetic data to Bigtable, read it with the PySpark custom data source.

Requires GCP credentials and a Bigtable instance/table (created by the test if missing).
Set GCP_PROJECT_ID, BIGTABLE_INSTANCE_ID, BIGTABLE_TABLE_ID (optional: BIGTABLE_REGION, BIGTABLE_COLUMN_FAMILY).

Run with:
  poetry run pytest tests/test_bigtable_integration.py -v -m integration

Skip in CI (no Bigtable):
  poetry run pytest -m "not integration"
"""

import threading
import time

import pytest

from tests.bigtable_integration_utils import (
    ensure_instance,
    ensure_table,
    get_bigtable_config_from_env,
    write_synthetic_mutations,
)


@pytest.fixture(scope="module")
def bigtable_config():
    """Bigtable config from env; skip integration tests if not set."""
    config = get_bigtable_config_from_env()
    if config is None:
        pytest.skip(
            "Bigtable integration tests require GCP_PROJECT_ID, BIGTABLE_INSTANCE_ID, BIGTABLE_TABLE_ID"
        )
    return config


@pytest.fixture(scope="module")
def bigtable_ready(bigtable_config):
    """Ensure Bigtable instance and table exist; yield config."""
    from google.cloud.bigtable import Client

    client = Client(project=bigtable_config["project_id"], admin=True)
    ensure_instance(client, bigtable_config["instance_id"], bigtable_config["region"])
    ensure_table(
        client,
        bigtable_config["instance_id"],
        bigtable_config["table_id"],
        bigtable_config["column_family"],
    )
    client.close()
    return bigtable_config


@pytest.fixture(scope="module", autouse=True)
def register_data_source(spark):
    """Register the Bigtable Change Stream data source for the module."""
    from bigtable_data_source import BigtableChangeStreamDataSource

    spark.dataSource.register(BigtableChangeStreamDataSource)


@pytest.mark.integration
def test_bigtable_synthetic_data_and_stream_read(spark, bigtable_ready):
    """
    Write synthetic mutations to Bigtable, then read change stream events
    via the custom PySpark data source and assert we see the mutations.
    """
    config = bigtable_ready
    project_id = config["project_id"]
    instance_id = config["instance_id"]
    table_id = config["table_id"]
    column_family = config["column_family"]

    # Stream options for the data source
    stream_options = {
        "project_id": project_id,
        "instance_id": instance_id,
        "table_id": table_id,
        "app_profile_id": "default",
        "max_rows_per_partition": "5000",
    }

    query_name = "bt_changes_integration"
    trigger_interval = "5 seconds"
    wait_after_writes = 25

    # Start streaming query in background (reads change stream, writes to in-memory table)
    changes = (
        spark.readStream.format("bigtable_changes")
        .options(**stream_options)
        .load()
    )

    query = (
        changes.writeStream.format("memory")
        .queryName(query_name)
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    def run_stream():
        query.awaitTermination()

    stream_thread = threading.Thread(target=run_stream, daemon=True)
    stream_thread.start()

    # Give the stream time to call initialOffset and start listening
    time.sleep(5)

    # Write synthetic data so change stream emits events
    num_mutations = 5
    write_synthetic_mutations(
        project_id=project_id,
        instance_id=instance_id,
        table_id=table_id,
        column_family=column_family,
        count=num_mutations,
        row_key=b"synth-row-1",
        column=b"payload",
    )

    # Wait for at least one micro-batch to run and pick up changes
    time.sleep(wait_after_writes)

    try:
        query.stop()
    except Exception:
        pass
    stream_thread.join(timeout=10)

    # Assert we read change stream events from our synthetic row
    result = spark.table(query_name)
    rows = result.collect()

    assert len(rows) >= 1, (
        f"Expected at least 1 change stream event, got {len(rows)}. "
        "Check Bigtable change stream is enabled and retention is set."
    )

    # Schema: row_key, column_family, column_qualifier, value, mutation_type, commit_timestamp, partition_key, low_watermark
    row_keys = [r.row_key for r in rows if r.row_key == b"synth-row-1"]
    set_cells = [r for r in rows if r.mutation_type == "SET_CELL"]

    assert len(row_keys) >= 1, (
        f"Expected at least 1 event for row_key=b'synth-row-1', got {len(row_keys)}. "
        f"Sample row_keys: {[r.row_key for r in rows[:5]]!r}"
    )
    assert len(set_cells) >= 1, (
        f"Expected at least 1 SET_CELL mutation, got {len(set_cells)}. "
        f"Sample mutation_types: {[r.mutation_type for r in rows[:5]]}"
    )

    # Our synthetic values contain this prefix
    values = [r.value for r in rows if r.value and b"integration-test-" in r.value]
    assert len(values) >= 1, (
        f"Expected at least 1 value with 'integration-test-' prefix, got {len(values)}. "
        f"Sample values: {[r.value[:50] if r.value else None for r in rows[:3]]!r}"
    )
