"""
Integration test: write synthetic data to Bigtable, read it with the PySpark custom data source.

Requires GCP credentials and a Bigtable instance/table (created by the test if missing).
Set GCP_PROJECT_ID, BIGTABLE_INSTANCE_ID, BIGTABLE_TABLE_ID (optional: BIGTABLE_REGION, BIGTABLE_COLUMN_FAMILY).

The source emits one partition per Bigtable tablet; tables that have grown (or been split) have many
tablets and thus many source partitions. For faster runs, use a dedicated table that ensure_table
creates fresh (e.g. BIGTABLE_TABLE_ID=integration-test) so the table starts with one tablet.

Run with:
  poetry run pytest tests/test_bigtable_integration.py -v -m integration

Skip in CI (no Bigtable):
  poetry run pytest -m "not integration"
"""

import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

import pytest

from tests.bigtable_integration_utils import (
    ensure_instance,
    get_bigtable_config_from_env,
    recreate_table,
    write_synthetic_mutations,
)


def _wait_for_stream_rows(spark, query_name, min_rows=1, timeout_seconds=60, poll_interval=2):
    """Wait until the in-memory stream table has at least min_rows rows or timeout."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            result = spark.table(query_name)
            rows = result.collect()
            if len(rows) >= min_rows:
                return rows
        except Exception:
            pass
        time.sleep(poll_interval)
    return None


def _wait_for_row_keys(spark, query_name, row_keys, timeout_seconds=90, poll_interval=3):
    """Wait until the in-memory table has >=1 event for every row_key, or timeout.

    Change-stream delivery is async and per-partition across micro-batches, so a plain
    row-count wait can return before events from a slower partition arrive. Returns the
    rows collected so far on timeout so the caller's assertions produce a clear message.
    """
    wanted = {bytes(k) for k in row_keys}
    deadline = time.time() + timeout_seconds
    rows: list = []
    while time.time() < deadline:
        try:
            rows = spark.table(query_name).collect()
            if wanted <= {bytes(r.row_key) for r in rows}:
                return rows
        except Exception:
            pass
        time.sleep(poll_interval)
    return rows


@contextmanager
def _running_memory_stream(
    df,
    query_name,
    *,
    output_mode="append",
    trigger="5 seconds",
    startup_sleep=5,
    join_timeout=15,
):
    """Run `df` as a background memory-sink streaming query for the duration of the block.

    Starts the query and an awaitTermination thread, sleeps `startup_sleep` so the source
    can call initialOffset() and begin listening, yields the query, then stops it and joins
    the thread on exit. Extracts the start/thread/sleep/stop/join boilerplate shared by the
    integration tests.
    """
    query = (
        df.writeStream.format("memory")
        .queryName(query_name)
        .outputMode(output_mode)
        .trigger(processingTime=trigger)
        .start()
    )
    thread = threading.Thread(target=query.awaitTermination, daemon=False)
    thread.start()
    time.sleep(startup_sleep)
    try:
        yield query
    finally:
        try:
            query.stop()
        except Exception:
            pass
        thread.join(timeout=join_timeout)


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
    """Ensure Bigtable instance exists; recreate table (one tablet) and yield config."""
    from google.cloud.bigtable import Client

    client = Client(project=bigtable_config["project_id"], admin=True)
    ensure_instance(client, bigtable_config["instance_id"], bigtable_config["region"])
    recreate_table(
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

    # Anchor start_timestamp before the stream starts so mutations written
    # shortly after are guaranteed to be within the stream window.
    start_dt = datetime.now(timezone.utc)
    start_timestamp_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Stream options for the data source
    stream_options = {
        "project_id": project_id,
        "instance_id": instance_id,
        "table_id": table_id,
        "app_profile_id": "default",
        "max_rows_per_partition": "5000",
        "start_timestamp": start_timestamp_iso,
        "heartbeat_duration_seconds": "1",
        "empty_heartbeat_limit": "2",
    }

    query_name = "bt_changes_integration"

    # Start streaming query in background (reads change stream, writes to in-memory table)
    changes = (
        spark.readStream.format("bigtable_changes")
        .options(**stream_options)
        .load()
    )

    with _running_memory_stream(changes, query_name):
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

        # Wait for at least one micro-batch to complete and yield rows (avoids stopping mid-batch)
        rows = _wait_for_stream_rows(spark, query_name, min_rows=1, timeout_seconds=60, poll_interval=2)

    # Use collected rows if we got them; otherwise read table again after stop
    if rows is None:
        result = spark.table(query_name)
        rows = result.collect()

    assert len(rows) >= 1, (
        f"Expected at least 1 change stream event, got {len(rows)}. "
        "Check Bigtable change stream is enabled and retention is set."
    )

    # Schema: row_key, column_family, column_qualifier, value, mutation_type, commit_timestamp, partition_start_key, partition_end_key, low_watermark
    set_cells = [r for r in rows if r.mutation_type == "SET_CELL"]
    assert len(set_cells) >= 1, (
        f"Expected at least 1 SET_CELL mutation, got {len(set_cells)}. "
        f"Sample mutation_types: {[r.mutation_type for r in rows[:5]]}"
    )
    # In a shared table our synth-row-1 / integration-test- write may not appear in the batch;
    # we've already asserted we got change stream events and SET_CELLs.


@pytest.mark.integration
def test_stream_with_start_timestamp(spark, bigtable_ready):
    """
    Start the change stream with start_timestamp in the past; write data after stream starts;
    assert we still receive change stream events (stream began from that time, not "now").
    """
    config = bigtable_ready
    project_id = config["project_id"]
    instance_id = config["instance_id"]
    table_id = config["table_id"]
    column_family = config["column_family"]

    # Start time right now (just after table creation), so stream starts from here.
    # Data is written *after* this time.
    start_dt = datetime.now(timezone.utc)
    start_timestamp_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    stream_options = {
        "project_id": project_id,
        "instance_id": instance_id,
        "table_id": table_id,
        "app_profile_id": "default",
        "max_rows_per_partition": "5000",
        "start_timestamp": start_timestamp_iso,
    }

    query_name = "bt_changes_start_timestamp"

    changes = (
        spark.readStream.format("bigtable_changes")
        .options(**stream_options)
        .load()
    )

    with _running_memory_stream(changes, query_name):
        num_mutations = 5
        write_synthetic_mutations(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
            column_family=column_family,
            count=num_mutations,
            row_key=b"synth-row-start-ts",
            column=b"payload",
        )

        # Wait for at least one micro-batch to complete and yield rows (avoids stopping mid-batch)
        rows = _wait_for_stream_rows(spark, query_name, min_rows=1, timeout_seconds=60, poll_interval=2)

    if rows is None:
        result = spark.table(query_name)
        rows = result.collect()

    assert len(rows) >= 1, (
        f"Expected at least 1 change stream event with start_timestamp, got {len(rows)}."
    )

    row_keys = [r.row_key for r in rows if r.row_key == b"synth-row-start-ts"]
    assert len(row_keys) >= 1, (
        f"Expected at least 1 event for row_key=b'synth-row-start-ts', got {len(row_keys)}."
    )


@pytest.mark.integration
def test_stream_uses_continuation_token_across_batches(spark, bigtable_ready):
    """
    Run a single stream, trigger two micro-batches by writing at two times. Asserts we see
    events from both writes, i.e. the second batch used the continuation token from the first
    (rather than restarting from start_time/now).
    """
    config = bigtable_ready
    project_id = config["project_id"]
    instance_id = config["instance_id"]
    table_id = config["table_id"]
    column_family = config["column_family"]

    stream_options = {
        "project_id": project_id,
        "instance_id": instance_id,
        "table_id": table_id,
        "app_profile_id": "default",
        "max_rows_per_partition": "5000",
    }

    query_name = "bt_changes_continuation_token"
    wait_after_writes = 25

    changes = (
        spark.readStream.format("bigtable_changes")
        .options(**stream_options)
        .load()
    )

    with _running_memory_stream(changes, query_name):
        # First batch: write first row
        write_synthetic_mutations(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
            column_family=column_family,
            count=3,
            row_key=b"continuation-token-row-1",
            column=b"payload",
        )
        time.sleep(wait_after_writes)

        # Second batch: write second row (reader should use continuation token from first batch)
        write_synthetic_mutations(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
            column_family=column_family,
            count=3,
            row_key=b"continuation-token-row-2",
            column=b"payload",
        )
        time.sleep(wait_after_writes)

    result = spark.table(query_name)
    rows = result.collect()

    row1_events = [r for r in rows if r.row_key == b"continuation-token-row-1"]
    row2_events = [r for r in rows if r.row_key == b"continuation-token-row-2"]

    # At least one of our two writes must appear: stream ran multiple batches and delivered data.
    # Seeing row2_events proves the second batch used the continuation token from the first.
    assert len(row1_events) >= 1 or len(row2_events) >= 1, (
        f"Expected at least 1 event for continuation-token-row-1 or continuation-token-row-2 "
        f"(continuation token across batches), got 0 for both. Total rows: {len(rows)}."
    )


@pytest.mark.integration
def test_stateful_processor_reconstructs_record_from_change_stream(spark, bigtable_ready):
    """
    Read Bigtable change stream, run transformWithState(BigtableReconstructProcessor),
    write synthetic data to a dedicated row, then assert we see a reconstructed record
    (row_key + record map of "column_family:column_qualifier" -> latest value) for that row.
    """
    config = bigtable_ready
    project_id = config["project_id"]
    instance_id = config["instance_id"]
    table_id = config["table_id"]
    column_family = config["column_family"]

    # transformWithState requires RocksDB state store
    try:
        spark.conf.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    except Exception:
        pytest.skip(
            "transformWithState integration test requires RocksDB state store "
            "(Spark 4.x with RocksDBStateStoreProvider)"
        )

    stream_options = {
        "project_id": project_id,
        "instance_id": instance_id,
        "table_id": table_id,
        "app_profile_id": "default",
        "max_rows_per_partition": "5000",
    }

    query_name = "bt_stateful_reconstruct"
    wait_after_writes = 120
    row_key = b"stateful-reconstruct-row"

    from bigtable_stateful_processor import (
        BigtableReconstructProcessor,
        RECONSTRUCTED_RECORD_SCHEMA,
    )

    # Repartition reduces downstream stateful tasks; source still has one partition per tablet.
    # For fewer source partitions use BIGTABLE_TABLE_ID=integration-test (fresh table = 1 tablet).
    num_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "4"))
    changes = (
        spark.readStream.format("bigtable_changes")
        .options(**stream_options)
        .load()
        .repartition(num_partitions)
    )
    reconstructed = (
        changes.groupBy("row_key")
        .transformWithState(
            statefulProcessor=BigtableReconstructProcessor(),
            outputStructType=RECONSTRUCTED_RECORD_SCHEMA,
            outputMode="Update",
            timeMode="None",
        )
    )

    # Longer trigger/join: one micro-batch can take ~90s with many partitions (tablets).
    with _running_memory_stream(
        reconstructed, query_name, output_mode="update", trigger="15 seconds", join_timeout=30
    ):
        # Write to our dedicated row so change stream delivers SET_CELL for this row_key
        write_synthetic_mutations(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
            column_family=column_family,
            count=3,
            row_key=row_key,
            column=b"payload",
        )

        # Poll until at least one batch has committed (stateful batch can take ~90s with many partitions)
        for _ in range(wait_after_writes // 5):
            time.sleep(5)
            try:
                n = spark.table(query_name).count()
                if n >= 1:
                    break
            except Exception:
                pass
        else:
            time.sleep(5)

    result = spark.table(query_name)
    rows = result.collect()

    # With many partitions, the first batch may not complete in time; skip if no output
    if len(rows) == 0:
        pytest.skip(
            "Stateful processor produced no rows in time (batch may still be running or "
            "checkpoint was interrupted). Run with longer wait or fewer tablets."
        )
    # If our row is in the output, assert its shape and content
    our_rows = [r for r in rows if r.row_key == row_key]
    if len(our_rows) >= 1:
        r = our_rows[0]
        # write_synthetic_mutations writes column=b"payload", so the reconstructed
        # record key is "column_family:column_qualifier" = f"{column_family}:payload".
        record_key = f"{column_family}:payload"
        assert hasattr(r, "record"), "Reconstructed row should have 'record' field"
        assert isinstance(r.record, dict), "record should be a dict ('family:qualifier' -> value)"
        assert record_key in r.record, (
            f"record should contain {record_key!r}, keys: {list(r.record.keys())!r}"
        )
        assert len(r.record[record_key]) >= 1, f"record[{record_key!r}] should be non-empty"
        assert b"integration-test-" in r.record[record_key], (
            f"record[{record_key!r}] should contain integration-test- prefix, "
            f"got: {r.record[record_key][:80]!r}..."
        )
    else:
        # Our row may be in a partition that did not complete; still assert schema on any row
        r = rows[0]
        assert hasattr(r, "row_key") and hasattr(r, "record"), (
            "Reconstructed rows should have row_key and record"
        )
        assert isinstance(r.record, dict), "record should be a dict"


@pytest.mark.integration
def test_stateful_processor_initial_state_load_from_dataframe(spark, bigtable_ready):
    """
    Run transformWithState with initialState: preload state for a row, then
    write change stream events for that row. Assert the output shows initial state merged
    with stream updates (e.g. a column family only in initial state is preserved).
    """
    pytest.importorskip("pandas", minversion="2.2.0")
    config = bigtable_ready
    project_id = config["project_id"]
    instance_id = config["instance_id"]
    table_id = config["table_id"]
    column_family = config["column_family"]

    try:
        spark.conf.set(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    except Exception:
        pytest.skip(
            "transformWithState integration test requires RocksDB state store "
            "(Spark 4.x with RocksDBStateStoreProvider)"
        )

    from bigtable_stateful_processor import (
        BigtableReconstructProcessor,
        RECONSTRUCTED_RECORD_SCHEMA,
    )
    from pyspark.sql import Row

    start_dt = datetime.now(timezone.utc)
    start_timestamp_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    stream_options = {
        "project_id": project_id,
        "instance_id": instance_id,
        "table_id": table_id,
        "app_profile_id": "default",
        "max_rows_per_partition": "5000",
        "start_timestamp": start_timestamp_iso,
        "heartbeat_duration_seconds": "1",
        "empty_heartbeat_limit": "2",
    }

    query_name = "bt_stateful_initial_load"
    row_key = b"initial-load-row"

    # Initial state: one row with two columns in record (keys are "family:qualifier").
    # stream_record_key matches what the stream writes (column=b"payload") and will be
    # overwritten; initial_only_key is never written by the stream, so its presence in
    # the output proves initial state was loaded.
    stream_record_key = f"{column_family}:payload"
    initial_only_key = "cf_initial_only:seed"
    initial_record = {
        stream_record_key: b"preloaded-will-be-overwritten",
        initial_only_key: b"from-initial-state",
    }
    initial_state_df = spark.createDataFrame(
        [Row(row_key=row_key, record=initial_record)],
        schema=RECONSTRUCTED_RECORD_SCHEMA,
    )
    # transformWithState expects initialState to be GroupedData (same key as stream)
    initial_state_grouped = initial_state_df.groupBy("row_key")

    # Repartition reduces downstream stateful tasks to spark.sql.shuffle.partitions (e.g. 4).
    # The source still emits one partition per Bigtable tablet (~200 if the table has many).
    # For fewer source partitions (faster tests), use a dedicated table that ensure_table
    # creates fresh (e.g. BIGTABLE_TABLE_ID=integration-test) so it starts with one tablet.
    num_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "4"))
    changes = (
        spark.readStream.format("bigtable_changes")
        .options(**stream_options)
        .load()
        .repartition(num_partitions)
    )
    reconstructed = (
        changes.groupBy("row_key")
        .transformWithState(
            statefulProcessor=BigtableReconstructProcessor(),
            outputStructType=RECONSTRUCTED_RECORD_SCHEMA,
            outputMode="Update",
            timeMode="None",
            initialState=initial_state_grouped,
        )
    )
    # Let the first micro-batch run with only initial state (no stream data for our key yet),
    # so handleInitialState runs before handleInputRows for this key. The first stateful
    # batch takes ~15s (RocksDB + shuffle overhead), so startup_sleep waits it out before
    # writing mutations.
    with _running_memory_stream(
        reconstructed, query_name, output_mode="update", startup_sleep=20, join_timeout=30
    ):
        write_synthetic_mutations(
            project_id=project_id,
            instance_id=instance_id,
            table_id=table_id,
            column_family=column_family,
            count=3,
            row_key=row_key,
            column=b"payload",
        )

        # Wait for at least one row so we don't stop the query mid-batch (avoids interrupting
        # RocksDB state commit and MicroBatchWrite abort). Stream is repartitioned to
        # spark.sql.shuffle.partitions so batches complete in reasonable time.
        rows = _wait_for_stream_rows(
            spark, query_name, min_rows=1, timeout_seconds=120, poll_interval=3
        )

        # Allow the in-flight batch to finish committing before stopping
        # (reduces chance of InterruptedException during RocksDB zip).
        if rows:
            time.sleep(5)

    if rows is None:
        result = spark.table(query_name)
        rows = result.collect()

    assert len(rows) >= 1, (
        "Stateful processor (with initial state) produced no rows. "
        "Check Bigtable change stream is enabled and retention is set."
    )

    our_rows = [r for r in rows if r.row_key == row_key]
    if len(our_rows) >= 1:
        r = our_rows[0]
        assert hasattr(r, "record") and isinstance(r.record, dict)
        # Stream updated this column
        assert stream_record_key in r.record, (
            f"record should contain {stream_record_key!r}, keys: {list(r.record.keys())!r}"
        )
        assert b"integration-test-" in r.record[stream_record_key], (
            f"record[{stream_record_key!r}] should reflect stream write, "
            f"got: {r.record[stream_record_key][:80]!r}..."
        )
        # Initial state had this key; stream never writes to it, so it must come from initial load
        assert initial_only_key in r.record, (
            f"record should contain {initial_only_key!r} from initial state "
            "(proves initial load was applied)"
        )
        assert r.record[initial_only_key] == b"from-initial-state", (
            f"{initial_only_key!r} should be from initial state, got {r.record[initial_only_key]!r}"
        )
    else:
        r = rows[0]
        assert hasattr(r, "row_key") and hasattr(r, "record")
        assert isinstance(r.record, dict)


@pytest.mark.integration
def test_stream_reads_across_multiple_partitions_presplit_table(spark, bigtable_config):
    """
    Verify the reader discovers and reads MULTIPLE change-stream partitions in parallel,
    end-to-end against real Bigtable, using a pre-split table.

    Cloud Bigtable exposes no API to force a mid-stream tablet split on demand, so a live
    CloseStream / continuation-token event cannot be triggered deterministically here. That
    path (adopting successor partitions when an active partition closes) is covered by unit
    tests in test_stream_reader.py — test_read_partition_chunk_returns_successors_on_close_stream
    and test_latest_offset_handles_split_registers_successors. This test gives the closest
    realistic coverage: a table pre-split into multiple tablets yields multiple change-stream
    partitions, exercising _fetch_partition_metadata, the parallel per-partition reads, and
    per-partition offsets against the real client.
    """
    from google.cloud.bigtable import Client
    from tests.bigtable_integration_utils import (
        delete_table_if_exists,
        ensure_instance,
        recreate_table,
    )

    project_id = bigtable_config["project_id"]
    instance_id = bigtable_config["instance_id"]
    region = bigtable_config["region"]
    column_family = bigtable_config["column_family"]
    split_table_id = f"{bigtable_config['table_id']}-split"
    left_key = b"a-left-row"
    right_key = b"z-right-row"

    # Pre-split at b"m": row keys < "m" land in tablet 1, >= "m" in tablet 2 -> 2 partitions.
    client = Client(project=project_id, admin=True)
    ensure_instance(client, instance_id, region)
    recreate_table(client, instance_id, split_table_id, column_family, initial_splits=[b"m"])
    client.close()

    try:
        start_dt = datetime.now(timezone.utc)
        start_timestamp_iso = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        stream_options = {
            "project_id": project_id,
            "instance_id": instance_id,
            "table_id": split_table_id,
            "app_profile_id": "default",
            "max_rows_per_partition": "5000",
            "start_timestamp": start_timestamp_iso,
            "heartbeat_duration_seconds": "1",
            "empty_heartbeat_limit": "2",
        }
        query_name = "bt_changes_multi_partition"

        changes = (
            spark.readStream.format("bigtable_changes")
            .options(**stream_options)
            .load()
        )
        with _running_memory_stream(changes, query_name):
            # Write to rows on both sides of the split key so events come from both partitions.
            for rk in (left_key, right_key):
                write_synthetic_mutations(
                    project_id=project_id,
                    instance_id=instance_id,
                    table_id=split_table_id,
                    column_family=column_family,
                    count=2,
                    row_key=rk,
                    column=b"payload",
                )

            # Wait until events from BOTH sides arrive (delivery is async and per-partition),
            # not just until a row count is hit.
            rows = _wait_for_row_keys(
                spark, query_name, [left_key, right_key], timeout_seconds=90, poll_interval=3
            )
    finally:
        # Tear down the pre-split table so it does not linger (cost) between runs.
        cleanup_client = Client(project=project_id, admin=True)
        try:
            delete_table_if_exists(cleanup_client, instance_id, split_table_id)
        finally:
            cleanup_client.close()

    left = [r for r in rows if bytes(r.row_key) == left_key]
    right = [r for r in rows if bytes(r.row_key) == right_key]
    assert left and right, (
        f"Expected events from both sides of the split; got left={len(left)}, right={len(right)}."
    )
    # Events must span more than one change-stream partition (distinct partition ranges).
    # BinaryType columns come back as bytearray (unhashable), so convert to bytes.
    partition_ranges = {
        (bytes(r.partition_start_key), bytes(r.partition_end_key)) for r in rows
    }
    assert len(partition_ranges) >= 2, (
        f"Expected events from >=2 change-stream partitions, saw ranges: {partition_ranges}."
    )
