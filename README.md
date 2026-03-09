# Python Data Source for Bigtable Change Streams

Python Data Source for Apache Spark enabling streaming reads from Google Cloud Bigtable Change Streams.

### What this solves

Many pipelines today copy or export Bigtable data into a separate cluster (e.g. Databricks via DataProc, or a GCS snapshot) so Spark can process it. That adds extra moving parts: export jobs, staging storage, and duplicated data. This data source **reads Bigtable Change Streams directly** from your Spark environment (Databricks, EMR, or any Spark 4.x cluster). You get a single, simpler architecture: Bigtable → Spark Structured Streaming → Delta (or other sinks), without standing up DataProc or copying data into an intermediate system. Change stream events are consumed as a native streaming source with exactly-once semantics and backpressure.

## Features

* **Streaming Reads**: Consume Bigtable Change Streams as a Spark Structured Streaming source
* **Partition Discovery**: Automatically discovers tablet partitions via SampleRowKeys
* **Continuation Tokens**: Tracks per-partition tokens for exactly-once processing
* **Backpressure**: Configurable `max_rows_per_partition` caps reads per micro-batch
* **Tablet Split/Merge Handling**: Detects CloseStream events and re-discovers partitions
* **Watermark Support**: Exposes `low_watermark` from heartbeats for Spark `withWatermark()`
* **Fixed Schema**: All change stream events use a consistent 8-column schema
* **Stateful processor**: Reconstruct full row state per key from the change stream via `transformWithState` (Spark 4.x)

## Installation

```bash
poetry install
```

## Quick Start

### Streaming Read

```python
from pyspark.sql import SparkSession
from bigtable_data_source import BigtableChangeStreamDataSource

spark = SparkSession.builder.appName("bigtable-changes").getOrCreate()
spark.dataSource.register(BigtableChangeStreamDataSource)

changes = spark.readStream \
    .format("bigtable_changes") \
    .option("project_id", "my-gcp-project") \
    .option("instance_id", "my-bigtable-instance") \
    .option("table_id", "my-table") \
    .load()

changes.printSchema()
# root
#  |-- row_key: binary
#  |-- column_family: string
#  |-- column_qualifier: binary
#  |-- value: binary
#  |-- mutation_type: string
#  |-- commit_timestamp: timestamp
#  |-- partition_start_key: binary
#  |-- partition_end_key: binary
#  |-- low_watermark: timestamp
```

### Filter and Write to Delta Lake

```python
from pyspark.sql.functions import col

upserts = changes.filter(col("mutation_type") == "SET_CELL")

query = upserts.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "gs://my-bucket/checkpoints/bt-stream") \
    .trigger(processingTime="15 seconds") \
    .start("gs://my-bucket/delta/bigtable-changes")

query.awaitTermination()
```

### Custom Options

```python
changes = spark.readStream \
    .format("bigtable_changes") \
    .option("project_id", "my-gcp-project") \
    .option("instance_id", "my-bigtable-instance") \
    .option("table_id", "my-table") \
    .option("app_profile_id", "streaming-profile") \
    .option("batch_duration_seconds", "15") \
    .option("max_rows_per_partition", "10000") \
    .load()
```

### Reconstruct full records with transformWithState

The **stateful processor** reconstructs the latest row state per `row_key` from the change stream: it keeps a map of **column_family → latest value** in state and emits one row `(row_key, record)` on every change. Use `BigtableReconstructProcessor` and `RECONSTRUCTED_RECORD_SCHEMA` from `bigtable_stateful_processor`. **Requires Spark 4.x** and a RocksDB state store (e.g. Databricks Runtime with `transformWithState` support).

```python
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
)

from bigtable_stateful_processor import BigtableReconstructProcessor, RECONSTRUCTED_RECORD_SCHEMA

changes = spark.readStream.format("bigtable_changes").options(**common_options).load()

reconstructed = (
    changes.groupBy("row_key")
    .transformWithState(
        statefulProcessor=BigtableReconstructProcessor(),
        outputStructType=RECONSTRUCTED_RECORD_SCHEMA,
        outputMode="Update",
        timeMode="None",
    )
)

query = reconstructed.writeStream \
    .format("memory") \
    .queryName("bt_reconstructed") \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/bt_reconstruct_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

# Query: spark.table("bt_reconstructed").show(20, truncate=60)
```

#### Loading initial state from a Delta table

You can preload state from a Delta table (e.g. a previous run’s reconstructed output or an exported snapshot) so the processor starts from existing row state instead of empty. The table must have a `record` column (map of column family → value, same as `RECONSTRUCTED_RECORD_SCHEMA`). Pass the initial state as a **GroupedData** with the same grouping key as the stream (`groupBy("row_key")`).

```python
# Load initial state from a Delta table (schema: row_key, record), then group by key
initial_state = spark.read.table("catalog.schema.bt_reconstructed").groupBy("row_key")

reconstructed = (
    changes.groupBy("row_key")
    .transformWithState(
        statefulProcessor=BigtableReconstructProcessor(),
        outputStructType=RECONSTRUCTED_RECORD_SCHEMA,
        outputMode="Update",
        timeMode="None",
        initialState=initial_state,
    )
)
```

Use this to resume from a saved snapshot, migrate from another pipeline, or bootstrap state from a batch export.

See `stream-demo.ipynb` and `examples.ipynb` for full examples.

## Configuration Options

### Connection Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `project_id` | Yes | — | GCP project ID |
| `instance_id` | Yes | — | Bigtable instance ID |
| `table_id` | Yes | — | Bigtable table ID |
| `app_profile_id` | No | `default` | Bigtable app profile ID |

### Stream Options

| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `batch_duration_seconds` | No | `10` | Target duration per micro-batch |
| `max_rows_per_partition` | No | `5000` | Max mutations to read per partition per batch |
| `start_timestamp` | No | (now) | When no checkpoint exists, start from this time: ISO 8601 (e.g. `2025-03-01T00:00:00Z`) or Unix seconds. Ignored when resuming with a checkpoint. |

## Schema

All change stream events are exposed with this fixed schema:

| Column | Type | Description |
|--------|------|-------------|
| `row_key` | `BinaryType` | The row key of the mutated row |
| `column_family` | `StringType` | Column family name |
| `column_qualifier` | `BinaryType` | Column qualifier |
| `value` | `BinaryType` | Cell value (empty for deletes) |
| `mutation_type` | `StringType` | One of: `SET_CELL`, `DELETE_COLUMN`, `DELETE_FAMILY`, `DELETE_ROW` |
| `commit_timestamp` | `TimestampType` | When the mutation was committed |
| `partition_start_key` | `BinaryType` | Start key (inclusive) of the tablet partition |
| `partition_end_key` | `BinaryType` | End key (exclusive) of the tablet partition; empty means end of table |
| `low_watermark` | `TimestampType` | Safe-to-process-up-to watermark |

## Testing with Synthetic Data

A helper script deploys a Bigtable instance and table (with change streams enabled) in your GCP sandbox and writes repeated synthetic updates to a single row, so you can test the streaming source without manual setup.

**Prerequisites:** `gcloud auth application-default login`, Bigtable Admin API enabled, and permissions to create instances and tables.

```bash
# Set your GCP sandbox project and Bigtable IDs
export GCP_PROJECT_ID=your-sandbox-project
export BIGTABLE_INSTANCE_ID=bt-sandbox
export BIGTABLE_TABLE_ID=changes
export BIGTABLE_REGION=us-central1

# Create instance + table (if needed), then write 100 updates every 2 seconds
poetry run python scripts/deploy_bigtable_and_synthetic_updates.py

# Only run synthetic updates (instance/table must already exist)
poetry run python scripts/deploy_bigtable_and_synthetic_updates.py --no-create

# Customize: 50 updates every 5 seconds
poetry run python scripts/deploy_bigtable_and_synthetic_updates.py --interval 5 --count 50
```

The script creates a **DEVELOPMENT** instance (low cost) in the given region, a table with one column family (`cf1`) and 7-day change stream retention, then writes to row `synth-row-1`, column `cf1:payload`. Use the same `project_id`, `instance_id`, and `table_id` in your `readStream.format("bigtable_changes")` options.

### Integration test

An integration test writes synthetic mutations to Bigtable and reads them via the custom data source. It requires the same env vars and will create the instance/table if missing.

```bash
export GCP_PROJECT_ID=your-sandbox-project
export BIGTABLE_INSTANCE_ID=bt-sandbox
export BIGTABLE_TABLE_ID=changes
# optional: BIGTABLE_REGION=us-central1, BIGTABLE_COLUMN_FAMILY=cf1

poetry run pytest tests/test_bigtable_integration.py -v -m integration
```

## Development

### Setup

```bash
poetry install
```

### Run Tests

```bash
# Unit tests only
poetry run pytest -v -m "not integration"

# All tests (requires Bigtable access)
poetry run pytest -v
```

### Code Quality

```bash
poetry run ruff check src/
poetry run ruff format src/
poetry run mypy src/
```

## How It Works

### Partition Discovery

On stream start, the reader calls `SampleRowKeys` to discover tablet boundaries. Each tablet
becomes a `BigtablePartition` with a start/end key range. The number of partitions scales
automatically with the number of tablets.

### Micro-Batch Processing

Each trigger interval:
1. `latestOffset()` reads a bounded chunk of changes from each partition
2. `partitions()` returns the set of partitions for this batch
3. `read(partition)` yields the buffered rows for each partition
4. `commit()` is called after successful processing

### Continuation Tokens

Each partition maintains a continuation token. On restart, Spark replays from the last
committed offset, and the reader resumes the change stream from the corresponding token.

### Tablet Splits and Merges

When Bigtable splits or merges tablets, a `CloseStream` event is received. The reader
resets the token for that partition and re-discovers the partition layout on the next batch.

## License

MIT
