# Python Data Source for Bigtable Change Streams

Python Data Source for Apache Spark enabling streaming reads from Google Cloud Bigtable Change Streams.

## Features

* **Streaming Reads**: Consume Bigtable Change Streams as a Spark Structured Streaming source
* **Partition Discovery**: Automatically discovers tablet partitions via SampleRowKeys
* **Continuation Tokens**: Tracks per-partition tokens for exactly-once processing
* **Backpressure**: Configurable `max_rows_per_partition` caps reads per micro-batch
* **Tablet Split/Merge Handling**: Detects CloseStream events and re-discovers partitions
* **Watermark Support**: Exposes `low_watermark` from heartbeats for Spark `withWatermark()`
* **Fixed Schema**: All change stream events use a consistent 8-column schema

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
#  |-- partition_key: string
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
| `partition_key` | `StringType` | Which tablet partition this came from |
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
