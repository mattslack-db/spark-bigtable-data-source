# AGENTS.md

This file provides guidance to LLM when working with code in this repository.

You're an experienced Spark developer. You're developing a custom Python streaming data source to
read Google Cloud Bigtable Change Streams using the PySpark DataSource API (Spark 3.4+).
You follow the architecture and development guidelines outlined below.

## Project Overview

This repository contains source code of a Python data source (part of Apache Spark API) for
reading Google Cloud Bigtable Change Streams in a streaming manner.

**Architecture**: Simple, flat structure with one data source per file. Each data source implements:
- `DataSource` base class with `name()`, `schema()`, and `streamReader()` methods
- Separate stream reader class (`DataSourceStreamReader`) for streaming reads
- Shared reader logic in a common base class (`BigtableStreamReader`)

### Documentation and examples of custom data sources using the same API

There is a number of publicly available examples that demonstrate how to implement custom Python data sources:

- https://github.com/alexott/cyber-spark-data-connectors
- https://github.com/databricks/tmm/tree/main/Lakeflow-OpenSkyNetwork
- https://github.com/allisonwang-db/pyspark-data-sources
- https://github.com/databricks-industry-solutions/python-data-sources
- https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html
- https://docs.databricks.com/aws/en/pyspark/datasources

Documentation about Google Cloud Bigtable is available at:

- [Cloud Bigtable Documentation](https://cloud.google.com/bigtable/docs)
- [Change Streams Overview](https://cloud.google.com/bigtable/docs/change-streams)
- [Python Client Library](https://cloud.google.com/python/docs/reference/bigtable/latest)

## Architecture Patterns

### Data Source Implementation Pattern

1. **DataSource class** (`data_source.py`): Entry point
   - Implements `name()` class method (returns `"bigtable_changes"`)
   - Implements `schema()` returning the fixed `CHANGE_STREAM_SCHEMA`
   - Implements `streamReader()` for streaming read operations

2. **Base Stream Reader class** (`stream_reader.py`): Core streaming logic
   - Extracts and validates options in `__init__`
   - Implements `initialOffset()` to discover tablet partitions
   - Implements `latestOffset()` to read change stream chunks per partition
   - Implements `partitions(start, end)` to return partition objects
   - Implements `read(partition)` to yield buffered rows
   - Implements `commit(end)` and `stop()`

3. **Stream Reader class**: Inherits from base reader + `DataSourceStreamReader`
   - No additional methods needed

4. **Schema** (`schema.py`): Fixed schema for change stream events
   - `CHANGE_STREAM_SCHEMA` with row_key, column_family, column_qualifier, value,
     mutation_type, commit_timestamp, partition_key, low_watermark

5. **Partitioning** (`partitioning.py`): Bigtable tablet partition representation
   - `BigtablePartition(InputPartition)` with partition_index, start_key, end_key, token

### Key Design Principles

1. **SIMPLE over CLEVER**: No abstract base classes, factory patterns, or complex inheritance
2. **EXPLICIT over IMPLICIT**: Direct implementations, no hidden abstractions
3. **FLAT over NESTED**: Single-level inheritance (DataSource → Reader → Stream)
4. **Imports inside methods**: For partition-level execution, import libraries within methods
5. **Lazy client creation**: Bigtable client is created on first use, not in __init__

### Data Source Registration

Users register data sources like this:
```python
from bigtable_data_source import BigtableChangeStreamDataSource
spark.dataSource.register(BigtableChangeStreamDataSource)

# Then use with .format("bigtable_changes")
df = spark.readStream.format("bigtable_changes") \
    .option("project_id", "my-project") \
    .option("instance_id", "my-instance") \
    .option("table_id", "my-table") \
    .load()
```

## Development Commands

### Python Execution Rules

**CRITICAL: Always use `poetry run` instead of direct `python`:**
```bash
# CORRECT
poetry run python script.py

# WRONG
python script.py
```

## Development Workflow

### Package Management

- **Python**: Use `poetry add/remove` for dependencies, never edit `pyproject.toml` manually
- Always check if dependencies already exist before adding new ones

### Setup
```bash
poetry install
```

### Testing
```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_stream_reader.py

# Run single test
poetry run pytest tests/test_stream_reader.py::test_stream_reader_init_with_valid_options

# Run with verbose output
poetry run pytest -v
```

### Code Quality
```bash
poetry run ruff check src/
poetry run ruff format src/
poetry run mypy src/
```

## Testing Guidelines

- Tests use `pytest` with `pytest-spark` for Spark session fixtures
- Mock Bigtable client calls using `unittest.mock.patch`
- Test reader initialization, option validation, partition discovery, and data processing
- Use fixtures for common setup (`basic_options`)

## Important Notes

- **Python version**: 3.10+ (defined in `pyproject.toml`)
- **Spark version**: 4.0.1+ required (PySpark DataSource API)
- **Dependencies**: Keep minimal - only add if critically needed
- **Never use direct `python` commands**: Always use `poetry run python`
