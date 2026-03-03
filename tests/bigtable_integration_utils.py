"""
Helpers for Bigtable integration tests: ensure instance/table and write synthetic data.

Used by test_bigtable_integration.py. Requires GCP credentials and Bigtable Admin API.
Loads .env from the project root if present, so variables can be set there.
"""

import os
import time


def _load_dotenv_if_available():
    """Load .env from project root; set GOOGLE_APPLICATION_CREDENTIALS from GCP_CREDENTIALS_JSON if present."""
    try:
        from dotenv import load_dotenv

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        load_dotenv(os.path.join(root, ".env"))
        key_rel = os.environ.get("GCP_CREDENTIALS_JSON")
        if key_rel:
            key_path = os.path.join(root, key_rel)
            if os.path.isfile(key_path):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath(key_path)
    except ImportError:
        pass


def get_bigtable_config_from_env():
    """Read Bigtable config from env (and .env if present). Returns None if any required var is missing."""
    _load_dotenv_if_available()
    project_id = os.environ.get("GCP_PROJECT_ID")
    instance_id = os.environ.get("BIGTABLE_INSTANCE_ID")
    table_id = os.environ.get("BIGTABLE_TABLE_ID")
    region = os.environ.get("BIGTABLE_REGION", "us-central1")
    if not all([project_id, instance_id, table_id]):
        return None
    return {
        "project_id": project_id,
        "instance_id": instance_id,
        "table_id": table_id,
        "region": region,
        "column_family": os.environ.get("BIGTABLE_COLUMN_FAMILY", "cf1"),
    }


def ensure_instance(client, instance_id: str, region: str) -> None:
    """Create Bigtable instance if it does not exist (DEVELOPMENT, one cluster)."""
    from google.cloud.bigtable import enums

    instance = client.instance(instance_id)
    if instance.exists():
        return

    instance.type_ = enums.Instance.Type.DEVELOPMENT
    cluster = instance.cluster(
        f"{instance_id}-cluster",
        location_id=region,
        serve_nodes=None,
    )
    operation = instance.create(clusters=[cluster])
    operation.result(timeout=600)


def ensure_table(client, instance_id: str, table_id: str, column_family: str) -> None:
    """Create Bigtable table with change stream if it does not exist."""
    from google.cloud.bigtable_admin_v2.types import bigtable_table_admin
    from google.cloud.bigtable_admin_v2.types import table as gba_table
    from google.protobuf import duration_pb2

    instance = client.instance(instance_id)
    table_obj = instance.table(table_id)

    if table_obj.exists():
        return

    table_client = client.table_admin_client
    parent = instance.name
    retention_seconds = 7 * 24 * 3600
    change_stream_config = gba_table.ChangeStreamConfig(
        retention_period=duration_pb2.Duration(seconds=retention_seconds),
    )
    table_pb = gba_table.Table(
        column_families={column_family: gba_table.ColumnFamily()},
        change_stream_config=change_stream_config,
    )
    request = bigtable_table_admin.CreateTableRequest(
        parent=parent,
        table_id=table_id,
        table=table_pb,
    )
    table_client.create_table(request=request)


def write_synthetic_mutations(
    project_id: str,
    instance_id: str,
    table_id: str,
    column_family: str,
    count: int,
    row_key: bytes = b"synth-row-1",
    column: bytes = b"payload",
) -> None:
    """Write `count` mutations to a single row to generate change stream events."""
    from google.cloud.bigtable import Client

    client = Client(project=project_id, admin=True)
    table = client.instance(instance_id).table(table_id)

    for i in range(count):
        value = f"integration-test-{i}-{time.time():.6f}".encode("utf-8")
        row = table.direct_row(row_key)
        row.set_cell(column_family, column, value)
        row.commit()
        if i < count - 1:
            time.sleep(0.2)
    client.close()
