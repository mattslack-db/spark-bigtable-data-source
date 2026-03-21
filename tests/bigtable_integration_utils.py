"""
Helpers for Bigtable integration tests: ensure instance/table and write synthetic data.

Used by test_bigtable_integration.py. Requires GCP credentials and Bigtable Admin API.
Loads .env from the project root if present, so variables can be set there.
"""

import os
import time
from pathlib import Path


def _safe_credentials_path(project_root: Path, key_rel: str) -> Path | None:
    """
    Resolve a credentials path only if it exists and stays under project_root
    (blocks path traversal via .. or absolute paths outside the repo).
    """
    root = project_root.resolve()
    p = Path(key_rel)
    resolved = p.resolve() if p.is_absolute() else (root / key_rel).resolve()
    if not resolved.is_relative_to(root):
        return None
    if not resolved.is_file():
        return None
    return resolved


def _load_dotenv_if_available():
    """Load .env from project root; set GOOGLE_APPLICATION_CREDENTIALS from GCP_CREDENTIALS_JSON if present."""
    try:
        from dotenv import load_dotenv

        root = Path(__file__).resolve().parent.parent
        load_dotenv(root / ".env")
        key_rel = os.environ.get("GCP_CREDENTIALS_JSON")
        if key_rel:
            key_path = _safe_credentials_path(root, key_rel)
            if key_path is not None:
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(key_path)
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
    """Create Bigtable table with change stream if it does not exist.

    No initial splits are set, so a newly created table starts with one tablet
    (one change-stream partition). Tables that already exist may have many tablets
    from prior writes (Bigtable auto-splits as data grows), which increases
    partition count and micro-batch time. For faster integration tests, use a
    dedicated table id (e.g. integration-test) that ensure_table creates fresh.
    """
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


def recreate_table(client, instance_id: str, table_id: str, column_family: str) -> None:
    """Delete the table if it exists, then create it with change stream (one tablet, no splits).

    Bigtable does not allow deleting a table while its change stream is enabled,
    so we disable the change stream first via UpdateTable, then delete, then create.
    """
    from google.cloud.bigtable_admin_v2.types import bigtable_table_admin
    from google.cloud.bigtable_admin_v2.types import table as gba_table
    from google.protobuf import field_mask_pb2

    instance = client.instance(instance_id)
    table_obj = instance.table(table_id)
    if table_obj.exists():
        table_client = client.table_admin_client
        # Disable change stream so the table can be deleted.
        update_request = bigtable_table_admin.UpdateTableRequest(
            table=gba_table.Table(name=table_obj.name),
            update_mask=field_mask_pb2.FieldMask(paths=["change_stream_config"]),
        )
        operation = table_client.update_table(request=update_request)
        operation.result(timeout=120)
        table_obj.delete()
    ensure_table(client, instance_id, table_id, column_family)


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
