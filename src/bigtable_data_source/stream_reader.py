"""Bigtable Change Stream reader implementation."""

from datetime import datetime, timezone
from typing import Iterator, List, Optional

from pyspark.sql.datasource import DataSourceStreamReader

from .partitioning import BigtablePartition


def _to_datetime_utc(ts):
    """Convert protobuf Timestamp or datetime-like to timezone-aware datetime."""
    if ts is None:
        return None
    if hasattr(ts, "ToDatetime"):
        return ts.ToDatetime(tzinfo=timezone.utc)
    if hasattr(ts, "timestamp"):
        from datetime import datetime
        return datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc)
    # Already datetime-like (e.g. DatetimeWithNanoseconds from proto-plus)
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


class BigtableStreamReader:
    """
    Base reader for Bigtable Change Streams.

    Implements micro-batch streaming by:
      1. initialOffset()  — discover all tablet partitions, tokens = None
      2. latestOffset()   — read a bounded chunk per partition, save new tokens
      3. partitions()     — return BigtablePartition objects for the batch
      4. read(partition)  — yield rows from that partition's buffered data

    Optional options:
      credentials_json: JSON string of a GCP service account key dict. If set,
        credentials are created via google.oauth2.service_account.Credentials
        .from_service_account_info(); otherwise application default credentials
        are used (e.g. GOOGLE_APPLICATION_CREDENTIALS or ADC).
      start_timestamp: When no continuation token is set, start the change stream
        from this time instead of "now". ISO 8601 string (e.g. "2025-03-01T00:00:00Z")
        or Unix timestamp (seconds). Ignored when resuming with a token.
    """

    def __init__(self, options):
        self._validate_options(options)
        # Fail fast with a clear error if the Bigtable library is missing. Otherwise the
        # import happens lazily in _get_client() when initialOffset()/latestOffset() run
        # in a context where exceptions can be swallowed (e.g. trigger(availableNow=True)).
        try:
            import google.cloud.bigtable  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "The Bigtable change stream source requires google-cloud-bigtable. "
                "Install it with: pip install google-cloud-bigtable"
            ) from e

        self.project_id = options["project_id"]
        self.instance_id = options["instance_id"]
        self.table_id = options["table_id"]
        self.app_profile = options.get("app_profile_id", "default")
        self.batch_seconds = int(options.get("batch_duration_seconds", "10"))
        self.max_rows_per_partition = int(options.get("max_rows_per_partition", "5000"))
        # Optional: JSON string of service account key dict; if set, use it instead of ADC
        self._credentials_json = options.get("credentials_json")
        # Optional: when no continuation token, start from this time (ISO 8601 str or Unix seconds)
        self._start_timestamp: Optional[datetime] = self._parse_start_timestamp(
            options.get("start_timestamp")
        )
        self.options = options

        # partition_index → list of row dicts
        self._buffered_rows: dict[int, list] = {}
        # partition_index → continuation token string
        self._tokens: dict[int, Optional[str]] = {}
        # partition_index → BigtablePartition
        self._partitions: dict[int, BigtablePartition] = {}
        # partition_index → raw StreamPartition from API (for exact request match)
        self._raw_partitions: dict[int, object] = {}

        self._client = None
        self._table = None

    def _validate_options(self, options):
        required = ["project_id", "instance_id", "table_id"]
        missing = [opt for opt in required if opt not in options]
        if missing:
            raise ValueError(f"Missing required options: {', '.join(missing)}")

    @staticmethod
    def _parse_start_timestamp(value) -> Optional[datetime]:
        """Parse start_timestamp option to UTC datetime, or None if not set."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        s = str(value).strip()
        if not s:
            return None
        # ISO 8601: allow Z or +00:00 for UTC
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _get_client(self):
        """Lazily create the Bigtable client and table reference."""
        if self._client is None:
            import json
            try:
                from google.cloud import bigtable
            except ImportError as e:
                raise ImportError(
                    "The Bigtable change stream source requires google-cloud-bigtable. "
                    "Install it with: pip install google-cloud-bigtable"
                ) from e

            credentials = None
            if self._credentials_json:
                try:
                    from google.oauth2 import service_account
                    sa_info = json.loads(self._credentials_json)
                    credentials = service_account.Credentials.from_service_account_info(
                        sa_info,
                        scopes=["https://www.googleapis.com/auth/bigtable.data", "https://www.googleapis.com/auth/bigtable.admin"],
                    )
                except Exception:
                    pass
            if credentials is not None:
                self._client = bigtable.Client(
                    project=self.project_id, admin=True, credentials=credentials
                )
            else:
                # Fall back to application default credentials (e.g. GOOGLE_APPLICATION_CREDENTIALS or ADC)
                self._client = bigtable.Client(project=self.project_id, admin=True)
            self._table = self._client.instance(self.instance_id).table(self.table_id)
        return self._client, self._table

    def initialOffset(self) -> dict:
        """
        Called once on stream start. Discover tablet partitions via
        SampleRowKeys and return initial offset with no tokens.
        """
        partitions = self._fetch_partition_metadata()
        return {str(p.partition_index): None for p in partitions}

    def latestOffset(self) -> dict:
        """
        Called each micro-batch trigger. Reads up to max_rows_per_partition
        changes from each partition and buffers them. Returns new token offsets.
        """
        self._buffered_rows = {}
        new_offsets = {}

        for idx, partition in self._partitions.items():
            rows, new_token = self._read_partition_chunk(partition)
            self._buffered_rows[idx] = rows
            new_offsets[str(idx)] = new_token
            self._tokens[idx] = new_token

        return new_offsets

    def partitions(self, start: dict, end: dict) -> List[BigtablePartition]:
        """
        Returns the list of partitions to process between start and end offsets.
        Each partition carries the start token and the buffered rows so read() can
        yield them on the executor.
        """
        result = []
        for idx, partition in self._partitions.items():
            start_token = start.get(str(idx))
            result.append(
                BigtablePartition(
                    partition_index=idx,
                    start_key=partition.start_key,
                    end_key=partition.end_key,
                    token=start_token,
                    rows=self._buffered_rows.get(idx, []),
                )
            )
        return result

    def read(self, partition: BigtablePartition) -> Iterator[tuple]:
        """
        Called on Spark executors. Yields rows from this partition.
        Rows are carried on the partition object so they are available on the executor.
        """
        rows = partition.rows if hasattr(partition, "rows") else self._buffered_rows.get(partition.partition_index, [])
        for row in rows:
            yield (
                row["row_key"],
                row["column_family"],
                row["column_qualifier"],
                row["value"],
                row["mutation_type"],
                row["commit_timestamp"],
                row["partition_key"],
                row["low_watermark"],
            )

    def commit(self, end: dict) -> None:
        """Called after a batch completes successfully."""
        pass

    def stop(self) -> None:
        if self._client is not None:
            self._client.close()

    # ── Internal helpers ──────────────────────────────────────────────────

    def _fetch_partition_metadata(self) -> List[BigtablePartition]:
        """
        Discover change stream partitions via GenerateInitialChangeStreamPartitions.
        Uses the same partition layout the change stream API expects.
        """
        _, table = self._get_client()
        data_client = table._instance._client.table_data_client
        from google.cloud.bigtable_v2.types import GenerateInitialChangeStreamPartitionsRequest

        request = GenerateInitialChangeStreamPartitionsRequest(
            table_name=table.name,
            app_profile_id=self.app_profile,
        )
        partitions = []
        for i, response in enumerate(
            data_client.generate_initial_change_stream_partitions(request=request)
        ):
            part = response.partition
            self._raw_partitions[i] = part
            rr = part.row_range
            start_key = bytes(rr.start_key_closed) if rr.start_key_closed else b""
            end_key = bytes(rr.end_key_open) if rr.end_key_open else b""
            p = BigtablePartition(
                partition_index=i,
                start_key=start_key,
                end_key=end_key,
                token=None,
            )
            partitions.append(p)
            self._partitions[i] = p
            self._tokens[i] = None
        return partitions

    def _read_partition_chunk(
        self, partition: BigtablePartition
    ) -> tuple[list, Optional[str]]:
        """
        Calls ReadChangeStream for one partition, collects up to
        max_rows_per_partition mutations, returns (rows, continuation_token).
        """
        from google.protobuf.timestamp_pb2 import Timestamp

        rows = []
        new_token = self._tokens.get(partition.partition_index)
        low_watermark = None

        _, table = self._get_client()

        # Use the exact StreamPartition from GenerateInitialChangeStreamPartitions so the
        # server matches the same logical partition and delivers change stream events.
        raw_partition = self._raw_partitions.get(partition.partition_index)
        if raw_partition is None:
            # Fallback if partition came from elsewhere (e.g. tests)
            END_OF_TABLE = b"\xff" * 32
            end_key_open = partition.end_key if partition.end_key else END_OF_TABLE
            raw_partition = {"row_range": {"start_key_closed": partition.start_key, "end_key_open": end_key_open}}

        request = {
            "table_name": table.name,
            "app_profile_id": self.app_profile,
            "partition": raw_partition,
            "heartbeat_duration": {"seconds": 5},
        }

        if new_token:
            request["continuation_tokens"] = {
                "tokens": [
                    {
                        "partition": raw_partition,
                        "token": new_token,
                    }
                ]
            }
        else:
            if self._start_timestamp is not None:
                start_ts = Timestamp()
                start_ts.seconds = int(self._start_timestamp.timestamp())
                start_ts.nanos = int(
                    (self._start_timestamp.timestamp() % 1) * 1_000_000_000
                )
                request["start_time"] = start_ts
            else:
                now_ts = Timestamp()
                now_ts.GetCurrentTime()
                request["start_time"] = now_ts

        data_client = table._instance._client.table_data_client

        try:
            stream = data_client.read_change_stream(request=request)

            count = 0
            heartbeats_without_data = 0
            for response in stream:
                # Proto-plus: check which oneof is set by truthiness (no HasField)
                if response.heartbeat:
                    hb = response.heartbeat
                    low_watermark = _to_datetime_utc(hb.estimated_low_watermark)
                    new_token = hb.continuation_token.token if hb.continuation_token else None
                    heartbeats_without_data += 1
                    # End micro-batch at heartbeat if we have rows, or after 3 heartbeats with no data
                    if count >= 1 or heartbeats_without_data >= 3:
                        break

                elif response.close_stream:
                    new_token = None
                    break

                elif response.data_change:
                    dc = response.data_change
                    commit_ts = _to_datetime_utc(dc.commit_timestamp)
                    new_token = dc.token
                    low_wm = _to_datetime_utc(dc.estimated_low_watermark) if dc.estimated_low_watermark else low_watermark
                    row_key = bytes(dc.row_key) if dc.row_key else b""

                    for chunk in dc.chunks:
                        if chunk.mutation:
                            mutation = self._parse_mutation(
                                chunk.mutation, row_key, commit_ts, low_wm, partition
                            )
                            if mutation:
                                rows.append(mutation)
                                count += 1

                    if count >= self.max_rows_per_partition:
                        break

        except Exception as e:
            print(
                f"[BigtableChangeStream] Error on partition "
                f"{partition.partition_index}: {e}"
            )

        return rows, new_token

    def _parse_mutation(
        self, mutation, row_key: bytes, commit_ts, low_wm, partition
    ) -> Optional[dict]:
        """Converts a ReadChangeStream Mutation (from DataChange.chunks[].mutation) into a flat dict."""
        cf = ""
        cq = b""
        value = b""
        mutation_type = None

        if mutation.set_cell:
            mutation_type = "SET_CELL"
            sc = mutation.set_cell
            cf = sc.family_name or ""
            cq = sc.column_qualifier or b""
            value = sc.value or b""
        elif mutation.delete_from_column:
            mutation_type = "DELETE_COLUMN"
            d = mutation.delete_from_column
            cf = d.family_name or ""
            cq = d.column_qualifier or b""
        elif mutation.delete_from_family:
            mutation_type = "DELETE_FAMILY"
            cf = mutation.delete_from_family.family_name or ""
        elif mutation.delete_from_row:
            mutation_type = "DELETE_ROW"
        else:
            return None

        return {
            "row_key": row_key,
            "column_family": cf,
            "column_qualifier": cq,
            "value": value,
            "mutation_type": mutation_type,
            "commit_timestamp": commit_ts,
            "partition_key": f"{partition.start_key!r}-{partition.end_key!r}",
            "low_watermark": low_wm,
        }


class BigtableChangeStreamReader(BigtableStreamReader, DataSourceStreamReader):
    """Streaming reader for Bigtable Change Streams."""

    pass
