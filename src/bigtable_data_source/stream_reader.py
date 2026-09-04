"""Bigtable Change Stream reader implementation."""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterator, List, Mapping, Optional, Tuple

from google.api_core import exceptions as google_api_exceptions
from pyspark.sql.datasource import DataSourceStreamReader, InputPartition

from .mutation_types import MutationType
from .partitioning import BigtablePartition

_LOG = logging.getLogger(__name__)


def _parse_positive_int(option_name: str, value: str | int) -> int:
    """Parse a positive integer option; raise ValueError if invalid."""
    try:
        n = int(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"{option_name} must be an integer") from e
    if n <= 0:
        raise ValueError(f"{option_name} must be a positive integer")
    return n


def _to_datetime_utc(ts: Any) -> Optional[datetime]:
    """Convert protobuf Timestamp or datetime-like to timezone-aware datetime."""
    if ts is None:
        return None
    if hasattr(ts, "ToDatetime"):
        return ts.ToDatetime(tzinfo=timezone.utc)
    if hasattr(ts, "timestamp"):
        return datetime.fromtimestamp(ts.timestamp(), tz=timezone.utc)
    # Already datetime-like (e.g. DatetimeWithNanoseconds from proto-plus)
    if hasattr(ts, "tzinfo") and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _cancel_stream(stream: Any) -> None:
    """Best-effort cancel of a gRPC server-streaming call.

    A bare ``break`` out of a gRPC server-streaming iterator does NOT cancel the
    call server-side; the channel slot stays occupied until the server deadline.
    Calling cancel() frees it immediately. Safe to call if the stream is already
    finished or does not support cancellation.
    """
    cancel = getattr(stream, "cancel", None)
    if cancel is None:
        return
    try:
        cancel()
    except Exception:  # pragma: no cover - cancellation is best-effort
        _LOG.debug("Ignoring error while cancelling change stream", exc_info=True)


def _row_range_keys(raw_partition: Any) -> Tuple[bytes, bytes]:
    """Extract (start_key_closed, end_key_open) bytes from a StreamPartition-like object."""
    rr = getattr(raw_partition, "row_range", None)
    if rr is None:
        return b"", b""
    start = getattr(rr, "start_key_closed", b"") or b""
    end = getattr(rr, "end_key_open", b"") or b""
    return bytes(start), bytes(end)


def _extract_close_successors(close_stream: Any) -> List[Tuple[Any, Optional[str]]]:
    """Extract (raw_partition, token) successors from a CloseStream message.

    On a tablet split or merge, ``CloseStream.continuation_tokens`` describes the new
    partition(s) and the token(s) to resume from. Returns an empty list when the
    stream closed without successors (e.g. the change stream was dropped).
    """
    tokens = getattr(close_stream, "continuation_tokens", None) or []
    successors: List[Tuple[Any, Optional[str]]] = []
    for ct in tokens:
        raw_partition = getattr(ct, "partition", None)
        token = getattr(ct, "token", None)
        if raw_partition is not None:
            successors.append((raw_partition, token))
    return successors


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
      read_stream_timeout_seconds: Max wall-clock seconds per partition per
        read_change_stream call (default max(120, batch_duration_seconds * 12)).
        Prevents a stalled gRPC stream from hanging the micro-batch indefinitely.
      heartbeat_duration_seconds: Interval in seconds between server heartbeats
        on the change stream gRPC (default 5). Lower values make empty batches
        complete faster at the cost of more heartbeat messages.
      empty_heartbeat_limit: Number of consecutive heartbeats with no data
        before ending the micro-batch (default 3). Lower values reduce latency
        for empty batches but may cause the reader to return before data arrives.
    """

    def __init__(self, options: Mapping[str, Any]) -> None:
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
        self.batch_seconds = _parse_positive_int(
            "batch_duration_seconds",
            options.get("batch_duration_seconds", "10"),
        )
        self.max_rows_per_partition = _parse_positive_int(
            "max_rows_per_partition",
            options.get("max_rows_per_partition", "5000"),
        )
        _default_stream_timeout = str(max(120, self.batch_seconds * 12))
        self.read_stream_timeout_seconds = _parse_positive_int(
            "read_stream_timeout_seconds",
            options.get("read_stream_timeout_seconds", _default_stream_timeout),
        )
        self.heartbeat_duration_seconds = _parse_positive_int(
            "heartbeat_duration_seconds",
            options.get("heartbeat_duration_seconds", "5"),
        )
        self.empty_heartbeat_limit = _parse_positive_int(
            "empty_heartbeat_limit",
            options.get("empty_heartbeat_limit", "3"),
        )
        # Optional: JSON string of service account key dict; if set, use it instead of ADC
        self._credentials_json = options.get("credentials_json")
        if self._credentials_json:
            _LOG.warning(
                "The credentials_json Spark option embeds private key material in the job "
                "configuration; it may appear in Spark UI, logs, and event streams. For "
                "production, prefer Application Default Credentials (ADC) or Workload "
                "Identity Federation instead."
            )
        # Optional: when no continuation token, start from this time (ISO 8601 str or Unix seconds)
        self._start_timestamp: Optional[datetime] = self._parse_start_timestamp(
            options.get("start_timestamp")
        )
        self.options = dict(options)

        # partition_index → list of row dicts
        self._buffered_rows: dict[int, list] = {}
        # partition_index → continuation token string
        self._tokens: dict[int, Optional[str]] = {}
        # partition_index → BigtablePartition
        self._partitions: dict[int, BigtablePartition] = {}
        # partition_index → raw StreamPartition from API (for exact request match)
        self._raw_partitions: dict[int, object] = {}
        # Monotonic counter for allocating new partition indices when tablets
        # split/merge mid-stream (see _register_successor_partition). Never reused.
        self._partition_counter: int = 0

        self._client: Optional[Any] = None
        self._table: Optional[Any] = None
        self._initial_offset_completed = False
        # Guards client lifecycle so stop() cannot close the client while
        # latestOffset()'s worker threads are still using it.
        self._lock = threading.Lock()
        self._stopped = False

    def _validate_options(self, options: Mapping[str, Any]) -> None:
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

    def _get_client(self) -> Tuple[Any, Any]:
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
                    _bt_scopes = (
                        "https://www.googleapis.com/auth/bigtable.data",
                        "https://www.googleapis.com/auth/bigtable.admin",
                    )
                    credentials = service_account.Credentials.from_service_account_info(
                        sa_info,
                        scopes=list(_bt_scopes),
                    )
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    raise ValueError(
                        f"credentials_json option is invalid or malformed: {e}"
                    ) from e
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
        self._initial_offset_completed = True
        return {str(p.partition_index): None for p in partitions}

    def latestOffset(self) -> dict:
        """
        Called each micro-batch trigger. Reads up to max_rows_per_partition
        changes from each partition and buffers them. Returns new token offsets.
        If Spark has not called initialOffset() yet (e.g. new reader instance),
        we discover partitions here so the stream can proceed.
        """
        if not self._initial_offset_completed:
            _ = self._fetch_partition_metadata()
            self._initial_offset_completed = True

        # Hold the lifecycle lock for the whole batch so stop() cannot close the
        # client while the worker threads below are still reading from it.
        with self._lock:
            if self._stopped:
                return {str(idx): tok for idx, tok in self._tokens.items()}
            # Ensure the client is created before parallel partition reads.
            self._get_client()
            self._buffered_rows = {}
            new_offsets: dict = {}

            # Snapshot the partition set; topology changes from splits/merges are
            # applied only after the executor has drained.
            partitions_snapshot = dict(self._partitions)
            # Read partitions in parallel to avoid 200+ sequential round-trips per batch.
            max_workers = min(32, max(1, len(partitions_snapshot)))
            closed_indices: List[int] = []
            pending_successors: List[Tuple[Any, Optional[str]]] = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_idx = {
                    executor.submit(self._read_partition_chunk, partition): idx
                    for idx, partition in partitions_snapshot.items()
                }
                for future in as_completed(future_to_idx):
                    idx = future_to_idx[future]
                    try:
                        rows, new_token, successors = future.result()
                    except Exception:
                        _LOG.exception("Partition %s read failed", idx)
                        raise
                    self._buffered_rows[idx] = rows
                    if successors:
                        # Partition closed (split/merge): drop it and adopt its
                        # successors so data keeps flowing next micro-batch.
                        closed_indices.append(idx)
                        pending_successors.extend(successors)
                    else:
                        new_offsets[str(idx)] = new_token
                        self._tokens[idx] = new_token

            # Apply topology changes now that no worker is iterating the maps.
            for idx in closed_indices:
                self._partitions.pop(idx, None)
                self._raw_partitions.pop(idx, None)
                self._tokens.pop(idx, None)
                new_offsets.pop(str(idx), None)
            for raw_partition, token in pending_successors:
                new_idx = self._register_successor_partition(raw_partition, token)
                if new_idx is not None:
                    new_offsets[str(new_idx)] = token

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

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        """
        Called on Spark executors. Yields rows from this partition.

        Rows are carried on the partition object (BigtablePartition.rows) so they are
        available on the executor; driver-side buffers are not serialized to executors.
        """
        rows = getattr(partition, "rows", [])
        for row in rows:
            yield (
                row["row_key"],
                row["column_family"],
                row["column_qualifier"],
                row["value"],
                row["mutation_type"],
                row["commit_timestamp"],
                row["partition_start_key"],
                row["partition_end_key"],
                row["low_watermark"],
            )

    def commit(self, end: dict) -> None:
        """Called after a batch completes successfully."""
        pass

    def stop(self) -> None:
        # Serialize with latestOffset() so we never close the client out from under
        # in-flight worker threads (which would surface as CANCELLED errors).
        with self._lock:
            self._stopped = True
            if self._client is not None:
                self._client.close()
                self._client = None
                self._table = None

    # -- Internal helpers --

    def _data_client(self, table: Any) -> Any:
        """Return the low-level Bigtable data client (gapic stub) for change stream RPCs.

        NOTE: this reaches into private attributes of the google-cloud-bigtable client
        because the change stream RPCs (GenerateInitialChangeStreamPartitions,
        ReadChangeStream) are not exposed on the public Table API. The coupling is
        centralised here so it lives in one place and fails with a clear, actionable
        message if the library's internal layout changes across versions.
        """
        try:
            return table._instance._client.table_data_client
        except AttributeError as e:
            raise RuntimeError(
                "Could not access the Bigtable data client via "
                "table._instance._client.table_data_client. The installed "
                "google-cloud-bigtable version may have changed its internal layout."
            ) from e

    def _fetch_partition_metadata(self) -> List[BigtablePartition]:
        """
        Discover change stream partitions via GenerateInitialChangeStreamPartitions.
        Uses the same partition layout the change stream API expects.
        """
        _, table = self._get_client()
        data_client = self._data_client(table)
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
        # Next free index for successor partitions created by mid-stream splits/merges.
        self._partition_counter = max(self._partitions, default=-1) + 1
        return partitions

    def _register_successor_partition(
        self, raw_partition: Any, token: Optional[str]
    ) -> Optional[int]:
        """Register a successor partition produced by a mid-stream split/merge.

        Returns the new partition index, or None if a partition covering the same key
        range is already active (dedup, e.g. both parents of a merge point at it).
        """
        start_key, end_key = _row_range_keys(raw_partition)
        for existing in self._partitions.values():
            if existing.start_key == start_key and existing.end_key == end_key:
                return None
        idx = self._partition_counter
        self._partition_counter += 1
        self._partitions[idx] = BigtablePartition(
            partition_index=idx, start_key=start_key, end_key=end_key, token=token
        )
        self._raw_partitions[idx] = raw_partition
        self._tokens[idx] = token
        return idx

    def _read_partition_chunk(
        self, partition: BigtablePartition
    ) -> Tuple[list, Optional[str], List[Tuple[Any, Optional[str]]]]:
        """
        Calls ReadChangeStream for one partition, collects up to
        max_rows_per_partition mutations, returns (rows, continuation_token, successors).

        ``successors`` is non-empty only when the partition closed due to a tablet
        split/merge; it carries the (raw_partition, token) pairs to resume from.
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
            raw_partition = {
                "row_range": {
                    "start_key_closed": partition.start_key,
                    "end_key_open": end_key_open,
                },
            }

        request = {
            "table_name": table.name,
            "app_profile_id": self.app_profile,
            "partition": raw_partition,
            "heartbeat_duration": {"seconds": self.heartbeat_duration_seconds},
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

        data_client = self._data_client(table)
        successors: List[Tuple[Any, Optional[str]]] = []

        try:
            stream = data_client.read_change_stream(request=request)
            try:
                count = 0
                heartbeats_without_data = 0
                deadline = time.monotonic() + float(self.read_stream_timeout_seconds)
                for response in stream:
                    if time.monotonic() > deadline:
                        _LOG.warning(
                            "read_change_stream exceeded read_stream_timeout_seconds=%s "
                            "for partition %s; breaking to avoid hanging the micro-batch",
                            self.read_stream_timeout_seconds,
                            partition.partition_index,
                        )
                        break
                    # Proto-plus: check which oneof is set by truthiness (no HasField)
                    if response.heartbeat:
                        hb = response.heartbeat
                        low_watermark = _to_datetime_utc(hb.estimated_low_watermark)
                        new_token = hb.continuation_token.token if hb.continuation_token else None
                        heartbeats_without_data += 1
                        # End micro-batch at heartbeat if we have rows, or after N heartbeats with no data
                        if count >= 1 or heartbeats_without_data >= self.empty_heartbeat_limit:
                            break

                    elif response.close_stream:
                        # Tablet split/merge: this partition no longer exists. Capture
                        # the successor partitions from continuation_tokens so the reader
                        # keeps consuming instead of silently re-requesting a dead
                        # partition forever (which would drop all later mutations).
                        successors = _extract_close_successors(response.close_stream)
                        new_token = None
                        break

                    elif response.data_change:
                        dc = response.data_change
                        commit_ts = _to_datetime_utc(dc.commit_timestamp)
                        new_token = dc.token
                        low_wm = (
                            _to_datetime_utc(dc.estimated_low_watermark)
                            if dc.estimated_low_watermark
                            else low_watermark
                        )
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
            finally:
                # Always cancel the server-streaming call so an early break does not
                # leak the gRPC channel slot until the server deadline.
                _cancel_stream(stream)

        except google_api_exceptions.Unauthenticated:
            _LOG.exception(
                "Bigtable authentication failed on partition %s",
                partition.partition_index,
            )
            raise
        except google_api_exceptions.PermissionDenied:
            _LOG.exception(
                "Bigtable permission denied on partition %s",
                partition.partition_index,
            )
            raise
        except google_api_exceptions.GoogleAPICallError:
            _LOG.exception(
                "Bigtable API error on partition %s",
                partition.partition_index,
            )
            raise
        except Exception:
            _LOG.exception(
                "Unexpected error reading change stream on partition %s",
                partition.partition_index,
            )
            raise

        return rows, new_token, successors

    def _parse_mutation(
        self,
        mutation: Any,
        row_key: bytes,
        commit_ts: Optional[datetime],
        low_wm: Optional[datetime],
        partition: BigtablePartition,
    ) -> Optional[dict]:
        """Converts a ReadChangeStream Mutation (from DataChange.chunks[].mutation) into a flat dict."""
        cf = ""
        cq = b""
        value = b""
        mutation_type = None

        if mutation.set_cell:
            mutation_type = MutationType.SET_CELL.value
            sc = mutation.set_cell
            cf = sc.family_name or ""
            cq = sc.column_qualifier or b""
            value = sc.value or b""
        elif mutation.delete_from_column:
            mutation_type = MutationType.DELETE_COLUMN.value
            d = mutation.delete_from_column
            cf = d.family_name or ""
            cq = d.column_qualifier or b""
        elif mutation.delete_from_family:
            mutation_type = MutationType.DELETE_FAMILY.value
            cf = mutation.delete_from_family.family_name or ""
        elif mutation.delete_from_row:
            mutation_type = MutationType.DELETE_ROW.value
        else:
            return None

        return {
            "row_key": row_key,
            "column_family": cf,
            "column_qualifier": cq,
            "value": value,
            "mutation_type": mutation_type,
            "commit_timestamp": commit_ts,
            "partition_start_key": partition.start_key,
            "partition_end_key": partition.end_key,
            "low_watermark": low_wm,
        }


# latestOffset uses the backward-compatible no-arg signature (start/limit were added in
# Spark 4.2; Spark keeps the legacy form working — see DataSourceStreamReader.latestOffset).
# mypy flags the mismatch across the two base classes; it is intentional and safe.
class BigtableChangeStreamReader(BigtableStreamReader, DataSourceStreamReader):  # type: ignore[misc]
    """Streaming reader for Bigtable Change Streams."""

    pass
