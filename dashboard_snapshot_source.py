from __future__ import annotations

import hashlib
import json
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


SOURCE_NAME = "lakeview_dashboard_snapshot_sdk"
PRODUCT_NAME = "lakeview-dashboard-history-dlt"
PRODUCT_VERSION = "0.1.0"
FREQ_BUCKETS = ("minutely", "hourly", "daily", "weekly", "monthly", "yearly", "custom", "unknown")

OUTPUT_SCHEMA = StructType(
    [
        StructField("dashboard_id", StringType(), False),
        StructField("create_time", TimestampType(), True),
        StructField("display_name", StringType(), True),
        StructField("etag", StringType(), True),
        StructField("lifecycle_state", StringType(), True),
        StructField("parent_path", StringType(), True),
        StructField("path", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("dashboard_metadata_json", StringType(), True),
        StructField("dashboard_metadata_json_sha256", StringType(), True),
        StructField("serialized_dashboard", StringType(), True),
        StructField("serialized_dashboard_sha256", StringType(), True),
        StructField("update_time", TimestampType(), True),
        StructField("schedules_json", StringType(), True),
        StructField("schedules_json_sha256", StringType(), True),
        StructField("schedule_count", IntegerType(), True),
        StructField("schedule_frequency_minutely", IntegerType(), True),
        StructField("schedule_frequency_hourly", IntegerType(), True),
        StructField("schedule_frequency_daily", IntegerType(), True),
        StructField("schedule_frequency_weekly", IntegerType(), True),
        StructField("schedule_frequency_monthly", IntegerType(), True),
        StructField("schedule_frequency_yearly", IntegerType(), True),
        StructField("schedule_frequency_custom", IntegerType(), True),
        StructField("schedule_frequency_unknown", IntegerType(), True),
    ]
)


def _progress_log(message: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{timestamp}] {PRODUCT_NAME}: {message}", flush=True)


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_int(value: Any, default: int, minimum: int = 0) -> int:
    if value in (None, ""):
        return default
    parsed = int(value)
    return max(minimum, parsed)


def _coerce_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, (int, float)):
        seconds = value / 1000.0 if value > 1_000_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        normalized = normalized.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    return None


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def _canonical_json(value: Any) -> str:
    return json.dumps(_to_jsonable(value), sort_keys=True, separators=(",", ":"), default=_json_default)


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


def _owner_from_parent_path(parent_path: Any) -> str | None:
    if parent_path is None:
        return None
    parent = str(parent_path)
    if not parent.startswith("/Users/"):
        return None
    parts = parent.split("/")
    return parts[2] if len(parts) >= 3 else None


def _get_cron_frequency(expr: Any) -> str:
    if not expr or not isinstance(expr, str):
        return "unknown"
    parts = expr.strip().split()
    if len(parts) not in (6, 7):
        return "custom"

    _, minute, hour, dom, month, dow = parts[:6]
    dom_any = dom in ("*", "?")
    dow_any = dow in ("*", "?")
    month_any = month in ("*", "?")

    if minute in ("*", "*/1") and hour in ("*", "*/1") and dom_any and month_any and dow_any:
        return "minutely"
    if hour in ("*", "*/1") and dom_any and month_any and dow_any:
        return "hourly"
    if dom_any and month_any and dow_any:
        return "daily"
    if dow not in ("*", "?") and dom_any and month_any:
        return "weekly"
    if dom not in ("*", "?") and month_any and dow in ("*", "?"):
        return "monthly"
    if dom not in ("*", "?") and month not in ("*", "?") and dow in ("*", "?"):
        return "yearly"
    return "custom"


def _annotate_schedule(schedule_raw: dict[str, Any]) -> dict[str, Any]:
    schedule = dict(schedule_raw)
    cron_schedule = schedule.get("cron_schedule") or {}
    cron_schedule = dict(cron_schedule) if isinstance(cron_schedule, dict) else {}
    cron_schedule["human_schedule"] = _get_cron_frequency(cron_schedule.get("quartz_cron_expression"))
    schedule["cron_schedule"] = cron_schedule
    return _to_jsonable(schedule)


def _schedule_frequency_counts(schedules: list[dict[str, Any]]) -> dict[str, int]:
    counts = {bucket: 0 for bucket in FREQ_BUCKETS}
    for schedule in schedules:
        cron_schedule = schedule.get("cron_schedule") or {}
        bucket = cron_schedule.get("human_schedule") or _get_cron_frequency(cron_schedule.get("quartz_cron_expression"))
        counts[bucket if bucket in counts else "unknown"] += 1
    return counts


def _dashboard_row(detail_raw: dict[str, Any], schedules: list[dict[str, Any]], include_serialized_dashboard: bool) -> dict[str, Any]:
    serialized_dashboard = detail_raw.get("serialized_dashboard")
    if serialized_dashboard is not None and not isinstance(serialized_dashboard, str):
        serialized_dashboard = _canonical_json(serialized_dashboard)
    if not include_serialized_dashboard:
        serialized_dashboard = None

    dashboard_metadata = {k: _to_jsonable(v) for k, v in detail_raw.items() if k != "serialized_dashboard"}
    dashboard_metadata_json = _canonical_json(dashboard_metadata)
    schedules_json = _canonical_json(schedules) if schedules else None
    schedule_counts = _schedule_frequency_counts(schedules)

    return {
        "dashboard_id": str(detail_raw.get("dashboard_id")),
        "create_time": _coerce_timestamp(detail_raw.get("create_time")),
        "display_name": _normalize_scalar(detail_raw.get("display_name")),
        "etag": _normalize_scalar(detail_raw.get("etag")),
        "lifecycle_state": _normalize_scalar(detail_raw.get("lifecycle_state")),
        "parent_path": _normalize_scalar(detail_raw.get("parent_path")),
        "path": _normalize_scalar(detail_raw.get("path")),
        "warehouse_id": _normalize_scalar(detail_raw.get("warehouse_id")),
        "owner": _owner_from_parent_path(detail_raw.get("parent_path")),
        "dashboard_metadata_json": dashboard_metadata_json,
        "dashboard_metadata_json_sha256": hashlib.sha256(dashboard_metadata_json.encode("utf-8")).hexdigest()
        if dashboard_metadata_json
        else None,
        "serialized_dashboard": serialized_dashboard,
        "serialized_dashboard_sha256": hashlib.sha256(serialized_dashboard.encode("utf-8")).hexdigest()
        if serialized_dashboard
        else None,
        "update_time": _coerce_timestamp(detail_raw.get("update_time")),
        "schedules_json": schedules_json,
        "schedules_json_sha256": hashlib.sha256(schedules_json.encode("utf-8")).hexdigest() if schedules_json else None,
        "schedule_count": len(schedules),
        "schedule_frequency_minutely": schedule_counts["minutely"],
        "schedule_frequency_hourly": schedule_counts["hourly"],
        "schedule_frequency_daily": schedule_counts["daily"],
        "schedule_frequency_weekly": schedule_counts["weekly"],
        "schedule_frequency_monthly": schedule_counts["monthly"],
        "schedule_frequency_yearly": schedule_counts["yearly"],
        "schedule_frequency_custom": schedule_counts["custom"],
        "schedule_frequency_unknown": schedule_counts["unknown"],
    }


def _build_workspace_client(options: dict[str, Any]):
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config

    config_kwargs = {
        "product": PRODUCT_NAME,
        "product_version": PRODUCT_VERSION,
        "config_file": options.get("config_file") or "/dev/null",
        "max_connection_pools": _parse_int(options.get("http_max_connection_pools"), default=1, minimum=1),
        "max_connections_per_pool": _parse_int(options.get("http_max_connections_per_pool"), default=2, minimum=1),
    }
    host = options.get("host")
    token = options.get("token")
    profile = options.get("profile")

    if host:
        config_kwargs["host"] = host
    if token:
        config_kwargs["token"] = token
    elif profile:
        config_kwargs["profile"] = profile
    else:
        config_kwargs["auth_type"] = options.get("auth_type") or "runtime"

    return WorkspaceClient(config=Config(**config_kwargs))


def _extract_driver_auth(client) -> tuple[str | None, str | None]:
    try:
        oauth_token = client.config.oauth_token()
        if oauth_token.access_token:
            return client.config.host, oauth_token.access_token
    except Exception:
        pass

    try:
        headers = client.config.authenticate()
    except Exception:
        return None, None

    auth_header = headers.get("Authorization") or headers.get("authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        return None, None
    return client.config.host, auth_header.split(" ", 1)[1]


@dataclass
class DashboardIdPartition(InputPartition):
    partition_id: int
    dashboard_ids: list[str]
    host: str | None = None
    token: str | None = None


@dataclass
class DashboardFetchResult:
    status: str
    dashboard_id: str
    row: dict[str, Any] | None = None
    error_message: str | None = None


def _yield_batches(rows: list[dict[str, Any]], output_batch_size: int):
    try:
        import pyarrow as pa

        arrow_schema = pa.schema(
            [
                pa.field("dashboard_id", pa.string(), nullable=False),
                pa.field("create_time", pa.timestamp("us"), nullable=True),
                pa.field("display_name", pa.string(), nullable=True),
                pa.field("etag", pa.string(), nullable=True),
                pa.field("lifecycle_state", pa.string(), nullable=True),
                pa.field("parent_path", pa.string(), nullable=True),
                pa.field("path", pa.string(), nullable=True),
                pa.field("warehouse_id", pa.string(), nullable=True),
                pa.field("owner", pa.string(), nullable=True),
                pa.field("dashboard_metadata_json", pa.string(), nullable=True),
                pa.field("dashboard_metadata_json_sha256", pa.string(), nullable=True),
                pa.field("serialized_dashboard", pa.string(), nullable=True),
                pa.field("serialized_dashboard_sha256", pa.string(), nullable=True),
                pa.field("update_time", pa.timestamp("us"), nullable=True),
                pa.field("schedules_json", pa.string(), nullable=True),
                pa.field("schedules_json_sha256", pa.string(), nullable=True),
                pa.field("schedule_count", pa.int32(), nullable=True),
                pa.field("schedule_frequency_minutely", pa.int32(), nullable=True),
                pa.field("schedule_frequency_hourly", pa.int32(), nullable=True),
                pa.field("schedule_frequency_daily", pa.int32(), nullable=True),
                pa.field("schedule_frequency_weekly", pa.int32(), nullable=True),
                pa.field("schedule_frequency_monthly", pa.int32(), nullable=True),
                pa.field("schedule_frequency_yearly", pa.int32(), nullable=True),
                pa.field("schedule_frequency_custom", pa.int32(), nullable=True),
                pa.field("schedule_frequency_unknown", pa.int32(), nullable=True),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=arrow_schema)
        yield from table.to_batches(max_chunksize=output_batch_size)
    except Exception:
        column_names = [field.name for field in OUTPUT_SCHEMA.fields]
        for row in rows:
            yield tuple(row.get(name) for name in column_names)


class LakeviewDashboardSnapshotReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict[str, str]):
        self.schema = schema
        self.options = dict(options)

    def partitions(self):
        from databricks.sdk.service.dashboards import DashboardView

        progress_interval = _parse_int(self.options.get("progress_log_interval"), default=1000, minimum=1)
        requested_parallelism = _parse_int(self.options.get("parallelism"), default=8, minimum=1)
        page_size = _parse_int(self.options.get("page_size"), default=1000, minimum=1)
        requested_ids = self.options.get("dashboard_ids")

        if requested_ids:
            dashboard_ids = [dashboard_id.strip() for dashboard_id in requested_ids.split(",") if dashboard_id.strip()]
            host = self.options.get("host")
            token = self.options.get("token")
            _progress_log(f"using requested dashboard id list; count={len(dashboard_ids)}")
        else:
            client = _build_workspace_client(self.options)
            dashboard_ids = []
            for dashboard in client.lakeview.list(
                page_size=page_size,
                show_trashed=False,
                view=DashboardView.DASHBOARD_VIEW_BASIC,
            ):
                raw = dashboard.as_dict() if hasattr(dashboard, "as_dict") else vars(dashboard)
                dashboard_id = raw.get("dashboard_id")
                if dashboard_id:
                    dashboard_ids.append(str(dashboard_id))
                    if len(dashboard_ids) % progress_interval == 0:
                        _progress_log(f"listed dashboard ids; count={len(dashboard_ids)}")

            dashboard_ids = sorted(set(dashboard_ids))
            _progress_log(f"completed dashboard id listing; unique_count={len(dashboard_ids)}")
            dashboard_limit = _parse_int(self.options.get("dashboard_limit"), default=0, minimum=0)
            if dashboard_limit:
                dashboard_ids = dashboard_ids[:dashboard_limit]
                _progress_log(f"applied dashboard limit; limited_count={len(dashboard_ids)}")

            host = token = None
            if _parse_bool(self.options.get("use_driver_auth"), default=True):
                host, token = _extract_driver_auth(client)

        if not dashboard_ids:
            return [DashboardIdPartition(partition_id=0, dashboard_ids=[], host=host, token=token)]

        parallelism = min(requested_parallelism, len(dashboard_ids))
        chunk_size = math.ceil(len(dashboard_ids) / parallelism)
        _progress_log(
            f"creating partitions for dashboard source; dashboard_count={len(dashboard_ids)} parallelism={parallelism} chunk_size={chunk_size}"
        )

        partitions = []
        for partition_id in range(parallelism):
            start = partition_id * chunk_size
            stop = min(start + chunk_size, len(dashboard_ids))
            chunk = dashboard_ids[start:stop]
            if chunk:
                partitions.append(
                    DashboardIdPartition(
                        partition_id=partition_id,
                        dashboard_ids=chunk,
                        host=host,
                        token=token,
                    )
                )

        _progress_log(f"partition planning complete; partition_count={len(partitions)}")
        return partitions

    def read(self, partition: DashboardIdPartition):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from databricks.sdk.errors import NotFound, ResourceDoesNotExist
        from databricks.sdk.errors.platform import BadRequest, InvalidParameterValue

        include_serialized_dashboard = _parse_bool(self.options.get("include_serialized_dashboard"), default=True)
        per_partition_threads = _parse_int(self.options.get("per_partition_threads"), default=2, minimum=1)
        output_batch_size = _parse_int(self.options.get("output_batch_size"), default=128, minimum=1)
        progress_interval = _parse_int(self.options.get("progress_log_interval"), default=1000, minimum=1)

        thread_state = threading.local()
        processed_count = 0
        emitted_count = 0
        missing_count = 0
        invalid_count = 0

        _progress_log(
            f"partition {partition.partition_id} starting; dashboard_count={len(partition.dashboard_ids)} threads={per_partition_threads} batch_size={output_batch_size}"
        )

        def get_client():
            if not hasattr(thread_state, "client"):
                client_options = dict(self.options)
                if partition.host:
                    client_options["host"] = partition.host
                if partition.token:
                    client_options["token"] = partition.token
                thread_state.client = _build_workspace_client(client_options)
            return thread_state.client

        def fetch_dashboard_row(dashboard_id: str) -> DashboardFetchResult:
            client = get_client()
            try:
                dashboard = client.lakeview.get(dashboard_id)
            except (NotFound, ResourceDoesNotExist):
                return DashboardFetchResult(status="missing", dashboard_id=dashboard_id)
            except (InvalidParameterValue, BadRequest) as err:
                return DashboardFetchResult(status="invalid", dashboard_id=dashboard_id, error_message=str(err))

            detail_raw = dashboard.as_dict() if hasattr(dashboard, "as_dict") else vars(dashboard)
            schedules: list[dict[str, Any]] = []
            try:
                for schedule in client.lakeview.list_schedules(dashboard_id, page_size=10):
                    schedule_raw = schedule.as_dict() if hasattr(schedule, "as_dict") else vars(schedule)
                    schedules.append(_annotate_schedule(schedule_raw))
            except (NotFound, ResourceDoesNotExist):
                schedules = []

            schedules.sort(key=lambda schedule: ((schedule.get("schedule_id") or ""), (schedule.get("display_name") or "")))
            return DashboardFetchResult(
                status="ok",
                dashboard_id=dashboard_id,
                row=_dashboard_row(detail_raw, schedules, include_serialized_dashboard),
            )

        def handle_result(result: DashboardFetchResult):
            nonlocal processed_count, emitted_count, missing_count, invalid_count

            processed_count += 1
            if result.status == "ok" and result.row is not None:
                pending_rows.append(result.row)
            elif result.status == "missing":
                missing_count += 1
            elif result.status == "invalid":
                invalid_count += 1
                if invalid_count <= 10 or invalid_count % progress_interval == 0:
                    _progress_log(
                        f"partition {partition.partition_id} skipping unreadable dashboard payload; dashboard_id={result.dashboard_id} invalid_total={invalid_count} error={result.error_message}"
                    )

            if processed_count % progress_interval == 0:
                _progress_log(
                    f"partition {partition.partition_id} progress; processed={processed_count} buffered={len(pending_rows)} emitted={emitted_count} missing={missing_count} invalid={invalid_count}"
                )

            if len(pending_rows) >= output_batch_size:
                emitted_count += len(pending_rows)
                _progress_log(
                    f"partition {partition.partition_id} emitting batch; batch_rows={len(pending_rows)} emitted_total={emitted_count}"
                )
                yield from _yield_batches(pending_rows, output_batch_size)
                pending_rows.clear()

        pending_rows: list[dict[str, Any]] = []
        if per_partition_threads == 1:
            for dashboard_id in partition.dashboard_ids:
                yield from handle_result(fetch_dashboard_row(dashboard_id))
        else:
            with ThreadPoolExecutor(max_workers=per_partition_threads) as pool:
                futures = {pool.submit(fetch_dashboard_row, dashboard_id): dashboard_id for dashboard_id in partition.dashboard_ids}
                for future in as_completed(futures):
                    yield from handle_result(future.result())

        if pending_rows:
            emitted_count += len(pending_rows)
            _progress_log(
                f"partition {partition.partition_id} emitting final batch; batch_rows={len(pending_rows)} emitted_total={emitted_count}"
            )
            yield from _yield_batches(pending_rows, output_batch_size)

        _progress_log(
            f"partition {partition.partition_id} complete; processed={processed_count} emitted={emitted_count} missing={missing_count} invalid={invalid_count}"
        )


class LakeviewDashboardSnapshotSource(DataSource):
    @classmethod
    def name(cls):
        return SOURCE_NAME

    def schema(self):
        return OUTPUT_SCHEMA

    def reader(self, schema: StructType):
        return LakeviewDashboardSnapshotReader(schema, self.options)


_REGISTERED = False


def register_lakeview_dashboard_snapshot_source(spark):
    global _REGISTERED
    if not _REGISTERED:
        spark.dataSource.register(LakeviewDashboardSnapshotSource)
        _REGISTERED = True
