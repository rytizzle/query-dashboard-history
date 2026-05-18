from __future__ import annotations

import atexit
import os
import threading
from datetime import datetime, timezone
from typing import Any

from pyspark import pipelines as dp
from pyspark.sql import functions as F


THIS_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
with open(os.path.join(THIS_DIR, "dashboard_snapshot_source.py"), "r", encoding="utf-8") as _source_file:
    exec(_source_file.read(), globals())


HISTORY_TABLE = "lakeview_dashboard_history"
CURRENT_TABLE = "lakeview_dashboard_current"
DENORMALIZED_TABLE = "lakeview_dashboard_denormalized"

_EPHEMERAL_SOURCE_TOKEN_LOCK = threading.Lock()
_EPHEMERAL_SOURCE_TOKEN: tuple[str, str] | None = None
_EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS: int | None = None
_SOURCE_SNAPSHOT_VERSION = int(datetime.now(timezone.utc).timestamp() * 1000)


def _ddl(lines: tuple[str, ...]) -> str:
    return ",\n".join(lines)


HISTORY_TABLE_SCHEMA = _ddl(
    (
        "dashboard_id STRING COMMENT 'Stable Lakeview dashboard identifier'",
        "create_time TIMESTAMP COMMENT 'Time the dashboard was created'",
        "display_name STRING COMMENT 'Dashboard display name'",
        "etag STRING COMMENT 'Optimistic concurrency token returned by Lakeview'",
        "lifecycle_state STRING COMMENT 'Lifecycle state returned by Lakeview'",
        "parent_path STRING COMMENT 'Workspace parent path that owns the dashboard'",
        "path STRING COMMENT 'Workspace path for the dashboard'",
        "warehouse_id STRING COMMENT 'Warehouse configured for the dashboard, if any'",
        "owner STRING COMMENT 'Owner email inferred from the workspace path when available'",
        "dashboard_metadata_json STRING COMMENT 'Canonical JSON for dashboard metadata excluding serialized_dashboard'",
        "dashboard_metadata_json_sha256 STRING COMMENT 'SHA-256 hash of dashboard_metadata_json'",
        "serialized_dashboard STRING COMMENT 'Serialized dashboard definition retained for content history'",
        "serialized_dashboard_sha256 STRING COMMENT 'SHA-256 hash of serialized_dashboard'",
        "update_time TIMESTAMP COMMENT 'Last dashboard update time returned by Lakeview'",
        "schedules_json STRING COMMENT 'Canonical JSON array of dashboard schedules'",
        "schedules_json_sha256 STRING COMMENT 'SHA-256 hash of schedules_json'",
        "schedule_count INT COMMENT 'Number of schedules attached to the dashboard'",
        "schedule_frequency_minutely INT COMMENT 'Count of schedules classified as minutely'",
        "schedule_frequency_hourly INT COMMENT 'Count of schedules classified as hourly'",
        "schedule_frequency_daily INT COMMENT 'Count of schedules classified as daily'",
        "schedule_frequency_weekly INT COMMENT 'Count of schedules classified as weekly'",
        "schedule_frequency_monthly INT COMMENT 'Count of schedules classified as monthly'",
        "schedule_frequency_yearly INT COMMENT 'Count of schedules classified as yearly'",
        "schedule_frequency_custom INT COMMENT 'Count of schedules classified as custom'",
        "schedule_frequency_unknown INT COMMENT 'Count of schedules classified as unknown'",
        "__START_AT BIGINT COMMENT 'Snapshot version when this history row became active'",
        "__END_AT BIGINT COMMENT 'Snapshot version when this history row stopped being active; null means current'",
    )
)

CURRENT_TABLE_SCHEMA = _ddl(
    (
        "dashboard_id STRING COMMENT 'Stable Lakeview dashboard identifier'",
        "create_time TIMESTAMP COMMENT 'Time the dashboard was created'",
        "display_name STRING COMMENT 'Dashboard display name'",
        "etag STRING COMMENT 'Optimistic concurrency token returned by Lakeview'",
        "lifecycle_state STRING COMMENT 'Lifecycle state returned by Lakeview'",
        "parent_path STRING COMMENT 'Workspace parent path that owns the dashboard'",
        "path STRING COMMENT 'Workspace path for the dashboard'",
        "warehouse_id STRING COMMENT 'Warehouse configured for the dashboard, if any'",
        "owner STRING COMMENT 'Owner email inferred from the workspace path when available'",
        "dashboard_metadata_json STRING COMMENT 'Canonical JSON for dashboard metadata excluding serialized_dashboard'",
        "dashboard_metadata_json_sha256 STRING COMMENT 'SHA-256 hash of dashboard_metadata_json'",
        "serialized_dashboard STRING COMMENT 'Serialized dashboard definition retained for content history'",
        "serialized_dashboard_sha256 STRING COMMENT 'SHA-256 hash of serialized_dashboard'",
        "update_time TIMESTAMP COMMENT 'Last dashboard update time returned by Lakeview'",
        "schedules_json STRING COMMENT 'Canonical JSON array of dashboard schedules'",
        "schedules_json_sha256 STRING COMMENT 'SHA-256 hash of schedules_json'",
        "schedule_count INT COMMENT 'Number of schedules attached to the dashboard'",
        "schedule_frequency_minutely INT COMMENT 'Count of schedules classified as minutely'",
        "schedule_frequency_hourly INT COMMENT 'Count of schedules classified as hourly'",
        "schedule_frequency_daily INT COMMENT 'Count of schedules classified as daily'",
        "schedule_frequency_weekly INT COMMENT 'Count of schedules classified as weekly'",
        "schedule_frequency_monthly INT COMMENT 'Count of schedules classified as monthly'",
        "schedule_frequency_yearly INT COMMENT 'Count of schedules classified as yearly'",
        "schedule_frequency_custom INT COMMENT 'Count of schedules classified as custom'",
        "schedule_frequency_unknown INT COMMENT 'Count of schedules classified as unknown'",
        "schedules VARIANT COMMENT 'Parsed schedule payload'",
        "schedule_frequency_counts STRUCT<minutely: INT, hourly: INT, daily: INT, weekly: INT, monthly: INT, yearly: INT, custom: INT, unknown: INT> COMMENT 'Schedule counts grouped by derived cadence bucket'",
        "has_schedule BOOLEAN COMMENT 'Whether the dashboard currently has at least one schedule'",
        "most_frequent_schedule_count INT COMMENT 'Maximum schedule count among cadence buckets'",
        "most_frequent_schedule STRING COMMENT 'Most common derived schedule cadence bucket'",
    )
)

DENORMALIZED_TABLE_SCHEMA = _ddl(
    (
        "dashboard_id STRING COMMENT 'Stable Lakeview dashboard identifier'",
        "create_time TIMESTAMP COMMENT 'Time the dashboard was created'",
        "etag STRING COMMENT 'Optimistic concurrency token returned by Lakeview'",
        "lifecycle_state STRING COMMENT 'Lifecycle state returned by Lakeview'",
        "parent_path STRING COMMENT 'Workspace parent path that owns the dashboard'",
        "path STRING COMMENT 'Workspace path for the dashboard'",
        "warehouse_id STRING COMMENT 'Warehouse configured for the dashboard, if any'",
        "owner STRING COMMENT 'Owner email inferred from the workspace path when available'",
        "dashboard_metadata_json STRING COMMENT 'Canonical JSON for dashboard metadata excluding serialized_dashboard'",
        "dashboard_metadata_json_sha256 STRING COMMENT 'SHA-256 hash of dashboard_metadata_json'",
        "serialized_dashboard STRING COMMENT 'Serialized dashboard definition retained for content history'",
        "serialized_dashboard_sha256 STRING COMMENT 'SHA-256 hash of serialized_dashboard'",
        "update_time TIMESTAMP COMMENT 'Last dashboard update time returned by Lakeview'",
        "schedules_json STRING COMMENT 'Canonical JSON array of dashboard schedules'",
        "schedules_json_sha256 STRING COMMENT 'SHA-256 hash of schedules_json'",
        "schedule_count INT COMMENT 'Number of schedules attached to the dashboard'",
        "schedule_frequency_minutely INT COMMENT 'Count of schedules classified as minutely'",
        "schedule_frequency_hourly INT COMMENT 'Count of schedules classified as hourly'",
        "schedule_frequency_daily INT COMMENT 'Count of schedules classified as daily'",
        "schedule_frequency_weekly INT COMMENT 'Count of schedules classified as weekly'",
        "schedule_frequency_monthly INT COMMENT 'Count of schedules classified as monthly'",
        "schedule_frequency_yearly INT COMMENT 'Count of schedules classified as yearly'",
        "schedule_frequency_custom INT COMMENT 'Count of schedules classified as custom'",
        "schedule_frequency_unknown INT COMMENT 'Count of schedules classified as unknown'",
        "schedules VARIANT COMMENT 'Parsed schedule payload'",
        "schedule_frequency_counts STRUCT<minutely: INT, hourly: INT, daily: INT, weekly: INT, monthly: INT, yearly: INT, custom: INT, unknown: INT> COMMENT 'Schedule counts grouped by derived cadence bucket'",
        "has_schedule BOOLEAN COMMENT 'Whether the dashboard currently has at least one schedule'",
        "most_frequent_schedule_count INT COMMENT 'Maximum schedule count among cadence buckets'",
        "most_frequent_schedule STRING COMMENT 'Most common derived schedule cadence bucket'",
        "dashboard_name STRING COMMENT 'Dashboard display name copied into the published denormalized output'",
        "total_task_duration_l30d BIGINT COMMENT 'Total query task duration in the last 30 days'",
        "count_queries_l30d BIGINT COMMENT 'Number of dashboard-linked queries in the last 30 days'",
        "views STRUCT<published_l30d: BIGINT, published_users_l30d: BIGINT, draft_l30d: BIGINT, draft_users_l30d: BIGINT> COMMENT 'Thirty day published and draft view counts and distinct viewer counts'",
    )
)

TRACK_HISTORY_COLUMNS = (
    "create_time",
    "display_name",
    "etag",
    "lifecycle_state",
    "parent_path",
    "path",
    "warehouse_id",
    "owner",
    "dashboard_metadata_json_sha256",
    "serialized_dashboard_sha256",
    "update_time",
    "schedules_json_sha256",
    "schedule_count",
    "schedule_frequency_minutely",
    "schedule_frequency_hourly",
    "schedule_frequency_daily",
    "schedule_frequency_weekly",
    "schedule_frequency_monthly",
    "schedule_frequency_yearly",
    "schedule_frequency_custom",
    "schedule_frequency_unknown",
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


def _build_pipeline_token_client(options: dict[str, Any]):
    from databricks.sdk import WorkspaceClient

    kwargs = {
        "product": PRODUCT_NAME,
        "config_file": "/dev/null",
        "auth_type": options.get("auth_type") or "runtime",
    }
    host = options.get("host")
    profile = options.get("profile")
    if host:
        kwargs["host"] = host
    if profile:
        kwargs["profile"] = profile
    return WorkspaceClient(**kwargs)


def _register_ephemeral_token_cleanup(client, token_id: str):
    def _cleanup():
        try:
            client.tokens.delete(token_id)
        except Exception:
            pass

    atexit.register(_cleanup)


def _get_ephemeral_source_token(options: dict[str, Any]) -> tuple[str, str] | None:
    global _EPHEMERAL_SOURCE_TOKEN, _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS

    if not _parse_bool(options.get("mint_ephemeral_token"), default=True):
        return None

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if _EPHEMERAL_SOURCE_TOKEN and _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS and _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS - now_ms > 60_000:
        return _EPHEMERAL_SOURCE_TOKEN

    with _EPHEMERAL_SOURCE_TOKEN_LOCK:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if _EPHEMERAL_SOURCE_TOKEN and _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS and _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS - now_ms > 60_000:
            return _EPHEMERAL_SOURCE_TOKEN

        client = _build_pipeline_token_client(options)
        lifetime_seconds = _parse_int(options.get("ephemeral_token_lifetime_seconds"), default=3600, minimum=300)
        created = client.tokens.create(
            comment=f"{PRODUCT_NAME}:{datetime.now(timezone.utc).isoformat()}",
            lifetime_seconds=lifetime_seconds,
        )
        if not created.token_value:
            raise ValueError("Token API returned no token_value for the ephemeral source token")

        token_info = created.token_info
        token_id = token_info.token_id if token_info else None
        expiry_time = token_info.expiry_time if token_info else None
        _EPHEMERAL_SOURCE_TOKEN = (client.config.host, created.token_value)
        _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS = expiry_time or (now_ms + lifetime_seconds * 1000)

        expiry_msg = (
            datetime.fromtimestamp(_EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            if _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS
            else "unknown"
        )
        _progress_log(f"minted ephemeral workspace token; expires_at={expiry_msg}")
        if token_id:
            _register_ephemeral_token_cleanup(client, token_id)
        return _EPHEMERAL_SOURCE_TOKEN


register_lakeview_dashboard_snapshot_source(spark)


def _source_options() -> dict[str, str]:
    options = {
        "host": spark.conf.get("lakeview.source.host", ""),
        "token": spark.conf.get("lakeview.source.token", ""),
        "profile": spark.conf.get("lakeview.source.profile", ""),
        "auth_type": spark.conf.get("lakeview.source.auth_type", ""),
        "mint_ephemeral_token": spark.conf.get("lakeview.source.mint_ephemeral_token", "true"),
        "ephemeral_token_lifetime_seconds": spark.conf.get("lakeview.source.ephemeral_token_lifetime_seconds", "3600"),
        "page_size": spark.conf.get("lakeview.source.page_size", "1000"),
        "dashboard_limit": spark.conf.get("lakeview.source.dashboard_limit", "0"),
        "parallelism": spark.conf.get("lakeview.source.parallelism", "32"),
        "per_partition_threads": spark.conf.get("lakeview.source.per_partition_threads", "2"),
        "output_batch_size": spark.conf.get("lakeview.source.output_batch_size", "128"),
        "progress_log_interval": spark.conf.get("lakeview.source.progress_log_interval", "1000"),
        "include_serialized_dashboard": spark.conf.get("lakeview.source.include_serialized_dashboard", "true"),
        "use_driver_auth": spark.conf.get("lakeview.source.use_driver_auth", "true"),
    }
    if not (options["host"] and options["token"]):
        ephemeral_token = _get_ephemeral_source_token(options)
        if ephemeral_token:
            options["host"], options["token"] = ephemeral_token
    return {key: value for key, value in options.items() if value not in (None, "")}


def _load_sdk_source():
    reader = spark.read.format(SOURCE_NAME)
    for key, value in _source_options().items():
        reader = reader.option(key, value)
    return reader.load()


def _next_sdk_snapshot(latest_snapshot_version: Any):
    if latest_snapshot_version is not None and int(latest_snapshot_version) >= _SOURCE_SNAPSHOT_VERSION:
        return None
    return _load_sdk_source(), _SOURCE_SNAPSHOT_VERSION


dp.create_streaming_table(
    name=HISTORY_TABLE,
    comment="SCD Type 2 dashboard history loaded directly from the Lakeview SDK source.",
    schema=HISTORY_TABLE_SCHEMA,
)

dp.create_auto_cdc_from_snapshot_flow(
    target=HISTORY_TABLE,
    source=_next_sdk_snapshot,
    keys=["dashboard_id"],
    stored_as_scd_type=2,
    track_history_column_list=list(TRACK_HISTORY_COLUMNS),
)


def _schedule_count_columns():
    return [f"schedule_frequency_{bucket}" for bucket in FREQ_BUCKETS]


def _most_frequent_schedule_expr() -> F.Column:
    max_count = F.greatest(*[F.coalesce(F.col(name), F.lit(0)) for name in _schedule_count_columns()])
    expr = F.when(max_count <= 0, F.lit(None).cast("string"))
    for bucket in FREQ_BUCKETS:
        expr = expr.when(F.coalesce(F.col(f"schedule_frequency_{bucket}"), F.lit(0)) == max_count, F.lit(bucket))
    return expr.otherwise(F.lit(None).cast("string"))


@dp.materialized_view(
    name=CURRENT_TABLE,
    comment="Curated current dashboard metadata derived from active SCD Type 2 rows.",
    schema=CURRENT_TABLE_SCHEMA,
)
def lakeview_dashboard_current():
    schedule_count_columns = _schedule_count_columns()
    schedule_frequency_counts = F.struct(
        *[F.coalesce(F.col(name), F.lit(0)).alias(name.removeprefix("schedule_frequency_")) for name in schedule_count_columns]
    )

    return (
        spark.read.table(HISTORY_TABLE)
        .where(F.col("__END_AT").isNull())
        .drop("__START_AT", "__END_AT")
        .withColumn("schedules", F.expr("parse_json(schedules_json)"))
        .withColumn("schedule_frequency_counts", schedule_frequency_counts)
        .withColumn("has_schedule", F.coalesce(F.col("schedule_count"), F.lit(0)) > 0)
        .withColumn("most_frequent_schedule_count", F.greatest(*[F.coalesce(F.col(name), F.lit(0)) for name in schedule_count_columns]))
        .withColumn("most_frequent_schedule", _most_frequent_schedule_expr())
    )


@dp.materialized_view(
    name=DENORMALIZED_TABLE,
    comment="Current dashboards enriched with 30-day system-table usage aggregates.",
    schema=DENORMALIZED_TABLE_SCHEMA,
)
def lakeview_dashboard_denormalized():
    audit_window_days = int(spark.conf.get("lakeview.audit_window_days", "30"))
    return spark.sql(
        f"""
        WITH dashboard_views AS (
          SELECT
            request_params.dashboard_id AS dashboard_id,
            struct(
              sum(CASE WHEN action_name = 'getPublishedDashboard' THEN 1 ELSE 0 END) AS published_l30d,
              count(DISTINCT CASE WHEN action_name = 'getPublishedDashboard' THEN user_identity.email END) AS published_users_l30d,
              sum(CASE WHEN action_name = 'getDashboard' THEN 1 ELSE 0 END) AS draft_l30d,
              count(DISTINCT CASE WHEN action_name = 'getDashboard' THEN user_identity.email END) AS draft_users_l30d
            ) AS views
          FROM system.access.audit
          WHERE event_date >= current_timestamp() - INTERVAL {audit_window_days} DAYS
            AND action_name IN ('getPublishedDashboard', 'getDashboard')
            AND request_params.dashboard_id IS NOT NULL
          GROUP BY request_params.dashboard_id
        ),
        query_history_l30d AS (
          SELECT
            query_source.dashboard_id AS dashboard_id,
            coalesce(sum(total_task_duration_ms), 0) AS total_task_duration_l30d,
            count(statement_id) AS count_queries_l30d
          FROM system.query.history
          WHERE start_time >= current_timestamp() - INTERVAL {audit_window_days} DAYS
            AND query_source.dashboard_id IS NOT NULL
          GROUP BY query_source.dashboard_id
        )
        SELECT
          dash.* EXCEPT (display_name),
          dash.display_name AS dashboard_name,
          coalesce(hist.total_task_duration_l30d, 0) AS total_task_duration_l30d,
          coalesce(hist.count_queries_l30d, 0) AS count_queries_l30d,
          dv.views AS views
        FROM {CURRENT_TABLE} dash
        LEFT JOIN query_history_l30d hist
          ON dash.dashboard_id = hist.dashboard_id
        LEFT JOIN dashboard_views dv
          ON dash.dashboard_id = dv.dashboard_id
        """
    )
