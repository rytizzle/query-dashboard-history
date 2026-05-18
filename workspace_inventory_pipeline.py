from __future__ import annotations

import atexit
import os
import threading
from datetime import datetime, timezone
from typing import Any

from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Load and register all snapshot sources
# ---------------------------------------------------------------------------
THIS_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
for source_file in ("dashboard_snapshot_source.py", "genie_space_snapshot_source.py", "warehouse_snapshot_source.py"):
    with open(os.path.join(THIS_DIR, source_file), "r", encoding="utf-8") as _f:
        exec(_f.read(), globals())


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DASHBOARD_HISTORY_TABLE = "lakeview_dashboard_history"
DASHBOARD_CURRENT_TABLE = "lakeview_dashboard_current"
DASHBOARD_DENORMALIZED_TABLE = "lakeview_dashboard_denormalized"
GENIE_HISTORY_TABLE = "genie_space_history"
GENIE_CURRENT_TABLE = "genie_space_current"
WAREHOUSE_HISTORY_TABLE = "warehouse_history"
WAREHOUSE_CURRENT_TABLE = "warehouse_current"

_EPHEMERAL_SOURCE_TOKEN_LOCK = threading.Lock()
_EPHEMERAL_SOURCE_TOKEN: tuple[str, str] | None = None
_EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS: int | None = None
_SOURCE_SNAPSHOT_VERSION = int(datetime.now(timezone.utc).timestamp() * 1000)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------
def _ddl(lines: tuple[str, ...]) -> str:
    return ",\n".join(lines)


def _progress_log(message: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{timestamp}] workspace-inventory: {message}", flush=True)


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


# ---------------------------------------------------------------------------
# Auth: ephemeral token minting (reused from original)
# ---------------------------------------------------------------------------
def _build_pipeline_token_client(options: dict[str, Any]):
    from databricks.sdk import WorkspaceClient
    kwargs = {
        "product": "workspace-inventory",
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
            comment=f"workspace-inventory:{datetime.now(timezone.utc).isoformat()}",
            lifetime_seconds=lifetime_seconds,
        )
        if not created.token_value:
            raise ValueError("Token API returned no token_value")

        token_info = created.token_info
        token_id = token_info.token_id if token_info else None
        expiry_time = token_info.expiry_time if token_info else None
        _EPHEMERAL_SOURCE_TOKEN = (client.config.host, created.token_value)
        _EPHEMERAL_SOURCE_TOKEN_EXPIRY_MS = expiry_time or (now_ms + lifetime_seconds * 1000)

        _progress_log(f"minted ephemeral workspace token")
        if token_id:
            _register_ephemeral_token_cleanup(client, token_id)
        return _EPHEMERAL_SOURCE_TOKEN


# Register all custom data sources
register_lakeview_dashboard_snapshot_source(spark)
register_genie_space_snapshot_source(spark)
register_warehouse_snapshot_source(spark)


def _source_options() -> dict[str, str]:
    options = {
        "host": spark.conf.get("source.host", ""),
        "token": spark.conf.get("source.token", ""),
        "profile": spark.conf.get("source.profile", ""),
        "auth_type": spark.conf.get("source.auth_type", ""),
        "mint_ephemeral_token": spark.conf.get("source.mint_ephemeral_token", "true"),
        "ephemeral_token_lifetime_seconds": spark.conf.get("source.ephemeral_token_lifetime_seconds", "3600"),
        "use_driver_auth": spark.conf.get("source.use_driver_auth", "true"),
    }
    if not (options["host"] and options["token"]):
        ephemeral_token = _get_ephemeral_source_token(options)
        if ephemeral_token:
            options["host"], options["token"] = ephemeral_token
    return {key: value for key, value in options.items() if value not in (None, "")}


def _load_source(source_name: str, extra_options: dict[str, str] | None = None):
    reader = spark.read.format(source_name)
    for key, value in _source_options().items():
        reader = reader.option(key, value)
    if extra_options:
        for key, value in extra_options.items():
            reader = reader.option(key, value)
    return reader.load()


# =====================================================================
# DASHBOARDS
# =====================================================================
DASHBOARD_HISTORY_SCHEMA = _ddl((
    "dashboard_id STRING COMMENT 'Stable Lakeview dashboard identifier'",
    "create_time TIMESTAMP COMMENT 'Time the dashboard was created'",
    "display_name STRING COMMENT 'Dashboard display name'",
    "etag STRING COMMENT 'Optimistic concurrency token'",
    "lifecycle_state STRING COMMENT 'Lifecycle state'",
    "parent_path STRING COMMENT 'Workspace parent path'",
    "path STRING COMMENT 'Workspace path'",
    "warehouse_id STRING COMMENT 'Warehouse configured for the dashboard'",
    "owner STRING COMMENT 'Owner email inferred from workspace path'",
    "dashboard_metadata_json STRING COMMENT 'Canonical JSON for dashboard metadata'",
    "dashboard_metadata_json_sha256 STRING COMMENT 'SHA-256 of metadata JSON'",
    "serialized_dashboard STRING COMMENT 'Serialized dashboard definition'",
    "serialized_dashboard_sha256 STRING COMMENT 'SHA-256 of serialized dashboard'",
    "update_time TIMESTAMP COMMENT 'Last update time'",
    "schedules_json STRING COMMENT 'Canonical JSON array of schedules'",
    "schedules_json_sha256 STRING COMMENT 'SHA-256 of schedules JSON'",
    "schedule_count INT COMMENT 'Number of schedules'",
    "schedule_frequency_minutely INT", "schedule_frequency_hourly INT",
    "schedule_frequency_daily INT", "schedule_frequency_weekly INT",
    "schedule_frequency_monthly INT", "schedule_frequency_yearly INT",
    "schedule_frequency_custom INT", "schedule_frequency_unknown INT",
    "__START_AT BIGINT", "__END_AT BIGINT",
))

DASHBOARD_TRACK_COLUMNS = (
    "create_time", "display_name", "etag", "lifecycle_state", "parent_path", "path",
    "warehouse_id", "owner", "dashboard_metadata_json_sha256", "serialized_dashboard_sha256",
    "update_time", "schedules_json_sha256", "schedule_count",
    "schedule_frequency_minutely", "schedule_frequency_hourly", "schedule_frequency_daily",
    "schedule_frequency_weekly", "schedule_frequency_monthly", "schedule_frequency_yearly",
    "schedule_frequency_custom", "schedule_frequency_unknown",
)


def _next_dashboard_snapshot(latest_snapshot_version: Any):
    if latest_snapshot_version is not None and int(latest_snapshot_version) >= _SOURCE_SNAPSHOT_VERSION:
        return None
    extra = {
        "page_size": spark.conf.get("dashboard.source.page_size", "1000"),
        "dashboard_limit": spark.conf.get("dashboard.source.dashboard_limit", "0"),
        "parallelism": spark.conf.get("dashboard.source.parallelism", "32"),
        "per_partition_threads": spark.conf.get("dashboard.source.per_partition_threads", "2"),
        "output_batch_size": spark.conf.get("dashboard.source.output_batch_size", "128"),
        "include_serialized_dashboard": spark.conf.get("dashboard.source.include_serialized_dashboard", "true"),
    }
    return _load_source(SOURCE_NAME, extra), _SOURCE_SNAPSHOT_VERSION


dp.create_streaming_table(
    name=DASHBOARD_HISTORY_TABLE,
    comment="SCD Type 2 Lakeview dashboard history.",
    schema=DASHBOARD_HISTORY_SCHEMA,
)

dp.create_auto_cdc_from_snapshot_flow(
    target=DASHBOARD_HISTORY_TABLE,
    source=_next_dashboard_snapshot,
    keys=["dashboard_id"],
    stored_as_scd_type=2,
    track_history_column_list=list(DASHBOARD_TRACK_COLUMNS),
)


@dp.materialized_view(
    name=DASHBOARD_CURRENT_TABLE,
    comment="Current dashboard metadata from active SCD2 rows.",
)
def lakeview_dashboard_current():
    return (
        spark.read.table(DASHBOARD_HISTORY_TABLE)
        .where(F.col("__END_AT").isNull())
        .drop("__START_AT", "__END_AT")
    )


@dp.materialized_view(
    name=DASHBOARD_DENORMALIZED_TABLE,
    comment="Current dashboards enriched with 30-day system-table usage aggregates.",
)
def lakeview_dashboard_denormalized():
    audit_window_days = int(spark.conf.get("dashboard.audit_window_days", "30"))
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
          dash.*,
          coalesce(hist.total_task_duration_l30d, 0) AS total_task_duration_l30d,
          coalesce(hist.count_queries_l30d, 0) AS count_queries_l30d,
          dv.views AS views
        FROM {DASHBOARD_CURRENT_TABLE} dash
        LEFT JOIN query_history_l30d hist
          ON dash.dashboard_id = hist.dashboard_id
        LEFT JOIN dashboard_views dv
          ON dash.dashboard_id = dv.dashboard_id
        """
    )


# =====================================================================
# GENIE SPACES
# =====================================================================
GENIE_HISTORY_SCHEMA = _ddl((
    "space_id STRING COMMENT 'Genie space identifier'",
    "title STRING COMMENT 'Space title'",
    "description STRING COMMENT 'Space description'",
    "warehouse_id STRING COMMENT 'Warehouse configured for the space'",
    "creator_id STRING COMMENT 'Creator user ID'",
    "create_time TIMESTAMP COMMENT 'Time the space was created'",
    "update_time TIMESTAMP COMMENT 'Last update time'",
    "space_metadata_json STRING COMMENT 'Canonical JSON for space metadata'",
    "space_metadata_json_sha256 STRING COMMENT 'SHA-256 of metadata JSON'",
    "table_count INT COMMENT 'Number of tables in the space'",
    "instruction_count INT COMMENT 'Number of instructions'",
    "sample_question_count INT COMMENT 'Number of sample questions'",
    "curated_question_count INT COMMENT 'Number of curated questions'",
    "__START_AT BIGINT", "__END_AT BIGINT",
))

GENIE_TRACK_COLUMNS = (
    "title", "description", "warehouse_id", "creator_id", "create_time", "update_time",
    "space_metadata_json_sha256", "table_count", "instruction_count",
    "sample_question_count", "curated_question_count",
)


def _next_genie_snapshot(latest_snapshot_version: Any):
    if latest_snapshot_version is not None and int(latest_snapshot_version) >= _SOURCE_SNAPSHOT_VERSION:
        return None
    extra = {
        "space_limit": spark.conf.get("genie.source.space_limit", "0"),
        "parallelism": spark.conf.get("genie.source.parallelism", "4"),
        "output_batch_size": spark.conf.get("genie.source.output_batch_size", "64"),
    }
    return _load_source(GENIE_SOURCE_NAME, extra), _SOURCE_SNAPSHOT_VERSION


dp.create_streaming_table(
    name=GENIE_HISTORY_TABLE,
    comment="SCD Type 2 Genie space history.",
    schema=GENIE_HISTORY_SCHEMA,
)

dp.create_auto_cdc_from_snapshot_flow(
    target=GENIE_HISTORY_TABLE,
    source=_next_genie_snapshot,
    keys=["space_id"],
    stored_as_scd_type=2,
    track_history_column_list=list(GENIE_TRACK_COLUMNS),
)


@dp.materialized_view(
    name=GENIE_CURRENT_TABLE,
    comment="Current Genie space metadata from active SCD2 rows.",
)
def genie_space_current():
    return (
        spark.read.table(GENIE_HISTORY_TABLE)
        .where(F.col("__END_AT").isNull())
        .drop("__START_AT", "__END_AT")
    )


# =====================================================================
# WAREHOUSES
# =====================================================================
WAREHOUSE_HISTORY_SCHEMA = _ddl((
    "id STRING COMMENT 'Warehouse identifier'",
    "name STRING COMMENT 'Warehouse name'",
    "state STRING COMMENT 'Current state'",
    "warehouse_type STRING COMMENT 'Warehouse type'",
    "cluster_size STRING COMMENT 'Cluster size'",
    "min_num_clusters INT COMMENT 'Min clusters'",
    "max_num_clusters INT COMMENT 'Max clusters'",
    "auto_stop_mins INT COMMENT 'Auto stop minutes'",
    "num_clusters INT COMMENT 'Current cluster count'",
    "num_active_sessions INT COMMENT 'Active sessions'",
    "enable_photon STRING COMMENT 'Photon enabled'",
    "enable_serverless_compute STRING COMMENT 'Serverless enabled'",
    "spot_instance_policy STRING COMMENT 'Spot instance policy'",
    "channel STRING COMMENT 'Channel name'",
    "creator_name STRING COMMENT 'Creator name'",
    "warehouse_metadata_json STRING COMMENT 'Canonical JSON for warehouse metadata'",
    "warehouse_metadata_json_sha256 STRING COMMENT 'SHA-256 of metadata JSON'",
    "__START_AT BIGINT", "__END_AT BIGINT",
))

WAREHOUSE_TRACK_COLUMNS = (
    "name", "state", "warehouse_type", "cluster_size", "min_num_clusters", "max_num_clusters",
    "auto_stop_mins", "num_clusters", "num_active_sessions", "enable_photon",
    "enable_serverless_compute", "spot_instance_policy", "channel", "creator_name",
    "warehouse_metadata_json_sha256",
)


def _next_warehouse_snapshot(latest_snapshot_version: Any):
    if latest_snapshot_version is not None and int(latest_snapshot_version) >= _SOURCE_SNAPSHOT_VERSION:
        return None
    extra = {
        "warehouse_limit": spark.conf.get("warehouse.source.warehouse_limit", "0"),
        "output_batch_size": spark.conf.get("warehouse.source.output_batch_size", "64"),
    }
    return _load_source(WAREHOUSE_SOURCE_NAME, extra), _SOURCE_SNAPSHOT_VERSION


dp.create_streaming_table(
    name=WAREHOUSE_HISTORY_TABLE,
    comment="SCD Type 2 SQL warehouse history.",
    schema=WAREHOUSE_HISTORY_SCHEMA,
)

dp.create_auto_cdc_from_snapshot_flow(
    target=WAREHOUSE_HISTORY_TABLE,
    source=_next_warehouse_snapshot,
    keys=["id"],
    stored_as_scd_type=2,
    track_history_column_list=list(WAREHOUSE_TRACK_COLUMNS),
)


@dp.materialized_view(
    name=WAREHOUSE_CURRENT_TABLE,
    comment="Current warehouse metadata from active SCD2 rows.",
)
def warehouse_current():
    return (
        spark.read.table(WAREHOUSE_HISTORY_TABLE)
        .where(F.col("__END_AT").isNull())
        .drop("__START_AT", "__END_AT")
    )
