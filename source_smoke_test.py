from __future__ import annotations

import os
import sys

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F


THIS_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from dashboard_snapshot_source import SOURCE_NAME, register_lakeview_dashboard_snapshot_source


register_lakeview_dashboard_snapshot_source(spark)


def _sdk_host_token():
    host = spark.conf.get("lakeview.source.host", "") or os.getenv("LAKEVIEW_SOURCE_HOST", "")
    token = spark.conf.get("lakeview.source.token", "") or os.getenv("LAKEVIEW_SOURCE_TOKEN", "")
    profile = spark.conf.get("lakeview.source.profile", "") or os.getenv("DATABRICKS_CONFIG_PROFILE", "")
    auth_type = spark.conf.get("lakeview.source.auth_type", "") or os.getenv("DATABRICKS_AUTH_TYPE", "")

    if host and token:
        return host, token

    kwargs = {"config_file": os.getenv("DATABRICKS_CONFIG_FILE", "~/.databrickscfg")}
    if profile:
        kwargs["profile"] = profile
    if host:
        kwargs["host"] = host
    if auth_type:
        kwargs["auth_type"] = auth_type

    client = WorkspaceClient(**kwargs)
    try:
        oauth_token = client.config.oauth_token()
        if oauth_token.access_token:
            return client.config.host, oauth_token.access_token
    except Exception:
        pass

    headers = client.config.authenticate()
    auth_header = headers.get("Authorization") or headers.get("authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise ValueError("Could not resolve a bearer token from WorkspaceClient configuration")
    return client.config.host, auth_header.split(" ", 1)[1]


driver_host, driver_token = _sdk_host_token()

dashboard_limit = spark.conf.get("lakeview.source.dashboard_limit", "200")
parallelism = spark.conf.get("lakeview.source.parallelism", "2")
per_partition_threads = spark.conf.get("lakeview.source.per_partition_threads", "1")
output_batch_size = spark.conf.get("lakeview.source.output_batch_size", "64")
include_serialized_dashboard = spark.conf.get("lakeview.source.include_serialized_dashboard", "true")
use_driver_auth = spark.conf.get("lakeview.source.use_driver_auth", "true")

df = (
    spark.read.format(SOURCE_NAME)
    .option("dashboard_limit", dashboard_limit)
    .option("parallelism", parallelism)
    .option("per_partition_threads", per_partition_threads)
    .option("output_batch_size", output_batch_size)
    .option("include_serialized_dashboard", include_serialized_dashboard)
    .option("use_driver_auth", use_driver_auth)
)

if driver_host and driver_token:
    df = df.option("host", driver_host).option("token", driver_token)

df = df.load()

summary = (
    df.agg(
        F.count("*").alias("dashboard_count"),
        F.sum(F.length(F.coalesce(F.col("serialized_dashboard"), F.lit("")))).alias("serialized_dashboard_bytes"),
        F.avg(F.length(F.coalesce(F.col("serialized_dashboard"), F.lit("")))).alias("avg_serialized_dashboard_bytes"),
        F.sum(F.coalesce(F.col("schedule_count"), F.lit(0))).alias("schedule_count"),
    )
    .collect()[0]
)

print("Lakeview dashboard source smoke test")
print(f"dashboard_limit={dashboard_limit}")
print(f"parallelism={parallelism}")
print(f"per_partition_threads={per_partition_threads}")
print(f"dashboards={summary['dashboard_count']}")
print(f"serialized_dashboard_bytes={summary['serialized_dashboard_bytes']}")
print(f"avg_serialized_dashboard_bytes={summary['avg_serialized_dashboard_bytes']}")
print(f"schedules={summary['schedule_count']}")

df.select(
    "dashboard_id",
    "display_name",
    "owner",
    "schedule_count",
    "serialized_dashboard_sha256",
).orderBy("dashboard_id").show(20, truncate=False)
