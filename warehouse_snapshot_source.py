from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


WAREHOUSE_SOURCE_NAME = "warehouse_snapshot_sdk"
WAREHOUSE_PRODUCT_NAME = "workspace-inventory"


WAREHOUSE_OUTPUT_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("warehouse_type", StringType(), True),
        StructField("cluster_size", StringType(), True),
        StructField("min_num_clusters", IntegerType(), True),
        StructField("max_num_clusters", IntegerType(), True),
        StructField("auto_stop_mins", IntegerType(), True),
        StructField("num_clusters", IntegerType(), True),
        StructField("num_active_sessions", IntegerType(), True),
        StructField("enable_photon", StringType(), True),
        StructField("enable_serverless_compute", StringType(), True),
        StructField("spot_instance_policy", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("creator_name", StringType(), True),
        StructField("warehouse_metadata_json", StringType(), True),
        StructField("warehouse_metadata_json_sha256", StringType(), True),
    ]
)


def _wh_progress_log(message: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{timestamp}] {WAREHOUSE_PRODUCT_NAME}/warehouse: {message}", flush=True)


def _wh_parse_int(value: Any, default: int, minimum: int = 0) -> int:
    if value in (None, ""):
        return default
    parsed = int(value)
    return max(minimum, parsed)


def _wh_to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _wh_to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_wh_to_jsonable(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def _wh_canonical_json(value: Any) -> str:
    return json.dumps(_wh_to_jsonable(value), sort_keys=True, separators=(",", ":"))


def _wh_safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _wh_safe_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, bool):
        return str(value).lower()
    if hasattr(value, "value"):
        return str(value.value)
    return str(value)


def _warehouse_row(wh_raw: dict[str, Any]) -> dict[str, Any]:
    wh_metadata = _wh_to_jsonable(wh_raw)
    wh_metadata_json = _wh_canonical_json(wh_metadata)

    channel = wh_raw.get("channel")
    channel_name = None
    if isinstance(channel, dict):
        channel_name = channel.get("name")
    elif channel is not None:
        channel_name = str(channel)

    return {
        "id": str(wh_raw.get("id", "")),
        "name": wh_raw.get("name"),
        "state": _wh_safe_str(wh_raw.get("state")),
        "warehouse_type": _wh_safe_str(wh_raw.get("warehouse_type")),
        "cluster_size": _wh_safe_str(wh_raw.get("cluster_size")),
        "min_num_clusters": _wh_safe_int(wh_raw.get("min_num_clusters")),
        "max_num_clusters": _wh_safe_int(wh_raw.get("max_num_clusters")),
        "auto_stop_mins": _wh_safe_int(wh_raw.get("auto_stop_mins")),
        "num_clusters": _wh_safe_int(wh_raw.get("num_clusters")),
        "num_active_sessions": _wh_safe_int(wh_raw.get("num_active_sessions")),
        "enable_photon": _wh_safe_str(wh_raw.get("enable_photon")),
        "enable_serverless_compute": _wh_safe_str(wh_raw.get("enable_serverless_compute")),
        "spot_instance_policy": _wh_safe_str(wh_raw.get("spot_instance_policy")),
        "channel": channel_name,
        "creator_name": wh_raw.get("creator_name"),
        "warehouse_metadata_json": wh_metadata_json,
        "warehouse_metadata_json_sha256": hashlib.sha256(wh_metadata_json.encode("utf-8")).hexdigest(),
    }


def _wh_yield_batches(rows: list[dict[str, Any]], output_batch_size: int):
    try:
        import pyarrow as pa

        arrow_schema = pa.schema(
            [
                pa.field("id", pa.string(), nullable=False),
                pa.field("name", pa.string(), nullable=True),
                pa.field("state", pa.string(), nullable=True),
                pa.field("warehouse_type", pa.string(), nullable=True),
                pa.field("cluster_size", pa.string(), nullable=True),
                pa.field("min_num_clusters", pa.int32(), nullable=True),
                pa.field("max_num_clusters", pa.int32(), nullable=True),
                pa.field("auto_stop_mins", pa.int32(), nullable=True),
                pa.field("num_clusters", pa.int32(), nullable=True),
                pa.field("num_active_sessions", pa.int32(), nullable=True),
                pa.field("enable_photon", pa.string(), nullable=True),
                pa.field("enable_serverless_compute", pa.string(), nullable=True),
                pa.field("spot_instance_policy", pa.string(), nullable=True),
                pa.field("channel", pa.string(), nullable=True),
                pa.field("creator_name", pa.string(), nullable=True),
                pa.field("warehouse_metadata_json", pa.string(), nullable=True),
                pa.field("warehouse_metadata_json_sha256", pa.string(), nullable=True),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=arrow_schema)
        yield from table.to_batches(max_chunksize=output_batch_size)
    except Exception:
        column_names = [field.name for field in WAREHOUSE_OUTPUT_SCHEMA.fields]
        for row in rows:
            yield tuple(row.get(name) for name in column_names)


@dataclass
class WarehousePartition(InputPartition):
    partition_id: int
    host: str | None = None
    token: str | None = None


class WarehouseSnapshotReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict[str, str]):
        self.schema = schema
        self.options = dict(options)

    def partitions(self):
        host = self.options.get("host", "")
        token = self.options.get("token", "")
        return [WarehousePartition(partition_id=0, host=host, token=token)]

    def read(self, partition: WarehousePartition):
        import requests

        headers = {"Authorization": f"Bearer {partition.token}"}
        warehouse_limit = _wh_parse_int(self.options.get("warehouse_limit"), default=0, minimum=0)
        output_batch_size = _wh_parse_int(self.options.get("output_batch_size"), default=64, minimum=1)

        resp = requests.get(f"{partition.host}/api/2.0/sql/warehouses", headers=headers)
        resp.raise_for_status()
        warehouses = resp.json().get("warehouses", [])

        if warehouse_limit:
            warehouses = warehouses[:warehouse_limit]

        _wh_progress_log(f"fetched {len(warehouses)} warehouses")

        rows = [_warehouse_row(wh) for wh in warehouses]
        yield from _wh_yield_batches(rows, output_batch_size)

        _wh_progress_log(f"complete; emitted={len(rows)}")


class WarehouseSnapshotSource(DataSource):
    @classmethod
    def name(cls):
        return WAREHOUSE_SOURCE_NAME

    def schema(self):
        return WAREHOUSE_OUTPUT_SCHEMA

    def reader(self, schema: StructType):
        return WarehouseSnapshotReader(schema, self.options)


_WH_REGISTERED = False


def register_warehouse_snapshot_source(spark):
    global _WH_REGISTERED
    if not _WH_REGISTERED:
        spark.dataSource.register(WarehouseSnapshotSource)
        _WH_REGISTERED = True
