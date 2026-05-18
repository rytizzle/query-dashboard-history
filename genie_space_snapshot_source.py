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


GENIE_SOURCE_NAME = "genie_space_snapshot_sdk"
GENIE_PRODUCT_NAME = "workspace-inventory"


GENIE_OUTPUT_SCHEMA = StructType(
    [
        StructField("space_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("creator_id", StringType(), True),
        StructField("create_time", TimestampType(), True),
        StructField("update_time", TimestampType(), True),
        StructField("space_metadata_json", StringType(), True),
        StructField("space_metadata_json_sha256", StringType(), True),
        StructField("table_count", IntegerType(), True),
        StructField("instruction_count", IntegerType(), True),
        StructField("sample_question_count", IntegerType(), True),
        StructField("curated_question_count", IntegerType(), True),
    ]
)


def _genie_progress_log(message: str):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{timestamp}] {GENIE_PRODUCT_NAME}/genie: {message}", flush=True)


def _genie_parse_int(value: Any, default: int, minimum: int = 0) -> int:
    if value in (None, ""):
        return default
    parsed = int(value)
    return max(minimum, parsed)


def _genie_parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _genie_coerce_timestamp(value: Any) -> datetime | None:
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


def _genie_json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return str(value)


def _genie_to_jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _genie_to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_genie_to_jsonable(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "value"):
        return value.value
    return value


def _genie_canonical_json(value: Any) -> str:
    return json.dumps(_genie_to_jsonable(value), sort_keys=True, separators=(",", ":"), default=_genie_json_default)


def _genie_space_row(space_raw: dict[str, Any]) -> dict[str, Any]:
    space_metadata = _genie_to_jsonable(space_raw)
    space_metadata_json = _genie_canonical_json(space_metadata)

    serialized_space = space_raw.get("serialized_space")
    parsed_space = {}
    if serialized_space and isinstance(serialized_space, str):
        try:
            parsed_space = json.loads(serialized_space)
        except (json.JSONDecodeError, ValueError):
            pass

    config = parsed_space.get("config", {})
    data_sources = parsed_space.get("data_sources", {})
    tables = data_sources.get("tables", [])
    instructions = parsed_space.get("instructions", {})
    instruction_count = len(instructions) if isinstance(instructions, (list, dict)) else 0

    sample_questions = config.get("sample_questions", [])
    curated_questions = config.get("curated_questions", [])

    return {
        "space_id": str(space_raw.get("space_id", "")),
        "title": space_raw.get("title"),
        "description": space_raw.get("description"),
        "warehouse_id": space_raw.get("warehouse_id"),
        "creator_id": str(space_raw.get("creator_id", "")) if space_raw.get("creator_id") else None,
        "create_time": _genie_coerce_timestamp(space_raw.get("create_time")),
        "update_time": _genie_coerce_timestamp(space_raw.get("update_time")),
        "space_metadata_json": space_metadata_json,
        "space_metadata_json_sha256": hashlib.sha256(space_metadata_json.encode("utf-8")).hexdigest(),
        "table_count": len(tables) if isinstance(tables, list) else 0,
        "instruction_count": instruction_count,
        "sample_question_count": len(sample_questions) if isinstance(sample_questions, list) else 0,
        "curated_question_count": len(curated_questions) if isinstance(curated_questions, list) else 0,
    }


def _genie_build_workspace_client(options: dict[str, Any]):
    import requests

    host = options.get("host", "")
    token = options.get("token", "")
    return host, token


def _genie_yield_batches(rows: list[dict[str, Any]], output_batch_size: int):
    try:
        import pyarrow as pa

        arrow_schema = pa.schema(
            [
                pa.field("space_id", pa.string(), nullable=False),
                pa.field("title", pa.string(), nullable=True),
                pa.field("description", pa.string(), nullable=True),
                pa.field("warehouse_id", pa.string(), nullable=True),
                pa.field("creator_id", pa.string(), nullable=True),
                pa.field("create_time", pa.timestamp("us"), nullable=True),
                pa.field("update_time", pa.timestamp("us"), nullable=True),
                pa.field("space_metadata_json", pa.string(), nullable=True),
                pa.field("space_metadata_json_sha256", pa.string(), nullable=True),
                pa.field("table_count", pa.int32(), nullable=True),
                pa.field("instruction_count", pa.int32(), nullable=True),
                pa.field("sample_question_count", pa.int32(), nullable=True),
                pa.field("curated_question_count", pa.int32(), nullable=True),
            ]
        )
        table = pa.Table.from_pylist(rows, schema=arrow_schema)
        yield from table.to_batches(max_chunksize=output_batch_size)
    except Exception:
        column_names = [field.name for field in GENIE_OUTPUT_SCHEMA.fields]
        for row in rows:
            yield tuple(row.get(name) for name in column_names)


@dataclass
class GenieSpaceIdPartition(InputPartition):
    partition_id: int
    space_ids: list[str]
    host: str | None = None
    token: str | None = None


class GenieSpaceSnapshotReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict[str, str]):
        self.schema = schema
        self.options = dict(options)

    def partitions(self):
        import requests

        host = self.options.get("host", "")
        token = self.options.get("token", "")
        headers = {"Authorization": f"Bearer {token}"}
        space_limit = _genie_parse_int(self.options.get("space_limit"), default=0, minimum=0)
        requested_parallelism = _genie_parse_int(self.options.get("parallelism"), default=4, minimum=1)

        space_ids = []
        page_token = None
        while True:
            params = {}
            if page_token:
                params["page_token"] = page_token
            resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
            for s in data.get("spaces", []):
                sid = s.get("space_id")
                if sid:
                    space_ids.append(str(sid))
                    if space_limit and len(space_ids) >= space_limit:
                        break
            page_token = data.get("next_page_token")
            if not page_token or (space_limit and len(space_ids) >= space_limit):
                break

        space_ids = sorted(set(space_ids))
        _genie_progress_log(f"completed genie space listing; count={len(space_ids)}")

        if not space_ids:
            return [GenieSpaceIdPartition(partition_id=0, space_ids=[], host=host, token=token)]

        parallelism = min(requested_parallelism, len(space_ids))
        chunk_size = math.ceil(len(space_ids) / parallelism)
        partitions = []
        for pid in range(parallelism):
            start = pid * chunk_size
            chunk = space_ids[start : start + chunk_size]
            if chunk:
                partitions.append(GenieSpaceIdPartition(partition_id=pid, space_ids=chunk, host=host, token=token))

        _genie_progress_log(f"partition planning complete; partition_count={len(partitions)}")
        return partitions

    def read(self, partition: GenieSpaceIdPartition):
        import requests
        import time

        headers = {"Authorization": f"Bearer {partition.token}"}
        output_batch_size = _genie_parse_int(self.options.get("output_batch_size"), default=64, minimum=1)
        pending_rows = []
        processed = 0
        emitted = 0

        for sid in partition.space_ids:
            processed += 1
            try:
                for attempt in range(5):
                    resp = requests.get(f"{partition.host}/api/2.0/genie/spaces/{sid}", headers=headers)
                    if resp.status_code == 429:
                        time.sleep(2 ** attempt)
                        continue
                    resp.raise_for_status()
                    break
                raw = resp.json()
                pending_rows.append(_genie_space_row(raw))
            except Exception as e:
                _genie_progress_log(f"partition {partition.partition_id} error fetching space {sid}: {e}")

            if len(pending_rows) >= output_batch_size:
                emitted += len(pending_rows)
                yield from _genie_yield_batches(pending_rows, output_batch_size)
                pending_rows.clear()

        if pending_rows:
            emitted += len(pending_rows)
            yield from _genie_yield_batches(pending_rows, output_batch_size)

        _genie_progress_log(f"partition {partition.partition_id} complete; processed={processed} emitted={emitted}")


class GenieSpaceSnapshotSource(DataSource):
    @classmethod
    def name(cls):
        return GENIE_SOURCE_NAME

    def schema(self):
        return GENIE_OUTPUT_SCHEMA

    def reader(self, schema: StructType):
        return GenieSpaceSnapshotReader(schema, self.options)


_GENIE_REGISTERED = False


def register_genie_space_snapshot_source(spark):
    global _GENIE_REGISTERED
    if not _GENIE_REGISTERED:
        spark.dataSource.register(GenieSpaceSnapshotSource)
        _GENIE_REGISTERED = True
