# Workspace Inventory (Dashboards · Genie Spaces · Warehouses)

End-to-end Databricks Asset Bundle that snapshots three workspace-level resource types into Unity Catalog with SCD2 history, plus a Genie space backed by `system.query.history` for ad-hoc analytics.

## What it builds

One Lakeflow Declarative Pipeline (`workspace_inventory`) that produces seven UC tables:

| Source | History (Streaming Table, SCD2) | Current (Materialized View) | Enriched |
|---|---|---|---|
| Lakeview dashboards | `lakeview_dashboard_history` | `lakeview_dashboard_current` | `lakeview_dashboard_denormalized` |
| Genie spaces | `genie_space_history` | `genie_space_current` | — |
| SQL warehouses | `warehouse_history` | `warehouse_current` | — |

The dashboard denormalized view joins 30-day usage aggregates from `system.access.audit` and `system.query.history`.

## Repo layout

```
.
├── databricks.yml                          # DAB bundle + pipeline config
├── workspace_inventory_pipeline.py         # Lakeflow pipeline (all 3 sources)
├── dashboard_snapshot_source.py            # PySpark Python Data Source — Lakeview dashboards
├── genie_space_snapshot_source.py          # PySpark Python Data Source — Genie spaces
├── warehouse_snapshot_source.py            # PySpark Python Data Source — SQL warehouses
└── genie_space/                            # Reference artifacts for the Query History Genie space
    ├── query_history_metric_view.sql       # UC metric view definition
    ├── query_history_space.json            # Genie space config
    ├── genie_space_api_export.json         # Full export from Genie API
    └── genie_space_getspace_response.json  # Raw GetSpace response snapshot
```

## How it works

Each source is a PySpark Python Data Source that calls the Databricks SDK:

- **Dashboards** — `list()` for IDs, `get()` for full metadata + serialized dashboard, `list_schedules()` for schedule state
- **Genie spaces** — `list()` for IDs, `get()` for full space definition, instructions, and data sources
- **Warehouses** — `list()` returns full config (no `get()` needed)

All three feed `create_auto_cdc_from_snapshot_flow` with `stored_as_scd_type=2`, versioned on hash columns + scalar metadata so JSON-payload changes still create new history rows without using the raw JSON as a comparison key.

Bounded Spark partition concurrency with per-thread SDK client reuse. Optional ephemeral PAT minting at pipeline-driver level for worker auth.

## Config knobs

Set in `databricks.yml` under `configuration:`.

### Shared

- `source.host`, `source.token`, `source.profile`, `source.auth_type`
- `source.mint_ephemeral_token` — default `true`, mints a short-lived PAT on the driver
- `source.ephemeral_token_lifetime_seconds` — default `3600`
- `source.use_driver_auth` — default `true`

### Dashboards

- `dashboard.source.page_size` — default `1000`
- `dashboard.source.dashboard_limit` — `0` = no cap (use small value in dev)
- `dashboard.source.parallelism` — default `32` Spark partitions
- `dashboard.source.per_partition_threads` — default `2`
- `dashboard.source.output_batch_size` — default `128`
- `dashboard.source.include_serialized_dashboard` — default `true`
- `dashboard.audit_window_days` — default `30`

### Genie spaces

- `genie.source.space_limit` — `0` = no cap
- `genie.source.parallelism` — default `4`
- `genie.source.output_batch_size` — default `64`

### Warehouses

- `warehouse.source.warehouse_limit` — `0` = no cap
- `warehouse.source.output_batch_size` — default `64`

## Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev --profile <profile> --var catalog=<catalog> --var schema=<schema>
databricks bundle run workspace_inventory -t dev --profile <profile>
```

The `dev` target defaults to small limits (200 of each) for fast iteration. The `prod` target removes all limits.

## Query History Genie Space

Reference artifacts for a Genie space backed by a UC metric view over `system.query.history`. Use this on top of the pipeline outputs for natural-language analytics on performance, cost attribution, and capacity planning.

1. Edit `genie_space/query_history_metric_view.sql` to point at your catalog/schema and run it.
2. Create a Genie space in the workspace and point it at the metric view from step 1.
3. Import space config (instructions, benchmarks) using `query_history_space.json` via the Genie API or copy values manually.

## Possible improvements

- Pull dashboard, space, and warehouse permissions into the model so downstream consumers can build self-service datasets with row filters or column masks. LOE: medium, ~3-5 days.
- Add incremental loading using `system.access.audit` to scope the refresh set per run. LOE: high, ~1-2 weeks.
- Add dashboard subscriptions to extend the metadata beyond schedules and contents into delivery/recipient state. LOE: medium, ~2-4 days.
- Add account-level scraping with an account-level SP so one pipeline can inventory many workspaces. LOE: high, ~1-2 weeks.
- Wire the Genie space and metric view as DAB resources so the entire stack deploys in one `databricks bundle deploy`. LOE: low, ~1 day.
