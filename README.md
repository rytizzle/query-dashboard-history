# Workspace Inventory (Dashboards · Genie Spaces · Warehouses)

End-to-end Databricks Asset Bundle that snapshots three workspace-level resource types into Unity Catalog with SCD2 history, plus a Genie space backed by `system.query.history` for ad-hoc analytics.

## What it builds

One Lakeflow Declarative Pipeline (`workspace_inventory`) that produces the following UC tables:

| Source | History (Streaming Table, SCD2) | Current (Materialized View) | Enriched |
|---|---|---|---|
| Lakeview dashboards | `lakeview_dashboard_history` | `lakeview_dashboard_current` | `lakeview_dashboard_denormalized` |
| Genie spaces | `genie_space_history` | `genie_space_current` | — |
| SQL warehouses | `warehouse_history` | `warehouse_current` | — |

Plus four cost-attribution materialized views built from `system.billing.usage`, `system.billing.list_prices`, and `system.query.history`:

| Table | Grain | Purpose |
|---|---|---|
| `query_cost_attribution` | one row per statement | Per-statement attributed warehouse cost (USD + DBU) |
| `lakeview_dashboard_cost_l30d` | one row per dashboard | 30-day dashboard cost rollup |
| `genie_space_cost_l30d` | one row per Genie space | 30-day Genie space cost rollup |
| `warehouse_cost_l30d` | one row per warehouse | 30-day warehouse cost rollup |

The dashboard denormalized view joins 30-day usage aggregates from `system.access.audit` and `system.query.history`.

## Cost attribution

Implements the algorithm from the Granular Cost Monitoring for Databricks SQL Private Preview:

1. Bucket warehouse spend by hour (matches `system.billing.usage` grain).
2. Per-statement work time = `compilation_duration_ms + execution_duration_ms + result_fetch_duration_ms`.
3. Split each statement's work_ms across the hourly buckets it spans, proportional to its wall-clock overlap with each hour.
4. Per warehouse-hour bucket, attribute the bucket's cost across statements by share of work_ms.
5. Roll the per-statement attributed cost back up to dashboards, Genie spaces, and warehouses.

Config knobs (in `databricks.yml`):

- `cost.window_days` — attribution lookback in days (default `30`)
- `cost.discount_pct` — flat discount applied to list price (default `0`; e.g. `35` for 35% off)

The customer can plug in their effective $/DBU later; today we use `system.billing.list_prices.pricing.default` with the optional flat discount.

## Repo layout

```
.
├── databricks.yml                          # DAB bundle + pipeline config
├── workspace_inventory_pipeline.py         # Lakeflow pipeline (sources + cost MVs)
├── dashboard_snapshot_source.py            # PySpark Python Data Source — Lakeview dashboards
├── genie_space_snapshot_source.py          # PySpark Python Data Source — Genie spaces
├── warehouse_snapshot_source.py            # PySpark Python Data Source — SQL warehouses
├── tests/
│   └── cost_attribution_benchmarks.sql     # Sanity-check SQL for cost MV correctness
└── genie_space/                            # Reference artifacts for the Query History Genie space
    ├── query_history_metric_view.sql       # UC metric view definition
    ├── query_history_space.json            # Genie space config (now incl. cost benchmarks)
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

Reference artifacts for a Genie space backed by a UC metric view over `system.query.history`, augmented with the pipeline's cost-attribution outputs. Use this for natural-language analytics on performance, cost attribution, and capacity planning.

1. Edit `genie_space/query_history_metric_view.sql` to point at your catalog/schema and run it.
2. Create a Genie space in the workspace and point it at the metric view from step 1 plus the four cost MVs (`query_cost_attribution`, `lakeview_dashboard_cost_l30d`, `genie_space_cost_l30d`, `warehouse_cost_l30d`).
3. Import space config (instructions, benchmarks) using `query_history_space.json` via the Genie API or copy values manually. The benchmarks include 10 cost-attribution test questions ("which 10 dashboards cost the most", "which user runs the most expensive queries", etc.).

## Testing

After the pipeline runs, validate the cost attribution output:

```bash
databricks sql-warehouses execute --warehouse-id <id> --file tests/cost_attribution_benchmarks.sql
```

The benchmark file includes row-count, total-cost-vs-raw-billing, hour-overattribution, and source-coverage checks. Open it for inline assertions.

## Possible improvements

- Pull dashboard, space, and warehouse permissions into the model so downstream consumers can build self-service datasets with row filters or column masks. LOE: medium, ~3-5 days.
- Add incremental loading using `system.access.audit` to scope the refresh set per run. LOE: high, ~1-2 weeks.
- Add dashboard subscriptions to extend the metadata beyond schedules and contents into delivery/recipient state. LOE: medium, ~2-4 days.
- Add account-level scraping with an account-level SP so one pipeline can inventory many workspaces. LOE: high, ~1-2 weeks.
- Wire the Genie space and metric view as DAB resources so the entire stack deploys in one `databricks bundle deploy`. LOE: low, ~1 day.
