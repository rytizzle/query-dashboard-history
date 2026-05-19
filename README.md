# Workspace Inventory (Dashboards · Genie Spaces · Warehouses)

End-to-end Databricks Asset Bundle that snapshots three workspace-level resource types into Unity Catalog with SCD2 history, plus a Genie space backed by `system.query.history` for ad-hoc analytics.

## What it builds

One Lakeflow Declarative Pipeline (`workspace_inventory`) that produces the following UC tables:

| Source | History (Streaming Table, SCD2) | Current (Materialized View) | Enriched |
|---|---|---|---|
| Lakeview dashboards | `lakeview_dashboard_history` | `lakeview_dashboard_current` | `lakeview_dashboard_denormalized` |
| Genie spaces | `genie_space_history` | `genie_space_current` | — |
| SQL warehouses | `warehouse_history` | `warehouse_current` | — |

Plus four cost-attribution materialized views built from `system.billing.usage`, `system.billing.account_prices` (with fallback to `system.billing.list_prices`), and `system.query.history`:

| Table | Grain | Purpose |
|---|---|---|
| `query_cost_attribution` | one row per statement | Per-statement attributed warehouse cost (USD + DBU) |
| `lakeview_dashboard_cost` | one row per dashboard | Per-dashboard cost rollup over `cost.window_days` |
| `genie_space_cost` | one row per Genie space | Per-Genie-space cost rollup over `cost.window_days` |
| `warehouse_cost` | one row per warehouse | Per-warehouse cost rollup over `cost.window_days` |

The dashboard denormalized view joins 30-day usage aggregates from `system.access.audit` and `system.query.history`.

## Cost attribution

Implements the algorithm from the Granular Cost Monitoring for Databricks SQL Private Preview:

1. Bucket warehouse spend by hour (matches `system.billing.usage` grain).
2. Per-statement work time = `compilation_duration_ms + execution_duration_ms + result_fetch_duration_ms`.
3. Split each statement's work_ms across the hourly buckets it spans, proportional to its wall-clock overlap with each hour.
4. Per warehouse-hour bucket, attribute the bucket's cost across statements by share of work_ms.
5. Roll the per-statement attributed cost back up to dashboards, Genie spaces, and warehouses.

Config knobs (in `databricks.yml`):

- `cost.window_days` — attribution lookback in days (default `365`). Both `system.query.history` and `system.billing.usage` retain ~365 days, so the default captures the full available history. Raise further only if you want to keep recomputing the same window.
- `cost.discount_pct` — flat discount applied on top of the resolved price (default `0`; e.g. `35` for 35% off). Use this only if your contract is a flat discount that isn't already reflected in `system.billing.account_prices`.

> **Going beyond source retention:** to keep per-object cost history past ~365 days (i.e. once `system.query.history` rows start ageing out), convert `query_cost_attribution` from a materialized view to a **streaming table** so each pipeline run appends new statements and your copy outlives the source. That's a small follow-up (decorator + checkpoint).

**Pricing source:** the MV prefers `system.billing.account_prices` (the customer's contracted rate) and falls back to `system.billing.list_prices` (public catalog rate) only for SKU/price-window rows the account table doesn't cover. Many demo / internal accounts have an empty `account_prices` — list_prices then carries the load. `cost.discount_pct` is an optional flat percentage applied on top (use it if your contract gives a flat discount that isn't reflected in `account_prices`).

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
├── scripts/
│   └── render_genie_space.py               # Substitute {{CATALOG}}/{{SCHEMA}}/{{MV_SCHEMA}} in templates
├── Makefile                                # Wraps render + bundle deploy/run
└── genie_space/                            # Templated Genie space artifacts
    ├── query_history_metric_view.sql       # UC metric view definition (templated)
    ├── query_history_space.json            # Genie space config + cost benchmarks (templated)
    ├── genie_space_api_export.json         # Full export from Genie API (templated)
    ├── genie_space_getspace_response.json  # Raw GetSpace response snapshot (templated)
    └── rendered/                           # Output of `make render` — gitignored, deploy-ready
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

The artifacts in `genie_space/` are **templates** with three placeholders:

| Placeholder | Meaning | Source |
|---|---|---|
| `{{CATALOG}}` | UC catalog containing pipeline outputs | `var.catalog` |
| `{{SCHEMA}}` | UC schema containing pipeline outputs | `var.schema` |
| `{{MV_SCHEMA}}` | Schema where `query_history_mv` lives | `var.metric_view_schema` |

Render them with your target catalog/schema before importing:

```bash
make render                                         # uses defaults from databricks.yml
make render CATALOG=acme SCHEMA=workspace_inv       # override
python3 scripts/render_genie_space.py --catalog acme --schema workspace_inv --mv-schema default
```

Rendered output lands in `genie_space/rendered/` (gitignored).

Then deploy:

1. Run the rendered `genie_space/rendered/query_history_metric_view.sql` to create the metric view.
2. Create a Genie space and point it at the metric view plus the four cost MVs (`query_cost_attribution`, `lakeview_dashboard_cost`, `genie_space_cost`, `warehouse_cost`).
3. Import space config from `genie_space/rendered/query_history_space.json` via the Genie API (or paste values manually). The benchmarks include 10 cost-attribution test questions ("which 10 dashboards cost the most", "which user runs the most expensive queries", etc.).

The DAB itself (`make deploy`) renders automatically before `databricks bundle deploy`.

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
