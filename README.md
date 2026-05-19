# Workspace Inventory

**A Databricks Asset Bundle that gives you a complete, governed picture of your workspace — every dashboard, Genie space, and SQL warehouse, with full history, usage, and attributed cost.**

If you've ever been asked *"how much did this dashboard cost last month?"*, *"which Genie spaces are people actually using?"*, or *"what changed on this warehouse three weeks ago?"* — this DAB is the answer.

## What you get

One Lakeflow Declarative Pipeline that snapshots your workspace into Unity Catalog and produces three layers of insight:

### 🗂️ Inventory (SCD2 history)

A daily snapshot of every dashboard, Genie space, and warehouse, captured with full Slowly-Changing-Dimension Type 2 history. You can ask *"what did this dashboard look like on March 1?"* and get a real answer — schedules, owners, configured warehouse, the whole shape.

| Source | History | Current | Enriched |
|---|---|---|---|
| Lakeview dashboards | `lakeview_dashboard_history` | `lakeview_dashboard_current` | `lakeview_dashboard_denormalized` |
| Genie spaces | `genie_space_history` | `genie_space_current` | — |
| SQL warehouses | `warehouse_history` | `warehouse_current` | — |

The dashboard *denormalized* view also joins 30-day usage from `system.access.audit` and `system.query.history`, so you can immediately see view counts, distinct viewers, and query volume per dashboard.

### 💰 Cost attribution

Warehouses bill by uptime, not by query — which means standard billing tables can't tell you which dashboard or Genie space drove the cost. These four views fix that by attributing every dollar of warehouse spend down to the statement that consumed it, and then up to the object that owns the statement.

| Table | Grain | What it tells you |
|---|---|---|
| `query_cost_attribution` | per statement | "This exact query cost $0.024 and burned 0.012 DBUs" |
| `lakeview_dashboard_cost` | per dashboard | "Dashboard X cost $412 over the window, run by 14 users" |
| `genie_space_cost` | per Genie space | "Genie space Y cost $87, was queried by 6 people" |
| `warehouse_cost` | per warehouse | "Warehouse Z carried 8 dashboards and 3 Genie spaces, $1.2k total" |

Implements the algorithm from the *Granular Cost Monitoring for Databricks SQL* Private Preview — see [Cost attribution](#cost-attribution) below for the math.

### 🧞 Conversational analytics (Query History Genie Space)

A pre-built Genie space, complete with a UC metric view over `system.query.history` and 24 calibrated benchmark questions ("which 10 dashboards cost the most?", "which user runs the slowest queries?", "show daily cost trend by source type"). Drop it in your workspace, point it at the pipeline outputs, and your team can interrogate cost and usage in plain English.

## Cost attribution — how the math works

DBSQL bills you for warehouse uptime; it doesn't ship a "cost per query" column. So we recover it by **distributing each warehouse-hour of spend across the queries that did work in that hour**, in proportion to how much of the hour each query consumed.

1. **Bucket warehouse spend by hour.** Matches the grain of `system.billing.usage`.
2. **Compute per-statement "work time"** as `compilation_duration_ms + execution_duration_ms + result_fetch_duration_ms`. (These three phases reflect actual compute consumption.)
3. **Split long queries across hour boundaries.** A query that runs from 8:45 to 9:30 contributes 15 min of work_ms to the 8 o'clock bucket and 30 min to the 9 o'clock bucket — proportional to wall-clock overlap.
4. **Attribute the bucket's cost.** Each statement's share of bucket cost = (its work_ms in the bucket) / (total work_ms in the bucket). Sum across all buckets the statement touched.
5. **Roll up.** Per-statement cost → per-dashboard, per-Genie-space, per-warehouse rollups.

This is an approximation — DBSQL doesn't actually charge per query — but it's the same approximation Databricks themselves ship in their Private Preview cost-monitoring tooling, and it's accurate to within a few percent at the warehouse-hour grain.

### Pricing source

The pipeline prefers your account's **contracted** rates (`system.billing.account_prices`) and falls back to the public **list** rates (`system.billing.list_prices`) only for SKU/price-window rows the account table doesn't cover. Many demo and internal accounts have an empty `account_prices` — list_prices then carries the load.

If your contract is a flat discount that isn't already reflected in `account_prices`, set `cost.discount_pct` (e.g. `35` for 35% off).

### Config knobs

In `databricks.yml`:

- `cost.window_days` — attribution lookback in days (default `365`). Both `system.query.history` and `system.billing.usage` retain ~365 days, so the default captures everything available.
- `cost.discount_pct` — flat discount applied on top of the resolved price (default `0`).

> **Going beyond source retention:** to keep per-object cost history past ~365 days (i.e. once `system.query.history` rows start ageing out), convert `query_cost_attribution` from a materialized view to a **streaming table** so each pipeline run appends new statements and your copy outlives the source. Small follow-up — decorator + checkpoint.

## Repo layout

```
.
├── databricks.yml                          # DAB bundle + pipeline config
├── workspace_inventory_pipeline.py         # Lakeflow pipeline (inventory + cost MVs)
├── dashboard_snapshot_source.py            # PySpark Data Source — Lakeview dashboards
├── genie_space_snapshot_source.py          # PySpark Data Source — Genie spaces
├── warehouse_snapshot_source.py            # PySpark Data Source — SQL warehouses
├── Makefile                                # Wraps render + bundle deploy/run
├── scripts/
│   └── render_genie_space.py               # Templating step for the Genie space artifacts
├── tests/
│   └── cost_attribution_benchmarks.sql     # Sanity checks for the cost MVs
└── genie_space/                            # Templated Genie space artifacts
    ├── query_history_metric_view.sql       # UC metric view definition (templated)
    ├── query_history_space.json            # Genie space config + cost benchmarks (templated)
    ├── genie_space_api_export.json         # Full export from Genie API (templated)
    ├── genie_space_getspace_response.json  # Raw GetSpace response snapshot (templated)
    └── rendered/                           # Output of `make render` — gitignored, deploy-ready
```

## How the snapshot sources work

Each of the three source types is a custom **PySpark Python Data Source** that wraps the Databricks SDK:

- **Dashboards** — `list()` enumerates IDs, `get()` pulls full metadata + serialized definition, `list_schedules()` captures schedule state.
- **Genie spaces** — `list()` for IDs, `get()` for the full space definition (instructions, data sources, sample questions, curated questions).
- **Warehouses** — `list()` returns the full config in one pass; no per-resource `get()` needed.

All three feed `create_auto_cdc_from_snapshot_flow` with `stored_as_scd_type=2`. The track-history column list is keyed on hash columns and scalar metadata, so JSON-payload changes still create new SCD2 versions without using the raw JSON as a comparison key.

Bounded Spark partition concurrency with per-thread SDK client reuse keeps the control plane happy. The pipeline driver optionally mints a short-lived PAT and hands it to workers, so you don't need to wire long-lived credentials anywhere.

## Config knobs

All knobs are set in `databricks.yml` under `configuration:`.

### Shared (auth)

- `source.host`, `source.token`, `source.profile`, `source.auth_type`
- `source.mint_ephemeral_token` — default `true`. Mints a short-lived PAT on the driver and hands it to workers.
- `source.ephemeral_token_lifetime_seconds` — default `3600`
- `source.use_driver_auth` — default `true`

### Dashboards

- `dashboard.source.page_size` — default `1000`
- `dashboard.source.dashboard_limit` — `0` = no cap (use a small value in dev for fast iteration)
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
make deploy TARGET=dev PROFILE=<your-profile>
# equivalent to:
#   make render
#   databricks bundle deploy -t dev --profile <your-profile>
```

To run on demand:

```bash
make run TARGET=dev PROFILE=<your-profile>
```

The `dev` target defaults to small source limits (200 of each resource type) so iteration is fast. The `prod` target lifts the caps entirely.

Targets are intentionally generic — neither `dev` nor `prod` pins a workspace host or profile, so the same bundle deploys cleanly to any environment.

## Query History Genie Space

Reference artifacts for a Genie space backed by a UC metric view over `system.query.history`, augmented with the pipeline's cost-attribution outputs. Drop it in any workspace and your team can ask cost, performance, and usage questions in plain English.

The artifacts in `genie_space/` are **templates** with three placeholders so they can be deployed into any catalog/schema:

| Placeholder | Meaning | Source |
|---|---|---|
| `{{CATALOG}}` | UC catalog containing the pipeline outputs | `var.catalog` |
| `{{SCHEMA}}` | UC schema containing the pipeline outputs | `var.schema` |
| `{{MV_SCHEMA}}` | Schema where `query_history_mv` lives | `var.metric_view_schema` |

Render with your target values before importing:

```bash
make render                                         # uses defaults from databricks.yml
make render CATALOG=acme SCHEMA=workspace_inv       # override
python3 scripts/render_genie_space.py --catalog acme --schema workspace_inv --mv-schema default
```

Rendered output lands in `genie_space/rendered/` (gitignored). `make deploy` runs `make render` automatically.

Then in the workspace:

1. Run the rendered `genie_space/rendered/query_history_metric_view.sql` to create the metric view.
2. Create a Genie space and point it at the metric view plus the four cost MVs.
3. Import space config from `genie_space/rendered/query_history_space.json` via the Genie API (or paste in manually).

The space ships with 24 benchmark questions — performance tuning, cost attribution by user/dashboard/Genie space, daily trends — so you can validate that the space answers correctly right out of the box.

## Testing

After the pipeline runs, validate the cost attribution output:

```bash
databricks sql-warehouses execute --warehouse-id <id> --file tests/cost_attribution_benchmarks.sql
```

Eight inline checks: row counts, total-cost vs raw-billing variance, hour-overattribution, no-negative-cost, top-cost rollups, source-type coverage, unbilled-statement detection.

## Possible improvements

- **Permissions.** Pull dashboard, space, and warehouse permissions into the model so downstream consumers can build self-service datasets with row filters or column masks. *LOE: medium, ~3-5 days.*
- **Incremental refresh.** Use `system.access.audit` to narrow the refresh set per run — only re-snapshot resources that actually changed. *LOE: high, ~1-2 weeks.*
- **Dashboard subscriptions.** Extend the metadata beyond schedules and contents into delivery/recipient state. *LOE: medium, ~2-4 days.*
- **Account-level scraping.** Inventory multiple workspaces from one place using an account-level SP. *LOE: high, ~1-2 weeks.*
- **Cost as a streaming table.** Convert `query_cost_attribution` from MV to streaming table so per-object cost history persists past `system.query.history` retention. *LOE: low, ~1 day.*
- **Bundle the Genie space as a DAB resource** so the whole stack — pipeline + metric view + Genie space — deploys in one `databricks bundle deploy`. *LOE: low, ~1 day.*
