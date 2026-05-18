# Query & Dashboard History

End-to-end project for Lakeview dashboard and query-history observability on Databricks. Combines two parts:

1. **Dashboard History DAB** — a Lakeflow Declarative Pipeline that ingests Lakeview dashboard metadata via the Databricks SDK and maintains SCD2 history.
2. **Query History Genie Space** — a UC metric view + Genie space definition for natural-language analytics on `system.query.history`.

## Repo layout

```
.
├── databricks.yml                              # DAB bundle config
├── dashboard_snapshot_source.py                # PySpark Python Data Source
├── lakeview_dashboard_history_pipeline.py      # DLT / Lakeflow pipeline
├── source_smoke_test.py                        # Standalone source validator
└── genie_space/
    ├── query_history_metric_view.sql           # UC metric view (deploy this first)
    ├── query_history_space.json                # Genie space definition
    ├── genie_space_api_export.json             # Full export from Genie API
    └── genie_space_getspace_response.json      # API GetSpace response snapshot
```

## Part 1 — Dashboard History DAB

Ingests Lakeview dashboard metadata through the Databricks SDK using the PySpark Python Data Source API, then uses Lakeflow Declarative Pipelines snapshot CDC to maintain:

- a history table with SCD Type 2 semantics
- a curated current table derived from active history rows with parsed schedule metadata
- a denormalized table joined to 30-day system-table aggregates

### Source path

- one dashboard listing pass to enumerate IDs
- one dashboard `get()` per dashboard to retain full dashboard history
- one `list_schedules()` per dashboard to retain current schedule state
- bounded Spark partition concurrency with per-thread SDK client reuse

The standalone smoke test and the DLT pipeline use the same datasource implementation in `dashboard_snapshot_source.py`.

### Default output tables

- `lakeview_dashboard_history`
- `lakeview_dashboard_current`
- `lakeview_dashboard_denormalized`

### Config knobs

- `lakeview.source.page_size`: dashboard listing page size
- `lakeview.source.parallelism`: max Spark partitions used for SDK reads
- `lakeview.source.per_partition_threads`: per-partition SDK concurrency
- `lakeview.source.dashboard_limit`: optional cap for smoke tests
- `lakeview.source.use_driver_auth`: passes a resolved bearer token from the driver to workers when available

### Notes

- `list()` does not return enough metadata to safely skip unchanged dashboards while preserving `serialized_dashboard` history.
- `get()` is required for exact dashboard-content history.
- `list_schedules()` is required for exact current schedule state.
- `track_history_column_list` is configured to version on the hash columns and scalar metadata so large JSON payload changes still create new SCD2 versions without using the raw JSON columns as the comparison keys.

### Suggested rollout

1. Run `source_smoke_test.py` with a small `dashboard_limit`.
2. Validate control-plane behavior before raising `lakeview.source.parallelism`.
3. Deploy the DLT pipeline with your own workspace target and catalog/schema overrides.

### Deploy

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev --profile <profile> --var catalog=<catalog> --var schema=<schema>
databricks bundle run lakeview_dashboard_history -t dev --profile <profile>
```

### Bundle defaults

- Bundle targets are intentionally generic. `dev` and `prod` do not pin a workspace host or profile.
- Default source settings are conservative for publishing: `page_size=1000`, `parallelism=32`, `per_partition_threads=2`.

## Part 2 — Query History Genie Space

Reference artifacts for a Genie space backed by a UC metric view over `system.query.history`. Use this to provide natural-language analytics for performance tuning, cost attribution, and capacity planning.

### Files

- `genie_space/query_history_metric_view.sql` — `CREATE OR REPLACE VIEW ... WITH METRICS` defining dimensions and measures for query history. Edit the catalog/schema/source references before deploying.
- `genie_space/query_history_space.json` — Genie space configuration (data sources, instructions, benchmarks).
- `genie_space/genie_space_api_export.json` — Full export from the Genie API; useful as the canonical reference.
- `genie_space/genie_space_getspace_response.json` — Raw `GetSpace` API response snapshot.

### Deploy

1. **Metric view first.** Edit `query_history_metric_view.sql` to point at your target catalog/schema and source table, then run it.
2. **Create the Genie space** in your workspace and point it at the metric view from step 1.
3. **Import space config** (instructions, benchmarks, sample queries) using the `query_history_space.json` payload via the Genie API, or copy values manually.

## Possible improvements

- Pull dashboard permissions into the model and publish self-service datasets on top of that. Row filters, column masks, or dynamic views could make downstream access patterns simpler. Rough LOE: medium, about 3-5 days depending on how much downstream security modeling is required.
- Add incremental loading using `system.access.audit`. Scan audit events for the current workspace ID and identify dashboard edits, schedule changes, and other events that can narrow the refresh set. Rough LOE: high, about 1-2 weeks because the event coverage and correctness model need to be proven before changing the ingestion contract.
- Add dashboard subscriptions to the model. That would extend the metadata beyond schedules and dashboard contents into delivery/recipient state. Rough LOE: medium, about 2-4 days if the required subscription APIs are available and stable.
- Add account-level scraping using an account-level service principal. That would make it possible to inventory multiple workspaces from one control plane rather than deploying a separate workspace-local scrape everywhere. Rough LOE: high, about 1-2 weeks depending on auth model, workspace discovery, and how multi-workspace publishing should land in Unity Catalog.
- Wire the Genie space and metric view as DAB resources so the whole stack deploys in one `databricks bundle deploy`. Rough LOE: low, ~1 day.
