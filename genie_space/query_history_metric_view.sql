-- Metric View for Query History Analytics Genie Space
-- Source: system_utils.query_history.query_history_metrics
-- Deploy to: ryant_catalog.default.query_history_mv
-- Genie Space: 01f1225a0cff1b04a10de35374f97611

CREATE OR REPLACE VIEW ryant_catalog.default.query_history_mv
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Metric view for Query History Analytics - measures and dimensions for Genie and AI/BI dashboards"
source: system_utils.query_history.query_history_metrics
dimensions:
  # Time dimensions
  - name: Start Time
    expr: start_time
    comment: "Query start timestamp"
  - name: Query Date
    expr: query_date
    comment: "Query date - use for daily grouping"
  - name: Query Hour
    expr: query_hour
    comment: "Hour of day (0-23) - use for hourly patterns"
  - name: Day of Week
    expr: day_of_week
    comment: "Day of week (1=Sunday, 7=Saturday)"

  # Identity dimensions
  - name: Executed By
    expr: executed_by
    comment: "User who ran the query (email)"
  - name: Warehouse ID
    expr: warehouse_id
    comment: "SQL warehouse ID"
  - name: Account ID
    expr: account_id
    comment: "Databricks account ID"
  - name: Workspace ID
    expr: workspace_id
    comment: "Databricks workspace ID"

  # Query classification
  - name: Statement Type
    expr: statement_type
    comment: "Query type - SELECT, INSERT, MERGE, CREATE, etc."
  - name: Execution Status
    expr: execution_status
    comment: "FINISHED, FAILED, or CANCELED"
  - name: Compute Type
    expr: compute_type
    comment: "WAREHOUSE or SERVERLESS_COMPUTE"
  - name: Query Source Type
    expr: query_source_type
    comment: "Origin - Notebook, AI/BI Dashboard, Job, Genie Space, DLT Pipeline, etc."
  - name: Client Application
    expr: client_application
    comment: "Client tool - Tableau, Power BI, dbt, Databricks SQL Editor, etc."

  # Source entity IDs
  - name: Notebook ID
    expr: notebook_id
  - name: Dashboard ID
    expr: dashboard_id
  - name: Legacy Dashboard ID
    expr: legacy_dashboard_id
  - name: Job ID
    expr: job_id
  - name: Alert ID
    expr: alert_id
  - name: Genie Space ID
    expr: genie_space_id
  - name: Pipeline ID
    expr: pipeline_id
  - name: SQL Query ID
    expr: sql_query_id

  # Performance flags
  - name: Is Slow Query
    expr: is_slow_query
    comment: "Duration > 60 seconds"
  - name: Is Very Slow Query
    expr: is_very_slow_query
    comment: "Duration > 5 minutes"
  - name: Has Disk Spill
    expr: has_disk_spill
  - name: Is Cached
    expr: is_cached
  - name: Is Failed
    expr: is_failed
  - name: Had Long Wait
    expr: had_long_wait
    comment: "Waited > 30 seconds for compute or capacity"
  - name: Has Poor Scan Efficiency
    expr: has_poor_scan_efficiency
    comment: "Read 1000+ rows per row returned"

  # BI tool flags
  - name: Is Tableau
    expr: is_tableau
  - name: Is Power BI
    expr: is_power_bi
  - name: Is dbt
    expr: is_dbt

measures:
  # Volume
  - name: Total Queries
    expr: "COUNT(*)"
    comment: "Total number of queries"
  - name: Unique Users
    expr: "COUNT(DISTINCT executed_by)"
    comment: "Distinct users who ran queries"
  - name: Warehouses Used
    expr: "COUNT(DISTINCT warehouse_id)"
    comment: "Distinct warehouses used"

  # Duration (rounded)
  - name: Total Duration Sec
    expr: "SUM(duration_sec)"
    comment: "Total query duration in seconds"
  - name: Avg Duration Sec
    expr: "ROUND(AVG(duration_sec), 2)"
    comment: "Average query duration in seconds"
  - name: Max Duration Sec
    expr: "ROUND(MAX(duration_sec), 2)"
    comment: "Maximum query duration in seconds"
  - name: P50 Duration Sec
    expr: "ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_sec), 2)"
    comment: "Median query duration in seconds"
  - name: P95 Duration Sec
    expr: "ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_sec), 2)"
    comment: "95th percentile query duration in seconds"
  - name: P99 Duration Sec
    expr: "ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_sec), 2)"
    comment: "99th percentile query duration in seconds"

  # Wait time (rounded)
  - name: Total Compute Wait Sec
    expr: "ROUND(SUM(compute_wait_sec), 2)"
    comment: "Total time waiting for compute to start"
  - name: Total Capacity Wait Sec
    expr: "ROUND(SUM(capacity_wait_sec), 2)"
    comment: "Total time waiting at capacity"
  - name: Avg Wait Pct
    expr: "ROUND(AVG(wait_pct_of_total), 2)"
    comment: "Average wait time as pct of total duration"

  # Execution phases (rounded)
  - name: Avg Compilation Sec
    expr: "ROUND(AVG(compilation_sec), 2)"
  - name: Avg Execution Sec
    expr: "ROUND(AVG(execution_sec), 2)"
  - name: Avg Result Fetch Sec
    expr: "ROUND(AVG(result_fetch_sec), 2)"

  # Data volume (rounded)
  - name: Total Read GB
    expr: "ROUND(SUM(read_gb), 2)"
    comment: "Total data read in GB"
  - name: Total Read TB
    expr: "ROUND(SUM(read_tb), 4)"
    comment: "Total data read in TB"
  - name: Total Written GB
    expr: "ROUND(SUM(written_gb), 2)"
    comment: "Total data written in GB"
  - name: Total Shuffle GB
    expr: "ROUND(SUM(shuffle_gb), 2)"
    comment: "Total shuffle data in GB"
  - name: Total Spill GB
    expr: "ROUND(SUM(spill_gb), 2)"
    comment: "Total disk spill in GB"

  # Rows
  - name: Total Read Rows
    expr: "SUM(read_rows)"
  - name: Total Produced Rows
    expr: "SUM(produced_rows)"
  - name: Avg Scan to Output Ratio
    expr: "ROUND(AVG(scan_to_output_ratio), 2)"
    comment: "Average rows read per row returned - higher is less efficient"

  # Performance flag counts
  - name: Slow Query Count
    expr: "SUM(CASE WHEN is_slow_query THEN 1 ELSE 0 END)"
    comment: "Queries exceeding 60 seconds"
  - name: Very Slow Query Count
    expr: "SUM(CASE WHEN is_very_slow_query THEN 1 ELSE 0 END)"
    comment: "Queries exceeding 5 minutes"
  - name: Spill Query Count
    expr: "SUM(CASE WHEN has_disk_spill THEN 1 ELSE 0 END)"
  - name: Cached Query Count
    expr: "SUM(CASE WHEN is_cached THEN 1 ELSE 0 END)"
  - name: Failed Query Count
    expr: "SUM(CASE WHEN is_failed THEN 1 ELSE 0 END)"
  - name: Long Wait Query Count
    expr: "SUM(CASE WHEN had_long_wait THEN 1 ELSE 0 END)"
  - name: Poor Scan Query Count
    expr: "SUM(CASE WHEN has_poor_scan_efficiency THEN 1 ELSE 0 END)"

  # Rates (rounded)
  - name: Cache Hit Rate Pct
    expr: "ROUND(AVG(CASE WHEN is_cached THEN 1.0 ELSE 0.0 END) * 100, 2)"
    comment: "Percentage of queries served from cache"
  - name: Failure Rate Pct
    expr: "ROUND(AVG(CASE WHEN is_failed THEN 1.0 ELSE 0.0 END) * 100, 2)"
    comment: "Percentage of queries that failed"
  - name: Spill Rate Pct
    expr: "ROUND(AVG(CASE WHEN has_disk_spill THEN 1.0 ELSE 0.0 END) * 100, 2)"
    comment: "Percentage of queries that spilled to disk"
$$;
