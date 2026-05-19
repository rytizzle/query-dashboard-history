-- Cost attribution benchmarks
--
-- After the pipeline runs, execute these against the deploy catalog/schema
-- (e.g., ryant_catalog.system_tables_dev) to validate the cost attribution
-- output is sane. Each check has an inline assertion comment.
--
-- Usage:
--   databricks sql -e "USE CATALOG <catalog>; USE SCHEMA <schema>;"
--   then run these statements interactively or in a notebook.
--
-- Replace <catalog>.<schema> below with your deploy target.

-- =============================================================================
-- 1. Row count sanity
-- =============================================================================
-- Expect: > 0 if any SQL warehouse queries ran in the last cost.window_days.
SELECT count(*) AS statements_attributed
FROM query_cost_attribution;

-- =============================================================================
-- 2. Total attributed cost should approximately match raw warehouse spend
-- =============================================================================
-- Compares SUM(attributed_cost_usd) from the MV against SUM from raw
-- system.billing.usage joined with list_prices for the same window.
-- These should be within ~5% (rounding + hour-boundary edge cases).
WITH mv_total AS (
  SELECT sum(attributed_cost_usd) AS mv_total_usd
  FROM query_cost_attribution
),
raw_total AS (
  SELECT sum(u.usage_quantity * coalesce(lp.pricing.default, 0)) AS raw_total_usd
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud
   AND u.sku_name = lp.sku_name
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE u.billing_origin_product = 'SQL'
    AND u.usage_metadata.warehouse_id IS NOT NULL
    AND u.usage_start_time >= current_timestamp() - INTERVAL 30 DAYS
)
SELECT
  mv_total_usd,
  raw_total_usd,
  abs(mv_total_usd - raw_total_usd) / nullif(raw_total_usd, 0) AS variance_pct
FROM mv_total CROSS JOIN raw_total;
-- Note: variance > 5% may indicate warehouse idle time (no queries) is
-- being excluded from attribution by design — the doc states attribution
-- only covers warehouse uptime where >= 1 query is running.

-- =============================================================================
-- 3. No statement should have a negative or NULL attributed cost
-- =============================================================================
-- Expect: 0 rows.
SELECT count(*) AS bad_rows
FROM query_cost_attribution
WHERE attributed_cost_usd < 0 OR attributed_cost_usd IS NULL;

-- =============================================================================
-- 4. Statement-level cost should never exceed hourly bucket cost
-- =============================================================================
-- Expect: 0 rows. A single statement's attributed cost in any hour cannot
-- exceed the entire hourly warehouse cost for that warehouse-hour.
WITH per_hour AS (
  SELECT
    workspace_id, warehouse_id, date_trunc('HOUR', start_time) AS hour_start,
    sum(attributed_cost_usd) AS hour_attributed
  FROM query_cost_attribution
  GROUP BY 1, 2, 3
),
raw_hour AS (
  SELECT
    workspace_id,
    usage_metadata.warehouse_id AS warehouse_id,
    date_trunc('HOUR', usage_start_time) AS hour_start,
    sum(usage_quantity * coalesce(lp.pricing.default, 0)) AS raw_hour_cost
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices lp
    ON u.cloud = lp.cloud
   AND u.sku_name = lp.sku_name
   AND u.usage_start_time >= lp.price_start_time
   AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
  WHERE billing_origin_product = 'SQL'
    AND usage_metadata.warehouse_id IS NOT NULL
    AND usage_start_time >= current_timestamp() - INTERVAL 30 DAYS
  GROUP BY 1, 2, 3
)
SELECT count(*) AS hours_overattributed
FROM per_hour p
JOIN raw_hour r USING (workspace_id, warehouse_id, hour_start)
WHERE p.hour_attributed > r.raw_hour_cost * 1.01;  -- 1% tolerance

-- =============================================================================
-- 5. Top 10 most expensive dashboards (last cost.window_days)
-- =============================================================================
-- Smoke-test the dashboard rollup.
SELECT dashboard_name, owner, statements, cost_usd, dbus
FROM lakeview_dashboard_cost_l30d
WHERE cost_usd > 0
ORDER BY cost_usd DESC
LIMIT 10;

-- =============================================================================
-- 6. Top 10 most expensive Genie spaces
-- =============================================================================
SELECT genie_space_title, creator_id, statements, cost_usd, unique_users
FROM genie_space_cost_l30d
WHERE cost_usd > 0
ORDER BY cost_usd DESC
LIMIT 10;

-- =============================================================================
-- 7. Cost coverage by query source
-- =============================================================================
-- Breakdown of attributed cost by source type. Helps confirm dashboards
-- and Genie spaces actually account for a meaningful share.
SELECT
  CASE
    WHEN dashboard_id IS NOT NULL  THEN 'dashboard'
    WHEN genie_space_id IS NOT NULL THEN 'genie_space'
    WHEN notebook_id IS NOT NULL   THEN 'notebook'
    WHEN alert_id IS NOT NULL      THEN 'alert'
    WHEN job_id IS NOT NULL        THEN 'job'
    ELSE 'other / direct'
  END AS source_type,
  count(*) AS statements,
  round(sum(attributed_cost_usd), 2) AS cost_usd,
  round(100.0 * sum(attributed_cost_usd) / sum(sum(attributed_cost_usd)) OVER (), 2) AS pct_of_total
FROM query_cost_attribution
GROUP BY 1
ORDER BY cost_usd DESC;

-- =============================================================================
-- 8. Statements with no warehouse-hour billing match
-- =============================================================================
-- These are statements where the billing window didn't cover the query
-- (latency, or query ran outside the cost.window_days bound).
-- A small count is normal; a large count signals a config issue.
SELECT
  count(*) AS unbilled_statements,
  round(100.0 * count(*) / (SELECT count(*) FROM query_cost_attribution), 2) AS unbilled_pct
FROM query_cost_attribution
WHERE attributed_cost_usd = 0 OR attributed_cost_usd IS NULL;
