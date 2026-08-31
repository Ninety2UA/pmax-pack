-- R19 required set: performance, lag, and cohort at campaign and asset-group
-- grain. Entity marts are not in this empty-table contract because some are
-- legitimately empty; customer timezone completeness is enforced at observe.
WITH
account_state AS (
  SELECT
    COUNT(DISTINCT c.account_id) AS account_count,
    COUNT(DISTINCT IF(f.first_snapshot_date = @as_of, c.account_id, NULL))
      AS first_run_account_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_customer` AS c
  LEFT JOIN `{{ project }}.{{ ops_dataset }}.first_snapshot` AS f
    ON f.account_id = c.account_id
  WHERE c.snapshot_date = @as_of
),
campaign_state AS (
  SELECT MIN(first_seen_date) AS earliest_campaign_start
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign`
  WHERE snapshot_date = @as_of
),
counts AS (
  SELECT 'mart_performance_campaign' AS table_name, COUNT(*) AS row_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT 'mart_performance_asset_group', COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT 'int_lag_prefix_campaign', COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.int_lag_prefix_campaign`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT 'int_lag_prefix_asset_group', COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.int_lag_prefix_asset_group`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT 'mart_cohort_campaign', COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_campaign`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT 'mart_cohort_asset_group', COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset_group`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
),
violations AS (
  SELECT c.table_name
  FROM counts AS c
  CROSS JOIN account_state AS a
  CROSS JOIN campaign_state AS s
  WHERE c.row_count = 0
    AND NOT (
      a.account_count > 0
      AND a.first_run_account_count = a.account_count
      AND s.earliest_campaign_start >= DATE_SUB(
        @as_of, INTERVAL {{ window_days }} DAY
      )
    )
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  COALESCE(
    'empty required tables: ' || STRING_AGG(table_name, ', ' ORDER BY table_name),
    'all required tables are non-empty or carry the first-run downgrade'
  ) AS detail
FROM violations
