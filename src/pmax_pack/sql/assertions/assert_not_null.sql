WITH violations AS (
  SELECT 'campaign' AS family, COUNT(*) AS row_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date = @as_of
    AND (account_id IS NULL OR campaign_id IS NULL OR metric_basis IS NULL)
  UNION ALL
  SELECT 'asset_group', COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date = @as_of
    AND (account_id IS NULL OR campaign_id IS NULL OR asset_group_id IS NULL OR metric_basis IS NULL)
  UNION ALL
  SELECT 'asset', COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_asset_performance`
  WHERE date = @as_of
    AND (account_id IS NULL OR campaign_id IS NULL
      OR asset_group_id IS NULL OR asset_id IS NULL
      OR field_type IS NULL OR metric_basis IS NULL)
)
SELECT
  SUM(row_count) = 0 AS passed,
  SUM(row_count) AS observed,
  0 AS expected,
  'null required keys in performance marts' AS detail
FROM violations
