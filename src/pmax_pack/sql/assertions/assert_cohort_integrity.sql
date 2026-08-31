WITH
duplicate_groups AS (
  SELECT COUNT(*) AS row_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_campaign`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY click_date, account_id, campaign_id, ad_network_type,
    metric_basis, conversion_action_resource_name, cohort_day
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset_group`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_resource_name,
    cohort_day
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY click_date, account_id, campaign_id, asset_group_id, asset_id,
    field_type, ad_network_type, metric_basis,
    conversion_action_resource_name, cohort_day
  HAVING COUNT(*) > 1
),
null_keys AS (
  SELECT COUNT(*) AS row_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_campaign`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND (account_id IS NULL OR campaign_id IS NULL OR metric_basis IS NULL
      OR cohort_day IS NULL OR provenance IS NULL OR maturity IS NULL)
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset_group`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND (account_id IS NULL OR campaign_id IS NULL OR asset_group_id IS NULL
      OR metric_basis IS NULL OR cohort_day IS NULL OR provenance IS NULL
      OR maturity IS NULL)
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND (account_id IS NULL OR campaign_id IS NULL OR asset_group_id IS NULL
      OR asset_id IS NULL OR field_type IS NULL OR metric_basis IS NULL
      OR cohort_day IS NULL OR provenance IS NULL OR maturity IS NULL)
),
violations AS (
  SELECT row_count FROM duplicate_groups
  UNION ALL
  SELECT row_count FROM null_keys WHERE row_count > 0
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'cohort marts must have unique grains and non-null required keys' AS detail
FROM violations
