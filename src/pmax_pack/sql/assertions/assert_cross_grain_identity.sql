WITH
campaign_cells AS (
  SELECT
    click_date,
    account_id,
    campaign_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    cohort_day,
    SUM(cohorted_conversions) AS conversions,
    SUM(cohorted_value) AS conversions_value,
    SUM(unknown_lag_conversions) AS unknown_conversions,
    SUM(unknown_lag_value) AS unknown_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_campaign`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
  GROUP BY click_date, account_id, campaign_id, ad_network_type,
    metric_basis, conversion_action_resource_name, cohort_day
),
asset_group_cells AS (
  SELECT
    click_date,
    account_id,
    campaign_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    cohort_day,
    SUM(cohorted_conversions) AS conversions,
    SUM(cohorted_value) AS conversions_value,
    SUM(unknown_lag_conversions) AS unknown_conversions,
    SUM(unknown_lag_value) AS unknown_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset_group`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
  GROUP BY click_date, account_id, campaign_id, ad_network_type,
    metric_basis, conversion_action_resource_name, cohort_day
),
violations AS (
  SELECT 1 AS violation
  FROM campaign_cells AS c
  FULL OUTER JOIN asset_group_cells AS a
    ON a.click_date = c.click_date
    AND a.account_id = c.account_id
    AND a.campaign_id = c.campaign_id
    AND a.ad_network_type IS NOT DISTINCT FROM c.ad_network_type
    AND a.metric_basis = c.metric_basis
    AND a.conversion_action_resource_name IS NOT DISTINCT FROM
      c.conversion_action_resource_name
    AND a.cohort_day = c.cohort_day
  WHERE ABS(COALESCE(a.conversions, 0) - COALESCE(c.conversions, 0))
      > {{ tolerances.cross_grain }}
    OR ABS(COALESCE(a.conversions_value, 0) - COALESCE(c.conversions_value, 0))
      > {{ tolerances.cross_grain }}
    OR ABS(
      COALESCE(a.unknown_conversions, 0)
      - COALESCE(c.unknown_conversions, 0)
    ) > {{ tolerances.cross_grain }}
    OR ABS(COALESCE(a.unknown_value, 0) - COALESCE(c.unknown_value, 0))
      > {{ tolerances.cross_grain }}
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'asset-group cohort cells must sum to campaign cohort cells at the shared grain' AS detail
FROM violations
