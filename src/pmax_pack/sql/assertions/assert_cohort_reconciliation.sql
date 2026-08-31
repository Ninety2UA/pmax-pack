WITH
performance AS (
  SELECT
    'campaign' AS grain,
    date AS click_date,
    account_id,
    campaign_id,
    CAST(NULL AS INT64) AS asset_group_id,
    ad_network_type,
    'PRIMARY' AS metric_basis,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    SUM(network_conversions) AS conversions,
    SUM(network_conversions_value) AS conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, ad_network_type
  UNION ALL
  SELECT 'campaign', date, account_id, campaign_id, CAST(NULL AS INT64),
    ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS STRING),
    SUM(network_all_conversions), SUM(network_all_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, ad_network_type
  UNION ALL
  SELECT 'campaign', date, account_id, campaign_id, CAST(NULL AS INT64),
    ad_network_type, 'CONVERSION_ACTION', conversion_action_resource_name,
    SUM(action_conversions), SUM(action_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
  GROUP BY date, account_id, campaign_id, ad_network_type,
    conversion_action_resource_name
  UNION ALL
  SELECT 'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'PRIMARY', CAST(NULL AS STRING),
    SUM(network_conversions), SUM(network_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, asset_group_id, ad_network_type
  UNION ALL
  SELECT 'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS STRING),
    SUM(network_all_conversions), SUM(network_all_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, asset_group_id, ad_network_type
  UNION ALL
  SELECT 'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'CONVERSION_ACTION', conversion_action_resource_name,
    SUM(action_conversions), SUM(action_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
  GROUP BY date, account_id, campaign_id, asset_group_id, ad_network_type,
    conversion_action_resource_name
),
cohort AS (
  SELECT
    'campaign' AS grain,
    click_date,
    account_id,
    campaign_id,
    CAST(NULL AS INT64) AS asset_group_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    SUM(cohorted_conversions + unknown_lag_conversions) AS conversions,
    SUM(cohorted_value + unknown_lag_value) AS conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_campaign`
  WHERE click_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND is_window_rung
  GROUP BY click_date, account_id, campaign_id, ad_network_type,
    metric_basis, conversion_action_resource_name
  UNION ALL
  SELECT 'asset_group', click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_resource_name,
    SUM(cohorted_conversions + unknown_lag_conversions),
    SUM(cohorted_value + unknown_lag_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset_group`
  WHERE click_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND is_window_rung
  GROUP BY click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_resource_name
),
violations AS (
  SELECT 1 AS violation
  FROM performance AS p
  FULL OUTER JOIN cohort AS c
    ON c.grain = p.grain
    AND c.click_date = p.click_date
    AND c.account_id = p.account_id
    AND c.campaign_id = p.campaign_id
    AND c.asset_group_id IS NOT DISTINCT FROM p.asset_group_id
    AND c.ad_network_type IS NOT DISTINCT FROM p.ad_network_type
    AND c.metric_basis = p.metric_basis
    AND c.conversion_action_resource_name IS NOT DISTINCT FROM
      p.conversion_action_resource_name
  WHERE ABS(COALESCE(c.conversions, 0) - COALESCE(p.conversions, 0))
      > GREATEST(
        ABS(COALESCE(p.conversions, 0))
          * {{ tolerances.campaign_reconciliation }},
        0.000000001
      )
    OR ABS(COALESCE(c.conversions_value, 0) - COALESCE(p.conversions_value, 0))
      > GREATEST(
        ABS(COALESCE(p.conversions_value, 0))
          * {{ tolerances.campaign_reconciliation }},
        0.000000001
      )
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'window-rung cohort totals plus unknown lag must reconcile to performance marts' AS detail
FROM violations
