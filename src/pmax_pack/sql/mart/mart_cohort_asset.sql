BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset`
WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_cohort_asset`
WITH costs AS (
  SELECT
    date AS click_date,
    account_id,
    campaign_id,
    asset_group_id,
    asset_id,
    field_type,
    ad_network_type,
    SUM(network_cost) AS click_day_cost
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, asset_group_id, asset_id,
    field_type, ad_network_type
)
SELECT
  c.click_date,
  c.account_id,
  c.campaign_id,
  c.asset_group_id,
  c.asset_id,
  c.field_type,
  c.ad_network_type,
  c.metric_basis,
  c.conversion_action_id,
  c.conversion_action_resource_name,
  c.conversion_action_name,
  c.cohort_day,
  c.is_window_rung,
  c.cohort_label,
  c.window_days,
  c.window_provenance,
  k.click_day_cost,
  c.cohorted_conversions,
  c.cohorted_value,
  c.unknown_lag_conversions,
  c.unknown_lag_value,
  c.provenance,
  c.unavailable_reason,
  c.maturity,
  c.observed_through,
  c.source_refresh_date,
  IF(k.click_day_cost IS NULL, 1, 0) AS missing_cost_cell_count,
  IF(c.maturity = 'immature', 1, 0) AS stale_cell_count,
  c.source_run_id,
  @run_id AS run_id
FROM `{{ project }}.{{ marts_dataset }}.int_observation_cells` AS c
LEFT JOIN costs AS k
  ON k.click_date = c.click_date
  AND k.account_id = c.account_id
  AND k.campaign_id = c.campaign_id
  AND k.asset_group_id = c.asset_group_id
  AND k.asset_id = c.asset_id
  AND k.field_type IS NOT DISTINCT FROM c.field_type
  AND k.ad_network_type IS NOT DISTINCT FROM c.ad_network_type
WHERE c.click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  AND c.grain = 'asset';
COMMIT TRANSACTION;
