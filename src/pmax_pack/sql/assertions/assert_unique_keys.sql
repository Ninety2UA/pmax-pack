WITH duplicate_groups AS (
  SELECT COUNT(*) AS row_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY date, account_id, campaign_id, metric_basis, ad_network_type,
    conversion_action_id, conversion_action_resource_name
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY date, account_id, campaign_id, asset_group_id, metric_basis,
    ad_network_type, conversion_action_id, conversion_action_resource_name
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY date, account_id, campaign_id, asset_group_id, asset_id,
    field_type, metric_basis, ad_network_type, conversion_action_id,
    conversion_action_resource_name
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_asset_performance`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY date, account_id, campaign_id, asset_group_id, asset_id,
    field_type, metric_basis, ad_network_type, conversion_action_id,
    conversion_action_resource_name
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY date, account_id, campaign_id, ad_network_type
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id,
    asset_id, field_type
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group_signal`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id,
    signal_resource_name
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign_asset`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id, asset_resource_name,
    field_type
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_conversion_action`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, conversion_action_id
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_customer`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_bp_campaign`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_bp_asset_group`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id
  HAVING COUNT(*) > 1
  UNION ALL
  SELECT COUNT(*)
  FROM `{{ project }}.{{ marts_dataset }}.mart_bp_extended`
  WHERE snapshot_date = @as_of
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id
  HAVING COUNT(*) > 1
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'duplicate published-mart grain groups' AS detail
FROM duplicate_groups
