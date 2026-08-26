WITH
network AS (
  SELECT date, account_id,
    SUM(network_conversions) AS conversions,
    SUM(network_conversions_value) AS conversions_value,
    SUM(network_all_conversions) AS all_conversions,
    SUM(network_all_conversions_value) AS all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date = @as_of AND metric_basis = 'NETWORK'
  GROUP BY date, account_id
),
action AS (
  SELECT date, account_id,
    SUM(action_conversions) AS conversions,
    SUM(action_conversions_value) AS conversions_value,
    SUM(action_all_conversions) AS all_conversions,
    SUM(action_all_conversions_value) AS all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date = @as_of AND metric_basis = 'CONVERSION_ACTION'
  GROUP BY date, account_id
),
metric_mismatches AS (
  SELECT
    COALESCE(n.date, a.date) AS date,
    COALESCE(n.account_id, a.account_id) AS account_id
  FROM network AS n
  FULL OUTER JOIN action AS a USING (date, account_id)
  WHERE ABS(COALESCE(n.conversions, 0) - COALESCE(a.conversions, 0))
      > {{ tolerances.campaign_reconciliation }}
    OR ABS(COALESCE(n.conversions_value, 0) - COALESCE(a.conversions_value, 0))
      > {{ tolerances.campaign_reconciliation }}
    OR ABS(COALESCE(n.all_conversions, 0) - COALESCE(a.all_conversions, 0))
      > {{ tolerances.campaign_reconciliation }}
    OR ABS(COALESCE(n.all_conversions_value, 0) - COALESCE(a.all_conversions_value, 0))
      > {{ tolerances.campaign_reconciliation }}
),
observed_tombstone AS (
  SELECT 'campaign' AS entity_type, account_id,
    CAST(campaign_id AS STRING) AS entity_key
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign`
  WHERE snapshot_date = @as_of
  GROUP BY account_id, campaign_id
  HAVING COUNTIF(inferred_removed) > 0 AND COUNTIF(NOT inferred_removed) > 0
  UNION ALL
  SELECT 'asset_group', account_id,
    CONCAT(CAST(campaign_id AS STRING), ':', CAST(asset_group_id AS STRING))
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group`
  WHERE snapshot_date = @as_of
  GROUP BY account_id, campaign_id, asset_group_id
  HAVING COUNTIF(inferred_removed) > 0 AND COUNTIF(NOT inferred_removed) > 0
  UNION ALL
  SELECT 'asset', account_id,
    CONCAT(CAST(campaign_id AS STRING), ':', CAST(asset_group_id AS STRING),
      ':', CAST(asset_id AS STRING), ':', field_type)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset`
  WHERE snapshot_date = @as_of
  GROUP BY account_id, campaign_id, asset_group_id, asset_id, field_type
  HAVING COUNTIF(inferred_removed) > 0 AND COUNTIF(NOT inferred_removed) > 0
  UNION ALL
  SELECT 'asset_group_signal', account_id,
    CONCAT(CAST(campaign_id AS STRING), ':', CAST(asset_group_id AS STRING),
      ':', signal_resource_name)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group_signal`
  WHERE snapshot_date = @as_of
  GROUP BY account_id, campaign_id, asset_group_id, signal_resource_name
  HAVING COUNTIF(inferred_removed) > 0 AND COUNTIF(NOT inferred_removed) > 0
  UNION ALL
  SELECT 'campaign_asset', account_id,
    CONCAT(CAST(campaign_id AS STRING), ':', asset_resource_name, ':', field_type)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign_asset`
  WHERE snapshot_date = @as_of
  GROUP BY account_id, campaign_id, asset_resource_name, field_type
  HAVING COUNTIF(inferred_removed) > 0 AND COUNTIF(NOT inferred_removed) > 0
  UNION ALL
  SELECT 'conversion_action', account_id,
    CAST(conversion_action_id AS STRING)
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_conversion_action`
  WHERE snapshot_date = @as_of
  GROUP BY account_id, conversion_action_id
  HAVING COUNTIF(inferred_removed) > 0 AND COUNTIF(NOT inferred_removed) > 0
),
violations AS (
  SELECT 'metric_mismatch' AS violation FROM metric_mismatches
  UNION ALL
  SELECT 'observed_tombstone' FROM observed_tombstone
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'action totals must match network totals and entities cannot be observed and removed together' AS detail
FROM violations
