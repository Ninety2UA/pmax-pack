SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  asset_group.id AS asset_group_id,
  asset.id AS asset_id,
  asset_group_asset.field_type AS field_type,
  segments.date AS date,
  segments.ad_network_type AS ad_network_type,
  segments.conversion_action AS conversion_action,
  segments.conversion_action_name AS conversion_action_name,
  metrics.conversions AS conversions,
  metrics.conversions_value AS conversions_value,
  metrics.all_conversions AS all_conversions,
  metrics.all_conversions_value AS all_conversions_value
FROM asset_group_asset
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
