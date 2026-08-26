SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  asset_group.id AS asset_group_id,
  asset_group.name AS asset_group_name,
  segments.date AS date,
  segments.ad_network_type AS ad_network_type,
  metrics.impressions AS impressions,
  metrics.clicks AS clicks,
  metrics.cost_micros AS cost_micros,
  metrics.conversions AS conversions,
  metrics.conversions_value AS conversions_value,
  metrics.all_conversions AS all_conversions,
  metrics.all_conversions_value AS all_conversions_value
FROM asset_group
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
  AND segments.date BETWEEN '{start_date}' AND '{end_date}'
