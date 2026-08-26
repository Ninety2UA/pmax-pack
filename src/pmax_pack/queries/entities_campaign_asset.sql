SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  campaign.advertising_channel_type AS advertising_channel_type,
  campaign_asset.asset AS asset_resource_name,
  asset.id AS asset_id,
  campaign_asset.field_type AS field_type,
  campaign_asset.status AS status,
  campaign_asset.primary_status AS primary_status,
  campaign_asset.primary_status_reasons AS primary_status_reasons
FROM campaign_asset
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
