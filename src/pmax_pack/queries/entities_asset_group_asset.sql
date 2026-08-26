SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  asset_group.id AS asset_group_id,
  asset.id AS asset_id,
  asset_group_asset.field_type AS field_type,
  asset_group_asset.status AS status,
  asset_group_asset.primary_status AS primary_status,
  asset_group_asset.primary_status_reasons AS primary_status_reasons,
  asset_group_asset.source AS source
FROM asset_group_asset
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
