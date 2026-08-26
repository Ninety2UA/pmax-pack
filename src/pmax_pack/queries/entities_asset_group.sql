SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  asset_group.id AS asset_group_id,
  asset_group.name AS asset_group_name,
  asset_group.status AS status,
  asset_group.primary_status AS primary_status,
  asset_group.primary_status_reasons AS primary_status_reasons,
  asset_group.ad_strength AS ad_strength,
  asset_group.asset_coverage.ad_strength_action_items AS ad_strength_action_items,
  asset_group.final_urls AS final_urls
FROM asset_group
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
