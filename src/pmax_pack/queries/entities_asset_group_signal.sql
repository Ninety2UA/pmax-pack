SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  asset_group.id AS asset_group_id,
  asset_group_signal.resource_name AS signal_resource_name,
  asset_group_signal.approval_status AS approval_status,
  asset_group_signal.audience.audience AS audience,
  asset_group_signal.search_theme.text AS search_theme
FROM asset_group_signal
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
