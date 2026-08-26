SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  asset_group.id AS asset_group_id,
  asset.id AS asset_id,
  asset.name AS asset_name,
  asset.type AS asset_type,
  asset.orientation AS orientation,
  asset.text_asset.text AS text,
  asset.image_asset.full_size.url AS image_url,
  asset.image_asset.full_size.height_pixels AS image_height_pixels,
  asset.image_asset.full_size.width_pixels AS image_width_pixels,
  asset.youtube_video_asset.youtube_video_id AS video_id,
  asset.youtube_video_asset.youtube_video_title AS video_title
FROM asset_group_asset
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
