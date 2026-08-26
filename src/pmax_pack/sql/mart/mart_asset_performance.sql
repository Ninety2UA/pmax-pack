BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_asset_performance` WHERE date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_asset_performance`
SELECT date, account_id, campaign_id, asset_group_id, asset_id, metric_basis,
  ad_network_type, conversion_action_id, conversion_action_resource_name,
  conversion_action_name,
  network_impressions, network_clicks, network_cost, network_conversions,
  network_conversions_value, network_all_conversions,
  network_all_conversions_value, action_conversions,
  action_conversions_value, action_all_conversions,
  action_all_conversions_value, field_type, asset_status,
  asset_primary_status, asset_primary_status_reasons, asset_source,
  asset_name, asset_type, orientation, text, image_url,
  image_height_pixels, image_width_pixels, video_id, video_title,
  currency_code, time_zone, click_through_lookback_window_days,
  view_through_lookback_window_days, include_in_conversions_metric,
  conversion_action_type, attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset`
WHERE date = @as_of;
COMMIT TRANSACTION;
