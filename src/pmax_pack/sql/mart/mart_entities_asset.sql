BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset` WHERE snapshot_date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_entities_asset`
SELECT snapshot_date, account_id, campaign_id, asset_group_id, asset_id,
  field_type, status, primary_status, primary_status_reasons, source,
  asset_name, asset_type, orientation, text, image_url,
  image_height_pixels, image_width_pixels, video_id, video_title,
  first_seen_date, last_seen_date, inferred_removed,
  attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.v_int_entities_asset`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
