-- Rebuild stg_entities_asset with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_id, snapshot_date, asset_name, asset_type, orientation, text,
  image_url, image_height_pixels, image_width_pixels, video_id, video_title
FROM `{{ project }}.{{ raw_dataset }}.entities_asset`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id, asset_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
