-- Rebuild stg_entities_asset_group_asset with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_asset` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_asset`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_id, snapshot_date, field_type, status, primary_status,
  primary_status_reasons, source
FROM `{{ project }}.{{ raw_dataset }}.entities_asset_group_asset`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id,
    asset_id, field_type
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
