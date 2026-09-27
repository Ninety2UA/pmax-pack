-- Rebuild stg_entities_campaign_asset with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_id,
  snapshot_date, asset_resource_name, field_type, status, primary_status,
  primary_status_reasons
FROM `{{ project }}.{{ raw_dataset }}.entities_campaign_asset`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_resource_name, field_type
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
