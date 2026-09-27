-- Rebuild stg_entities_asset_group_signal with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  snapshot_date, signal_resource_name, approval_status, audience, search_theme
FROM `{{ project }}.{{ raw_dataset }}.entities_asset_group_signal`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id, signal_resource_name
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
