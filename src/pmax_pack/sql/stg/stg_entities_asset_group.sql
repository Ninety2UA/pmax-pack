-- Rebuild stg_entities_asset_group with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  snapshot_date, asset_group_name, status, primary_status,
  primary_status_reasons, ad_strength, ad_strength_action_items, final_urls
FROM `{{ project }}.{{ raw_dataset }}.entities_asset_group`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
