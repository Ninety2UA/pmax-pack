BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group` WHERE snapshot_date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group`
SELECT snapshot_date, account_id, campaign_id, asset_group_id,
  asset_group_name, status, primary_status, primary_status_reasons,
  ad_strength, ad_strength_action_items, final_urls, first_seen_date,
  last_seen_date, inferred_removed, attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.v_int_entities_asset_group`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
