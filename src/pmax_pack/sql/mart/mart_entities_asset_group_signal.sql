BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group_signal` WHERE snapshot_date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group_signal`
SELECT snapshot_date, account_id, campaign_id, asset_group_id,
  signal_resource_name, approval_status, audience, search_theme,
  first_seen_date, last_seen_date, inferred_removed,
  attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.v_int_entities_asset_group_signal`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
