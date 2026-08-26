BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign_asset` WHERE snapshot_date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_entities_campaign_asset`
SELECT snapshot_date, account_id, campaign_id, asset_id,
  asset_resource_name, field_type, status, primary_status,
  primary_status_reasons, first_seen_date, last_seen_date,
  inferred_removed, attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.v_int_entities_campaign_asset`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
