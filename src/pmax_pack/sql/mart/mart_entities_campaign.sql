BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign` WHERE snapshot_date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_entities_campaign`
SELECT snapshot_date, account_id, campaign_id, campaign_name, status,
  primary_status, primary_status_reasons, advertising_channel_type,
  asset_automation_settings, positive_geo_target_type,
  negative_geo_target_type, start_date_time, end_date_time, budget_id,
  SAFE_DIVIDE(CAST(budget_amount_micros AS NUMERIC), CAST(1000000 AS NUMERIC)),
  budget_explicitly_shared, budget_period, url_expansion_opt_out,
  first_seen_date, last_seen_date, inferred_removed,
  attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.v_int_entities_campaign`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
