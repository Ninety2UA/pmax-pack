-- Rebuild stg_entities_campaign with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_campaign`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, snapshot_date,
  campaign_name, status, primary_status, primary_status_reasons,
  advertising_channel_type, asset_automation_settings,
  positive_geo_target_type, negative_geo_target_type,
  SAFE.PARSE_DATETIME('%F %T', NULLIF(start_date_time, '')),
  SAFE.PARSE_DATETIME('%F %T', NULLIF(end_date_time, '')),
  budget_id, budget_amount_micros, budget_explicitly_shared, budget_period,
  url_expansion_opt_out
FROM `{{ project }}.{{ raw_dataset }}.entities_campaign`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
