-- Rebuild stg_lag_campaign with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_lag_campaign` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_lag_campaign`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, campaign_name,
  date, ad_network_type, conversion_action, conversion_action_name,
  conversion_lag_bucket, conversions, conversions_value, all_conversions,
  all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.lag_campaign`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, ad_network_type, conversion_action, conversion_lag_bucket
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
