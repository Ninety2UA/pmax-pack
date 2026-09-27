-- Rebuild stg_lag_asset_group with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_lag_asset_group` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_lag_asset_group`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_group_name, date, ad_network_type, conversion_action,
  conversion_action_name, conversion_lag_bucket, conversions,
  conversions_value, all_conversions, all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.lag_asset_group`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, asset_group_id, ad_network_type, conversion_action, conversion_lag_bucket
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
