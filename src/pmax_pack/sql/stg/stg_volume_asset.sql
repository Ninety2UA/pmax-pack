-- Rebuild stg_volume_asset with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_volume_asset` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_volume_asset`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_id, field_type, date, ad_network_type, impressions, clicks, cost_micros,
  conversions, conversions_value, all_conversions, all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.volume_asset`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, asset_group_id, asset_id,
    field_type, ad_network_type
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
