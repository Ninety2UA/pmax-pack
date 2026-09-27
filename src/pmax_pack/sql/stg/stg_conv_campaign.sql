-- Rebuild stg_conv_campaign with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_conv_campaign` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_conv_campaign`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, campaign_name,
  date, ad_network_type, conversion_action, conversion_action_name,
  conversions, conversions_value, all_conversions, all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.conv_campaign`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, ad_network_type, conversion_action
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
