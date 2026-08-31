-- Type and de-duplicate raw performance families for the shared click window.
CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_volume_campaign` (
  source_run_id STRING,
  loaded_at TIMESTAMP,
  query_hash STRING,
  account_id INT64,
  campaign_id INT64,
  campaign_name STRING,
  date DATE,
  ad_network_type STRING,
  impressions INT64,
  clicks INT64,
  cost_micros INT64,
  conversions FLOAT64,
  conversions_value FLOAT64,
  all_conversions FLOAT64,
  all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_volume_asset_group` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64,
  asset_group_name STRING, date DATE, ad_network_type STRING,
  impressions INT64, clicks INT64, cost_micros INT64,
  conversions FLOAT64, conversions_value FLOAT64,
  all_conversions FLOAT64, all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, asset_group_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_volume_asset` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64, asset_id INT64,
  field_type STRING, date DATE, ad_network_type STRING,
  impressions INT64, clicks INT64, cost_micros INT64,
  conversions FLOAT64, conversions_value FLOAT64,
  all_conversions FLOAT64, all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, asset_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_conv_campaign` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, campaign_name STRING,
  date DATE, ad_network_type STRING, conversion_action STRING,
  conversion_action_name STRING, conversions FLOAT64,
  conversions_value FLOAT64, all_conversions FLOAT64,
  all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_conv_asset_group` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64,
  asset_group_name STRING, date DATE, ad_network_type STRING,
  conversion_action STRING, conversion_action_name STRING,
  conversions FLOAT64, conversions_value FLOAT64,
  all_conversions FLOAT64, all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, asset_group_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_conv_asset` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64, asset_id INT64,
  field_type STRING, date DATE, ad_network_type STRING, conversion_action STRING,
  conversion_action_name STRING, conversions FLOAT64,
  conversions_value FLOAT64, all_conversions FLOAT64,
  all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, asset_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_lag_campaign` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, campaign_name STRING,
  date DATE, ad_network_type STRING, conversion_action STRING,
  conversion_action_name STRING, conversion_lag_bucket STRING,
  conversions FLOAT64, conversions_value FLOAT64,
  all_conversions FLOAT64, all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_lag_asset_group` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64,
  asset_group_name STRING, date DATE, ad_network_type STRING,
  conversion_action STRING, conversion_action_name STRING,
  conversion_lag_bucket STRING, conversions FLOAT64,
  conversions_value FLOAT64, all_conversions FLOAT64,
  all_conversions_value FLOAT64
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, asset_group_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_volume_campaign` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_volume_asset_group` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_volume_asset` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_conv_campaign` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_conv_asset_group` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_conv_asset` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_lag_campaign` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_lag_asset_group` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_volume_campaign`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, campaign_name,
  date, ad_network_type, impressions, clicks, cost_micros, conversions,
  conversions_value, all_conversions, all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.volume_campaign`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, ad_network_type
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_volume_asset_group`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_group_name, date, ad_network_type, impressions, clicks, cost_micros,
  conversions, conversions_value, all_conversions, all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.volume_asset_group`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, asset_group_id, ad_network_type
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

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

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_conv_asset_group`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_group_name, date, ad_network_type, conversion_action,
  conversion_action_name, conversions, conversions_value, all_conversions,
  all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.conv_asset_group`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, asset_group_id, ad_network_type, conversion_action
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_conv_asset`
SELECT run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_id, field_type, date, ad_network_type, conversion_action,
  conversion_action_name,
  conversions, conversions_value, all_conversions, all_conversions_value
FROM `{{ project }}.{{ raw_dataset }}.conv_asset`
WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY date, account_id, campaign_id, asset_group_id, asset_id,
    field_type, ad_network_type, conversion_action
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

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
