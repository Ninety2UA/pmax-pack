-- Type and de-duplicate entity snapshots. A customer row marks an account's
-- family-D snapshot day complete because extraction flushes the family atomically.
CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_campaign` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, snapshot_date DATE,
  campaign_name STRING, status STRING, primary_status STRING,
  primary_status_reasons ARRAY<STRING>, advertising_channel_type STRING,
  asset_automation_settings ARRAY<STRUCT<asset_automation_type STRING, asset_automation_status STRING>>,
  positive_geo_target_type STRING, negative_geo_target_type STRING,
  start_date_time DATETIME, end_date_time DATETIME,
  budget_id INT64, budget_amount_micros INT64,
  budget_explicitly_shared BOOL, budget_period STRING,
  url_expansion_opt_out BOOL
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64,
  snapshot_date DATE, asset_group_name STRING, status STRING,
  primary_status STRING, primary_status_reasons ARRAY<STRING>,
  ad_strength STRING,
  ad_strength_action_items ARRAY<STRUCT<action_item_type STRING, add_asset_details STRUCT<asset_field_type STRING, asset_count INT64, video_aspect_ratio_requirement STRING>>>,
  final_urls ARRAY<STRING>
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_group_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_asset` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64, asset_id INT64,
  snapshot_date DATE, field_type STRING, status STRING,
  primary_status STRING, primary_status_reasons ARRAY<STRING>, source STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_asset` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64, asset_id INT64,
  snapshot_date DATE, asset_name STRING, asset_type STRING,
  orientation STRING, text STRING, image_url STRING,
  image_height_pixels INT64, image_width_pixels INT64,
  video_id STRING, video_title STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_group_id INT64,
  snapshot_date DATE, signal_resource_name STRING,
  approval_status STRING, audience STRING, search_theme STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_group_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, campaign_id INT64, asset_id INT64, snapshot_date DATE,
  asset_resource_name STRING, field_type STRING, status STRING,
  primary_status STRING, primary_status_reasons ARRAY<STRING>
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_conversion_action` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, conversion_action_id INT64, snapshot_date DATE,
  conversion_action_name STRING, category STRING, counting_type STRING,
  status STRING, click_through_lookback_window_days INT64,
  view_through_lookback_window_days INT64,
  include_in_conversions_metric BOOL, conversion_action_type STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, conversion_action_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.stg_entities_customer` (
  source_run_id STRING, loaded_at TIMESTAMP, query_hash STRING,
  account_id INT64, snapshot_date DATE, descriptive_name STRING,
  currency_code STRING, time_zone STRING, status STRING, manager BOOL
)
PARTITION BY snapshot_date
CLUSTER BY account_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_asset` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_conversion_action` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer` WHERE snapshot_date = @as_of;

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

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  snapshot_date, asset_group_name, status, primary_status,
  primary_status_reasons, ad_strength, ad_strength_action_items, final_urls
FROM `{{ project }}.{{ raw_dataset }}.entities_asset_group`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_asset`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_id, snapshot_date, field_type, status, primary_status,
  primary_status_reasons, source
FROM `{{ project }}.{{ raw_dataset }}.entities_asset_group_asset`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id,
    asset_id, field_type
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  asset_id, snapshot_date, asset_name, asset_type, orientation, text,
  image_url, image_height_pixels, image_width_pixels, video_id, video_title
FROM `{{ project }}.{{ raw_dataset }}.entities_asset`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id, asset_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_group_id,
  snapshot_date, signal_resource_name, approval_status, audience, search_theme
FROM `{{ project }}.{{ raw_dataset }}.entities_asset_group_signal`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_group_id, signal_resource_name
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset`
SELECT
  run_id, loaded_at, query_hash, account_id, campaign_id, asset_id,
  snapshot_date, asset_resource_name, field_type, status, primary_status,
  primary_status_reasons
FROM `{{ project }}.{{ raw_dataset }}.entities_campaign_asset`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, campaign_id, asset_resource_name, field_type
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_conversion_action`
SELECT
  run_id, loaded_at, query_hash, account_id, conversion_action_id,
  snapshot_date, conversion_action_name, category, counting_type, status,
  click_through_lookback_window_days, view_through_lookback_window_days,
  include_in_conversions_metric, conversion_action_type
FROM `{{ project }}.{{ raw_dataset }}.entities_conversion_action`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, conversion_action_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_customer`
SELECT
  run_id, loaded_at, query_hash, account_id, snapshot_date,
  descriptive_name, currency_code, time_zone, status, manager
FROM `{{ project }}.{{ raw_dataset }}.entities_customer`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
