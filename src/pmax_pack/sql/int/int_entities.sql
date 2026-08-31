-- Derive entity history from complete family-D snapshot days. A tombstone is
-- emitted only on the first complete day after an entity was last observed.
CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` (
  account_id INT64, snapshot_date DATE, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_campaign` (
  snapshot_date DATE, account_id INT64, campaign_id INT64,
  campaign_name STRING, status STRING, primary_status STRING,
  primary_status_reasons ARRAY<STRING>, advertising_channel_type STRING,
  asset_automation_settings ARRAY<STRUCT<asset_automation_type STRING, asset_automation_status STRING>>,
  positive_geo_target_type STRING, negative_geo_target_type STRING,
  start_date_time DATETIME, end_date_time DATETIME,
  budget_id INT64, budget_amount_micros INT64,
  budget_explicitly_shared BOOL, budget_period STRING,
  url_expansion_opt_out BOOL, first_seen_date DATE, last_seen_date DATE,
  inferred_removed BOOL, attribute_provenance STRING,
  source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_asset_group` (
  snapshot_date DATE, account_id INT64, campaign_id INT64, asset_group_id INT64,
  asset_group_name STRING, status STRING, primary_status STRING,
  primary_status_reasons ARRAY<STRING>, ad_strength STRING,
  ad_strength_action_items ARRAY<STRUCT<action_item_type STRING, add_asset_details STRUCT<asset_field_type STRING, asset_count INT64, video_aspect_ratio_requirement STRING>>>,
  final_urls ARRAY<STRING>, first_seen_date DATE, last_seen_date DATE,
  inferred_removed BOOL, attribute_provenance STRING,
  source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_group_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_asset` (
  snapshot_date DATE, account_id INT64, campaign_id INT64,
  asset_group_id INT64, asset_id INT64, field_type STRING,
  status STRING, primary_status STRING,
  primary_status_reasons ARRAY<STRING>, source STRING,
  asset_name STRING, asset_type STRING, orientation STRING, text STRING,
  image_url STRING, image_height_pixels INT64, image_width_pixels INT64,
  video_id STRING, video_title STRING,
  first_seen_date DATE, last_seen_date DATE, inferred_removed BOOL,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_asset_group_signal` (
  snapshot_date DATE, account_id INT64, campaign_id INT64,
  asset_group_id INT64, signal_resource_name STRING,
  approval_status STRING, audience STRING, search_theme STRING,
  first_seen_date DATE, last_seen_date DATE, inferred_removed BOOL,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_group_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_campaign_asset` (
  snapshot_date DATE, account_id INT64, campaign_id INT64, asset_id INT64,
  asset_resource_name STRING, field_type STRING, status STRING,
  primary_status STRING, primary_status_reasons ARRAY<STRING>,
  first_seen_date DATE, last_seen_date DATE, inferred_removed BOOL,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_customer_asset` (
  snapshot_date DATE, account_id INT64, asset_id INT64,
  asset_resource_name STRING, field_type STRING, status STRING,
  primary_status STRING, primary_status_reasons ARRAY<STRING>,
  source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, asset_id, field_type;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_conversion_action` (
  snapshot_date DATE, account_id INT64, conversion_action_id INT64,
  conversion_action_name STRING, category STRING, counting_type STRING,
  status STRING, click_through_lookback_window_days INT64,
  view_through_lookback_window_days INT64,
  include_in_conversions_metric BOOL, conversion_action_type STRING,
  first_seen_date DATE, last_seen_date DATE, inferred_removed BOOL,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, conversion_action_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_entities_customer` (
  snapshot_date DATE, account_id INT64, descriptive_name STRING,
  currency_code STRING, time_zone STRING, status STRING, manager BOOL,
  first_seen_date DATE, last_seen_date DATE,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_campaign` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_asset_group` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_asset` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_asset_group_signal` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_campaign_asset` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_customer_asset` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_conversion_action` WHERE snapshot_date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_customer` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days`
SELECT account_id, snapshot_date, source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer`
WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_campaign`
WITH
bounds AS (
  SELECT s.account_id, s.campaign_id,
    MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.campaign_id
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.first_seen_date, b.last_seen_date,
    MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.campaign_id, s.campaign_name,
  s.status, s.primary_status, s.primary_status_reasons,
  s.advertising_channel_type, s.asset_automation_settings,
  s.positive_geo_target_type, s.negative_geo_target_type,
  s.start_date_time, s.end_date_time, s.budget_id, s.budget_amount_micros,
  s.budget_explicitly_shared, s.budget_period, s.url_expansion_opt_out,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign` AS s
JOIN bounds AS b USING (account_id, campaign_id)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id,
  CAST(NULL AS STRING), 'REMOVED', 'REMOVED',
  ['ENTITY_NOT_PRESENT_ON_COMPLETE_SNAPSHOT'], CAST(NULL AS STRING),
  CAST(NULL AS ARRAY<STRUCT<asset_automation_type STRING, asset_automation_status STRING>>),
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS DATETIME),
  CAST(NULL AS DATETIME), CAST(NULL AS INT64), CAST(NULL AS INT64),
  CAST(NULL AS BOOL), CAST(NULL AS STRING), CAST(NULL AS BOOL),
  CAST(NULL AS DATE), CAST(NULL AS DATE), TRUE, 'inferred-removed',
  CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_asset_group`
WITH
bounds AS (
  SELECT s.account_id, s.campaign_id, s.asset_group_id,
    MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.campaign_id, s.asset_group_id
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.asset_group_id,
    b.first_seen_date, b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.asset_group_id, b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.campaign_id, s.asset_group_id,
  s.asset_group_name, s.status, s.primary_status, s.primary_status_reasons,
  s.ad_strength, s.ad_strength_action_items, s.final_urls,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group` AS s
JOIN bounds AS b USING (account_id, campaign_id, asset_group_id)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id, n.asset_group_id,
  CAST(NULL AS STRING), 'REMOVED', 'REMOVED',
  ['ENTITY_NOT_PRESENT_ON_COMPLETE_SNAPSHOT'], CAST(NULL AS STRING),
  CAST(NULL AS ARRAY<STRUCT<action_item_type STRING, add_asset_details STRUCT<asset_field_type STRING, asset_count INT64, video_aspect_ratio_requirement STRING>>>),
  CAST(NULL AS ARRAY<STRING>), CAST(NULL AS DATE), CAST(NULL AS DATE),
  TRUE, 'inferred-removed', CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_asset`
WITH
observed AS (
  SELECT l.snapshot_date, l.account_id, l.campaign_id, l.asset_group_id,
    l.asset_id, l.field_type, l.status, l.primary_status,
    l.primary_status_reasons, l.source, a.asset_name, a.asset_type,
    a.orientation, a.text, a.image_url, a.image_height_pixels,
    a.image_width_pixels, a.video_id, a.video_title, l.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_asset` AS l
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.stg_entities_asset` AS a
    USING (snapshot_date, account_id, campaign_id, asset_group_id, asset_id)
),
bounds AS (
  SELECT account_id, campaign_id, asset_group_id, asset_id, field_type,
    MIN(snapshot_date) AS first_seen_date,
    MAX(snapshot_date) AS last_seen_date
  FROM observed
  WHERE snapshot_date <= @as_of
  GROUP BY account_id, campaign_id, asset_group_id, asset_id, field_type
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.asset_group_id, b.asset_id,
    b.field_type,
    b.first_seen_date, b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.asset_group_id, b.asset_id,
    b.field_type,
    b.first_seen_date, b.last_seen_date
)
SELECT o.snapshot_date, o.account_id, o.campaign_id, o.asset_group_id,
  o.asset_id, o.field_type, o.status, o.primary_status,
  o.primary_status_reasons, o.source, o.asset_name, o.asset_type,
  o.orientation, o.text, o.image_url, o.image_height_pixels,
  o.image_width_pixels, o.video_id, o.video_title,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  o.source_run_id, @run_id
FROM observed AS o
JOIN bounds AS b
  USING (account_id, campaign_id, asset_group_id, asset_id, field_type)
WHERE o.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id, n.asset_group_id,
  n.asset_id, n.field_type, 'REMOVED', 'REMOVED',
  ['ENTITY_NOT_PRESENT_ON_COMPLETE_SNAPSHOT'], CAST(NULL AS STRING),
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT64),
  CAST(NULL AS INT64), CAST(NULL AS STRING), CAST(NULL AS STRING),
  CAST(NULL AS DATE), CAST(NULL AS DATE), TRUE, 'inferred-removed',
  CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_asset_group_signal`
WITH
bounds AS (
  SELECT s.account_id, s.campaign_id, s.asset_group_id,
    s.signal_resource_name, MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.campaign_id, s.asset_group_id,
    s.signal_resource_name
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.asset_group_id, b.signal_resource_name,
    b.first_seen_date, b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.asset_group_id,
    b.signal_resource_name, b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.campaign_id, s.asset_group_id,
  s.signal_resource_name, s.approval_status, s.audience, s.search_theme,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal` AS s
JOIN bounds AS b USING (account_id, campaign_id, asset_group_id, signal_resource_name)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id, n.asset_group_id,
  n.signal_resource_name, 'REMOVED', CAST(NULL AS STRING), CAST(NULL AS STRING),
  CAST(NULL AS DATE), CAST(NULL AS DATE), TRUE, 'inferred-removed',
  CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_campaign_asset`
WITH
bounds AS (
  SELECT s.account_id, s.campaign_id, s.asset_resource_name, s.field_type,
    MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.campaign_id, s.asset_resource_name, s.field_type
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.asset_resource_name, b.field_type,
    b.first_seen_date, b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.asset_resource_name, b.field_type,
    b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.campaign_id, s.asset_id,
  s.asset_resource_name, s.field_type, s.status, s.primary_status,
  s.primary_status_reasons, CAST(NULL AS DATE), CAST(NULL AS DATE),
  FALSE, 'observed', s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset` AS s
JOIN bounds AS b USING (account_id, campaign_id, asset_resource_name, field_type)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id, CAST(NULL AS INT64),
  n.asset_resource_name, n.field_type, 'REMOVED', 'REMOVED',
  ['ENTITY_NOT_PRESENT_ON_COMPLETE_SNAPSHOT'], CAST(NULL AS DATE),
  CAST(NULL AS DATE), TRUE, 'inferred-removed', CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_customer_asset`
SELECT s.snapshot_date, s.account_id, s.asset_id, s.asset_resource_name,
  s.field_type, s.status, s.primary_status, s.primary_status_reasons,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer_asset` AS s
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_conversion_action`
WITH
bounds AS (
  SELECT s.account_id, s.conversion_action_id,
    MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_conversion_action` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.conversion_action_id
),
next_complete AS (
  SELECT b.account_id, b.conversion_action_id, b.first_seen_date,
    b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.conversion_action_id, b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.conversion_action_id,
  s.conversion_action_name, s.category, s.counting_type, s.status,
  s.click_through_lookback_window_days, s.view_through_lookback_window_days,
  s.include_in_conversions_metric, s.conversion_action_type,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_conversion_action` AS s
JOIN bounds AS b USING (account_id, conversion_action_id)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.conversion_action_id,
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), 'REMOVED',
  CAST(NULL AS INT64), CAST(NULL AS INT64), CAST(NULL AS BOOL),
  CAST(NULL AS STRING), CAST(NULL AS DATE), CAST(NULL AS DATE), TRUE,
  'inferred-removed', CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_customer`
WITH bounds AS (
  SELECT s.account_id, MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id
)
SELECT s.snapshot_date, s.account_id, s.descriptive_name, s.currency_code,
  s.time_zone, s.status, s.manager, CAST(NULL AS DATE), CAST(NULL AS DATE),
  'observed', s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer` AS s
JOIN bounds AS b USING (account_id)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of;
COMMIT TRANSACTION;
