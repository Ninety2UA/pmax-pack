-- Build additive performance bases and attach entity attributes as of click day.
-- Before the first complete entity day, the earliest snapshot is carried with
-- attribute_provenance = assumed-current.
CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_performance_campaign` (
  date DATE, account_id INT64, campaign_id INT64, metric_basis STRING,
  ad_network_type STRING, conversion_action_id INT64,
  conversion_action_resource_name STRING, conversion_action_name STRING,
  network_impressions INT64, network_clicks INT64, network_cost NUMERIC,
  network_conversions FLOAT64, network_conversions_value FLOAT64,
  network_all_conversions FLOAT64, network_all_conversions_value FLOAT64,
  action_conversions FLOAT64, action_conversions_value FLOAT64,
  action_all_conversions FLOAT64, action_all_conversions_value FLOAT64,
  campaign_name STRING, campaign_status STRING, campaign_primary_status STRING,
  campaign_primary_status_reasons ARRAY<STRING>, currency_code STRING,
  time_zone STRING, click_through_lookback_window_days INT64,
  view_through_lookback_window_days INT64,
  include_in_conversions_metric BOOL, conversion_action_type STRING,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_performance_asset_group` (
  date DATE, account_id INT64, campaign_id INT64, asset_group_id INT64,
  metric_basis STRING, ad_network_type STRING, conversion_action_id INT64,
  conversion_action_resource_name STRING, conversion_action_name STRING,
  network_impressions INT64, network_clicks INT64, network_cost NUMERIC,
  network_conversions FLOAT64, network_conversions_value FLOAT64,
  network_all_conversions FLOAT64, network_all_conversions_value FLOAT64,
  action_conversions FLOAT64, action_conversions_value FLOAT64,
  action_all_conversions FLOAT64, action_all_conversions_value FLOAT64,
  asset_group_name STRING, asset_group_status STRING,
  asset_group_primary_status STRING,
  asset_group_primary_status_reasons ARRAY<STRING>, ad_strength STRING,
  currency_code STRING, time_zone STRING,
  click_through_lookback_window_days INT64,
  view_through_lookback_window_days INT64,
  include_in_conversions_metric BOOL, conversion_action_type STRING,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, asset_group_id;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_performance_asset` (
  date DATE, account_id INT64, campaign_id INT64, asset_group_id INT64,
  asset_id INT64, metric_basis STRING, ad_network_type STRING,
  conversion_action_id INT64, conversion_action_resource_name STRING,
  conversion_action_name STRING,
  network_impressions INT64, network_clicks INT64, network_cost NUMERIC,
  network_conversions FLOAT64, network_conversions_value FLOAT64,
  network_all_conversions FLOAT64, network_all_conversions_value FLOAT64,
  action_conversions FLOAT64, action_conversions_value FLOAT64,
  action_all_conversions FLOAT64, action_all_conversions_value FLOAT64,
  field_type STRING, asset_status STRING, asset_primary_status STRING,
  asset_primary_status_reasons ARRAY<STRING>, asset_source STRING,
  asset_name STRING, asset_type STRING, orientation STRING, text STRING,
  image_url STRING, image_height_pixels INT64, image_width_pixels INT64,
  video_id STRING, video_title STRING, currency_code STRING, time_zone STRING,
  click_through_lookback_window_days INT64,
  view_through_lookback_window_days INT64,
  include_in_conversions_metric BOOL, conversion_action_type STRING,
  attribute_provenance STRING, source_run_id STRING, built_by_run_id STRING
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, asset_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_performance_campaign` WHERE date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset_group` WHERE date = @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset` WHERE date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_performance_campaign`
WITH
performance AS (
  SELECT date, account_id, campaign_id, 'NETWORK' AS metric_basis,
    ad_network_type, CAST(NULL AS INT64) AS conversion_action_id,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    CAST(NULL AS STRING) AS conversion_action_name,
    impressions AS network_impressions, clicks AS network_clicks,
    SAFE_DIVIDE(CAST(cost_micros AS NUMERIC), CAST(1000000 AS NUMERIC)) AS network_cost,
    conversions AS network_conversions,
    conversions_value AS network_conversions_value,
    all_conversions AS network_all_conversions,
    all_conversions_value AS network_all_conversions_value,
    CAST(NULL AS FLOAT64) AS action_conversions,
    CAST(NULL AS FLOAT64) AS action_conversions_value,
    CAST(NULL AS FLOAT64) AS action_all_conversions,
    CAST(NULL AS FLOAT64) AS action_all_conversions_value,
    source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_campaign`
  WHERE date = @as_of
  UNION ALL
  SELECT date, account_id, campaign_id, 'CONVERSION_ACTION', ad_network_type,
    SAFE_CAST(REGEXP_EXTRACT(conversion_action, r'/(\d+)$') AS INT64),
    conversion_action, conversion_action_name, CAST(NULL AS INT64),
    CAST(NULL AS INT64),
    CAST(NULL AS NUMERIC), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
    CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), conversions,
    conversions_value, all_conversions, all_conversions_value, source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_conv_campaign`
  WHERE date = @as_of
),
with_entity AS (
  SELECT p.*, e.campaign_name, e.status AS campaign_status,
    e.primary_status AS campaign_primary_status,
    e.primary_status_reasons AS campaign_primary_status_reasons,
    CASE
      WHEN e.first_seen_date IS NULL THEN 'unavailable'
      WHEN p.date < e.first_seen_date THEN 'assumed-current'
      ELSE e.attribute_provenance
    END AS attribute_provenance
  FROM performance AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_campaign` AS e
    ON e.account_id = p.account_id AND e.campaign_id = p.campaign_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.metric_basis,
      p.ad_network_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(e.snapshot_date <= p.date, 0, 1),
      IF(e.snapshot_date <= p.date, e.snapshot_date, NULL) DESC,
      e.snapshot_date ASC
  ) = 1
),
with_customer AS (
  SELECT p.*, c.currency_code, c.time_zone
  FROM with_entity AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_customer` AS c
    ON c.account_id = p.account_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.metric_basis,
      p.ad_network_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(c.snapshot_date <= p.date, 0, 1),
      IF(c.snapshot_date <= p.date, c.snapshot_date, NULL) DESC,
      c.snapshot_date ASC
  ) = 1
),
with_action AS (
  SELECT p.*, a.click_through_lookback_window_days,
    a.view_through_lookback_window_days, a.include_in_conversions_metric,
    a.conversion_action_type
  FROM with_customer AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_conversion_action` AS a
    ON a.account_id = p.account_id
    AND a.conversion_action_id = p.conversion_action_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.metric_basis,
      p.ad_network_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(a.snapshot_date <= p.date, 0, 1),
      IF(a.snapshot_date <= p.date, a.snapshot_date, NULL) DESC,
      a.snapshot_date ASC
  ) = 1
)
SELECT date, account_id, campaign_id, metric_basis, ad_network_type,
  conversion_action_id, conversion_action_resource_name,
  conversion_action_name, network_impressions,
  network_clicks, network_cost, network_conversions,
  network_conversions_value, network_all_conversions,
  network_all_conversions_value, action_conversions,
  action_conversions_value, action_all_conversions,
  action_all_conversions_value, campaign_name, campaign_status,
  campaign_primary_status, campaign_primary_status_reasons,
  currency_code, time_zone, click_through_lookback_window_days,
  view_through_lookback_window_days, include_in_conversions_metric,
  conversion_action_type, attribute_provenance, source_run_id, @run_id
FROM with_action;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_performance_asset_group`
WITH
performance AS (
  SELECT date, account_id, campaign_id, asset_group_id, 'NETWORK' AS metric_basis,
    ad_network_type, CAST(NULL AS INT64) AS conversion_action_id,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    CAST(NULL AS STRING) AS conversion_action_name,
    impressions AS network_impressions, clicks AS network_clicks,
    SAFE_DIVIDE(CAST(cost_micros AS NUMERIC), CAST(1000000 AS NUMERIC)) AS network_cost,
    conversions AS network_conversions,
    conversions_value AS network_conversions_value,
    all_conversions AS network_all_conversions,
    all_conversions_value AS network_all_conversions_value,
    CAST(NULL AS FLOAT64) AS action_conversions,
    CAST(NULL AS FLOAT64) AS action_conversions_value,
    CAST(NULL AS FLOAT64) AS action_all_conversions,
    CAST(NULL AS FLOAT64) AS action_all_conversions_value,
    source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_asset_group`
  WHERE date = @as_of
  UNION ALL
  SELECT date, account_id, campaign_id, asset_group_id,
    'CONVERSION_ACTION', ad_network_type,
    SAFE_CAST(REGEXP_EXTRACT(conversion_action, r'/(\d+)$') AS INT64),
    conversion_action, conversion_action_name, CAST(NULL AS INT64),
    CAST(NULL AS INT64),
    CAST(NULL AS NUMERIC), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
    CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), conversions,
    conversions_value, all_conversions, all_conversions_value, source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_conv_asset_group`
  WHERE date = @as_of
),
with_entity AS (
  SELECT p.*, e.asset_group_name, e.status AS asset_group_status,
    e.primary_status AS asset_group_primary_status,
    e.primary_status_reasons AS asset_group_primary_status_reasons,
    e.ad_strength,
    CASE
      WHEN e.first_seen_date IS NULL THEN 'unavailable'
      WHEN p.date < e.first_seen_date THEN 'assumed-current'
      ELSE e.attribute_provenance
    END AS attribute_provenance
  FROM performance AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_asset_group` AS e
    ON e.account_id = p.account_id AND e.campaign_id = p.campaign_id
    AND e.asset_group_id = p.asset_group_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.asset_group_id,
      p.metric_basis, p.ad_network_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(e.snapshot_date <= p.date, 0, 1),
      IF(e.snapshot_date <= p.date, e.snapshot_date, NULL) DESC,
      e.snapshot_date ASC
  ) = 1
),
with_customer AS (
  SELECT p.*, c.currency_code, c.time_zone
  FROM with_entity AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_customer` AS c
    ON c.account_id = p.account_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.asset_group_id,
      p.metric_basis, p.ad_network_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(c.snapshot_date <= p.date, 0, 1),
      IF(c.snapshot_date <= p.date, c.snapshot_date, NULL) DESC,
      c.snapshot_date ASC
  ) = 1
),
with_action AS (
  SELECT p.*, a.click_through_lookback_window_days,
    a.view_through_lookback_window_days, a.include_in_conversions_metric,
    a.conversion_action_type
  FROM with_customer AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_conversion_action` AS a
    ON a.account_id = p.account_id
    AND a.conversion_action_id = p.conversion_action_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.asset_group_id,
      p.metric_basis, p.ad_network_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(a.snapshot_date <= p.date, 0, 1),
      IF(a.snapshot_date <= p.date, a.snapshot_date, NULL) DESC,
      a.snapshot_date ASC
  ) = 1
)
SELECT date, account_id, campaign_id, asset_group_id, metric_basis,
  ad_network_type, conversion_action_id, conversion_action_resource_name,
  conversion_action_name,
  network_impressions, network_clicks, network_cost, network_conversions,
  network_conversions_value, network_all_conversions,
  network_all_conversions_value, action_conversions,
  action_conversions_value, action_all_conversions,
  action_all_conversions_value, asset_group_name, asset_group_status,
  asset_group_primary_status, asset_group_primary_status_reasons,
  ad_strength, currency_code, time_zone,
  click_through_lookback_window_days, view_through_lookback_window_days,
  include_in_conversions_metric, conversion_action_type,
  attribute_provenance, source_run_id, @run_id
FROM with_action;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_performance_asset`
WITH
performance AS (
  SELECT date, account_id, campaign_id, asset_group_id, asset_id, field_type,
    'NETWORK' AS metric_basis, ad_network_type,
    CAST(NULL AS INT64) AS conversion_action_id,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    CAST(NULL AS STRING) AS conversion_action_name,
    impressions AS network_impressions, clicks AS network_clicks,
    SAFE_DIVIDE(CAST(cost_micros AS NUMERIC), CAST(1000000 AS NUMERIC)) AS network_cost,
    conversions AS network_conversions,
    conversions_value AS network_conversions_value,
    all_conversions AS network_all_conversions,
    all_conversions_value AS network_all_conversions_value,
    CAST(NULL AS FLOAT64) AS action_conversions,
    CAST(NULL AS FLOAT64) AS action_conversions_value,
    CAST(NULL AS FLOAT64) AS action_all_conversions,
    CAST(NULL AS FLOAT64) AS action_all_conversions_value,
    source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_asset`
  WHERE date = @as_of
  UNION ALL
  SELECT date, account_id, campaign_id, asset_group_id, asset_id, field_type,
    'CONVERSION_ACTION', ad_network_type,
    SAFE_CAST(REGEXP_EXTRACT(conversion_action, r'/(\d+)$') AS INT64),
    conversion_action, conversion_action_name, CAST(NULL AS INT64),
    CAST(NULL AS INT64),
    CAST(NULL AS NUMERIC), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
    CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), conversions,
    conversions_value, all_conversions, all_conversions_value, source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_conv_asset`
  WHERE date = @as_of
),
with_entity AS (
  SELECT p.*, e.status AS asset_status,
    e.primary_status AS asset_primary_status,
    e.primary_status_reasons AS asset_primary_status_reasons,
    e.source AS asset_source, e.asset_name, e.asset_type, e.orientation,
    e.text, e.image_url, e.image_height_pixels, e.image_width_pixels,
    e.video_id, e.video_title,
    CASE
      WHEN e.first_seen_date IS NULL THEN 'unavailable'
      WHEN p.date < e.first_seen_date THEN 'assumed-current'
      ELSE e.attribute_provenance
    END AS attribute_provenance
  FROM performance AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_asset` AS e
    ON e.account_id = p.account_id AND e.campaign_id = p.campaign_id
    AND e.asset_group_id = p.asset_group_id AND e.asset_id = p.asset_id
    AND e.field_type = p.field_type
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.asset_group_id,
      p.asset_id, p.metric_basis, p.ad_network_type,
      p.field_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(e.snapshot_date <= p.date, 0, 1),
      IF(e.snapshot_date <= p.date, e.snapshot_date, NULL) DESC,
      e.snapshot_date ASC
  ) = 1
),
with_customer AS (
  SELECT p.*, c.currency_code, c.time_zone
  FROM with_entity AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_customer` AS c
    ON c.account_id = p.account_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.asset_group_id,
      p.asset_id, p.metric_basis, p.ad_network_type,
      p.field_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(c.snapshot_date <= p.date, 0, 1),
      IF(c.snapshot_date <= p.date, c.snapshot_date, NULL) DESC,
      c.snapshot_date ASC
  ) = 1
),
with_action AS (
  SELECT p.*, a.click_through_lookback_window_days,
    a.view_through_lookback_window_days, a.include_in_conversions_metric,
    a.conversion_action_type
  FROM with_customer AS p
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.v_int_entities_conversion_action` AS a
    ON a.account_id = p.account_id
    AND a.conversion_action_id = p.conversion_action_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.date, p.account_id, p.campaign_id, p.asset_group_id,
      p.asset_id, p.metric_basis, p.ad_network_type,
      p.field_type, p.conversion_action_id,
      p.conversion_action_resource_name
    ORDER BY IF(a.snapshot_date <= p.date, 0, 1),
      IF(a.snapshot_date <= p.date, a.snapshot_date, NULL) DESC,
      a.snapshot_date ASC
  ) = 1
)
SELECT date, account_id, campaign_id, asset_group_id, asset_id, metric_basis,
  ad_network_type, conversion_action_id, conversion_action_resource_name,
  conversion_action_name,
  network_impressions, network_clicks, network_cost, network_conversions,
  network_conversions_value, network_all_conversions,
  network_all_conversions_value, action_conversions,
  action_conversions_value, action_all_conversions,
  action_all_conversions_value, field_type, asset_status,
  asset_primary_status, asset_primary_status_reasons, asset_source,
  asset_name, asset_type, orientation, text, image_url,
  image_height_pixels, image_width_pixels, video_id, video_title,
  currency_code, time_zone, click_through_lookback_window_days,
  view_through_lookback_window_days, include_in_conversions_metric,
  conversion_action_type, attribute_provenance, source_run_id, @run_id
FROM with_action;
COMMIT TRANSACTION;
