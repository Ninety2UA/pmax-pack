-- Rebuild int_performance_campaign with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_performance_campaign` WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

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
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT date, account_id, campaign_id, 'CONVERSION_ACTION', ad_network_type,
    SAFE_CAST(REGEXP_EXTRACT(conversion_action, r'/(\d+)$') AS INT64),
    conversion_action, conversion_action_name, CAST(NULL AS INT64),
    CAST(NULL AS INT64),
    CAST(NULL AS NUMERIC), CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64),
    CAST(NULL AS FLOAT64), CAST(NULL AS FLOAT64), conversions,
    conversions_value, all_conversions, all_conversions_value, source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_conv_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
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
COMMIT TRANSACTION;
