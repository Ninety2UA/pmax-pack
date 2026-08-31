-- Family C cohort prefixes at campaign and asset-group grain. All
-- grain-neutral rules are evaluated once in lag_prefix_cells, then projected
-- to the two stable internal tables.
CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_lag_prefix_campaign` (
  click_date DATE, account_id INT64, campaign_id INT64,
  ad_network_type STRING, metric_basis STRING, conversion_action_id INT64,
  conversion_action_resource_name STRING, conversion_action_name STRING,
  cohort_day INT64, is_window_rung BOOL, cohort_label STRING,
  window_days INT64, window_provenance STRING,
  cohorted_conversions FLOAT64, cohorted_value FLOAT64,
  unknown_lag_conversions FLOAT64, unknown_lag_value FLOAT64,
  provenance STRING, unavailable_reason STRING, maturity STRING,
  observed_through TIMESTAMP, source_refresh_date DATE,
  source_run_id STRING, built_by_run_id STRING
)
PARTITION BY click_date
CLUSTER BY account_id, campaign_id, metric_basis;

CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_lag_prefix_asset_group` (
  click_date DATE, account_id INT64, campaign_id INT64,
  asset_group_id INT64, ad_network_type STRING, metric_basis STRING,
  conversion_action_id INT64, conversion_action_resource_name STRING,
  conversion_action_name STRING, cohort_day INT64, is_window_rung BOOL,
  cohort_label STRING, window_days INT64, window_provenance STRING,
  cohorted_conversions FLOAT64, cohorted_value FLOAT64,
  unknown_lag_conversions FLOAT64, unknown_lag_value FLOAT64,
  provenance STRING, unavailable_reason STRING, maturity STRING,
  observed_through TIMESTAMP, source_refresh_date DATE,
  source_run_id STRING, built_by_run_id STRING
)
PARTITION BY click_date
CLUSTER BY account_id, campaign_id, asset_group_id, metric_basis;

CREATE TEMP TABLE lag_prefix_cells AS
WITH
configured_days AS (
{% for day in cohort_days %}
  SELECT {{ day }} AS cohort_day{% if not loop.last %} UNION ALL{% endif %}
{% endfor %}
),
timezones AS (
  SELECT account_id, ANY_VALUE(time_zone) AS time_zone
  FROM (
    SELECT account_id, time_zone
    FROM `{{ project }}.{{ marts_dataset }}.int_performance_campaign`
    WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of AND time_zone IS NOT NULL
    UNION ALL
    SELECT account_id, time_zone
    FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset_group`
    WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of AND time_zone IS NOT NULL
  )
  GROUP BY account_id
),
-- The goals migration can leave a campaign-goal-primary action's legacy
-- inclusion flag false even while Google reports it in Conversions. Treat
-- nonzero family B/C conversions as PRIMARY membership evidence.
primary_action_evidence AS (
  SELECT
    click_date,
    account_id,
    conversion_action_resource_name,
    COUNTIF(COALESCE(conversions, 0) != 0) > 0 AS has_primary_conversions
  FROM (
    SELECT
      date AS click_date,
      account_id,
      conversion_action_resource_name,
      action_conversions AS conversions
    FROM `{{ project }}.{{ marts_dataset }}.int_performance_campaign`
    WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
      AND metric_basis = 'CONVERSION_ACTION'
    UNION ALL
    SELECT
      date,
      account_id,
      conversion_action,
      conversions
    FROM `{{ project }}.{{ marts_dataset }}.stg_lag_campaign`
    WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  )
  GROUP BY click_date, account_id, conversion_action_resource_name
),
source_buckets AS (
  SELECT
    'campaign' AS grain,
    b.date AS click_date,
    b.account_id,
    b.campaign_id,
    CAST(NULL AS INT64) AS asset_group_id,
    b.ad_network_type,
    SAFE_CAST(REGEXP_EXTRACT(b.conversion_action, r'/(\d+)$') AS INT64)
      AS conversion_action_id,
    b.conversion_action AS conversion_action_resource_name,
    b.conversion_action_name,
    b.conversion_lag_bucket,
    b.conversions,
    b.conversions_value,
    b.all_conversions,
    b.all_conversions_value,
    DATE(b.loaded_at, COALESCE(t.time_zone, 'UTC')) AS source_refresh_date,
    b.loaded_at AS source_refresh_ts,
    b.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_lag_campaign` AS b
  LEFT JOIN timezones AS t USING (account_id)
  WHERE b.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT
    'asset_group', b.date, b.account_id, b.campaign_id, b.asset_group_id,
    b.ad_network_type,
    SAFE_CAST(REGEXP_EXTRACT(b.conversion_action, r'/(\d+)$') AS INT64),
    b.conversion_action, b.conversion_action_name, b.conversion_lag_bucket,
    b.conversions, b.conversions_value, b.all_conversions,
    b.all_conversions_value,
    DATE(b.loaded_at, COALESCE(t.time_zone, 'UTC')), b.loaded_at,
    b.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_lag_asset_group` AS b
  LEFT JOIN timezones AS t USING (account_id)
  WHERE b.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
),
action_buckets AS (
  SELECT
    b.*,
    CASE b.conversion_lag_bucket
      WHEN 'LESS_THAN_ONE_DAY' THEN 1
      WHEN 'ONE_TO_TWO_DAYS' THEN 2
      WHEN 'TWO_TO_THREE_DAYS' THEN 3
      WHEN 'THREE_TO_FOUR_DAYS' THEN 4
      WHEN 'FOUR_TO_FIVE_DAYS' THEN 5
      WHEN 'FIVE_TO_SIX_DAYS' THEN 6
      WHEN 'SIX_TO_SEVEN_DAYS' THEN 7
      WHEN 'SEVEN_TO_EIGHT_DAYS' THEN 8
      WHEN 'EIGHT_TO_NINE_DAYS' THEN 9
      WHEN 'NINE_TO_TEN_DAYS' THEN 10
      WHEN 'TEN_TO_ELEVEN_DAYS' THEN 11
      WHEN 'ELEVEN_TO_TWELVE_DAYS' THEN 12
      WHEN 'TWELVE_TO_THIRTEEN_DAYS' THEN 13
      WHEN 'THIRTEEN_TO_FOURTEEN_DAYS' THEN 14
      WHEN 'FOURTEEN_TO_TWENTY_ONE_DAYS' THEN 21
      WHEN 'TWENTY_ONE_TO_THIRTY_DAYS' THEN 30
      WHEN 'THIRTY_TO_FORTY_FIVE_DAYS' THEN 45
      WHEN 'FORTY_FIVE_TO_SIXTY_DAYS' THEN 60
      WHEN 'SIXTY_TO_NINETY_DAYS' THEN 90
      ELSE NULL
    END AS bucket_upper_day,
    COALESCE(w.click_through_lookback_window_days, 90)
      AS action_window_days,
    COALESCE(w.window_provenance, 'assumed-current') AS action_window_provenance,
    w.include_in_conversions_metric,
    COALESCE(w.include_in_conversions_metric, FALSE)
      OR COALESCE(p.has_primary_conversions, FALSE) AS contributes_to_primary
  FROM source_buckets AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_lookback_windows` AS w
    ON w.click_date = b.click_date
    AND w.account_id = b.account_id
    AND w.metric_basis = 'CONVERSION_ACTION'
    AND w.conversion_action_resource_name = b.conversion_action_resource_name
  LEFT JOIN primary_action_evidence AS p
    ON p.click_date = b.click_date
    AND p.account_id = b.account_id
    AND p.conversion_action_resource_name IS NOT DISTINCT FROM
      b.conversion_action_resource_name
),
basis_buckets AS (
  SELECT
    grain, click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'CONVERSION_ACTION' AS metric_basis,
    conversion_action_id, conversion_action_resource_name,
    conversion_action_name, bucket_upper_day, conversions,
    conversions_value, source_refresh_date, source_refresh_ts, source_run_id,
    action_window_days, action_window_provenance
  FROM action_buckets
  UNION ALL
  SELECT
    grain, click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'PRIMARY', CAST(NULL AS INT64), CAST(NULL AS STRING),
    'Primary conversions', bucket_upper_day, conversions, conversions_value,
    source_refresh_date, source_refresh_ts, source_run_id,
    action_window_days, action_window_provenance
  FROM action_buckets
  WHERE contributes_to_primary
  UNION ALL
  SELECT
    grain, click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS INT64),
    CAST(NULL AS STRING), 'All conversions', bucket_upper_day,
    all_conversions, all_conversions_value, source_refresh_date,
    source_refresh_ts, source_run_id, action_window_days,
    action_window_provenance
  FROM action_buckets
),
keys AS (
  SELECT
    grain, click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_id,
    conversion_action_resource_name, conversion_action_name,
    MAX(action_window_days) AS window_days,
    IF(COUNTIF(action_window_provenance = 'assumed-current') > 0,
      'assumed-current', 'observed') AS window_provenance
  FROM basis_buckets
  GROUP BY grain, click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_id,
    conversion_action_resource_name, conversion_action_name
),
ladder AS (
  SELECT
    k.*,
    d.cohort_day,
    d.cohort_day = k.window_days AS is_window_rung
  FROM keys AS k
  CROSS JOIN configured_days AS d
  WHERE d.cohort_day <= k.window_days
  UNION DISTINCT
  SELECT k.*, k.window_days, TRUE
  FROM keys AS k
),
prefixes AS (
  SELECT
    l.*,
    SUM(IF(b.bucket_upper_day IS NOT NULL
      AND b.bucket_upper_day <= LEAST(l.cohort_day, b.action_window_days),
      b.conversions, 0)) AS prefix_conversions,
    SUM(IF(b.bucket_upper_day IS NOT NULL
      AND b.bucket_upper_day <= LEAST(l.cohort_day, b.action_window_days),
      b.conversions_value, 0)) AS prefix_value,
    SUM(IF(b.bucket_upper_day IS NULL, b.conversions, 0))
      AS unknown_lag_conversions,
    SUM(IF(b.bucket_upper_day IS NULL, b.conversions_value, 0))
      AS unknown_lag_value,
    MAX(b.source_refresh_date) AS bucket_refresh_date,
    MAX(b.source_refresh_ts) AS bucket_refresh_ts,
    MIN(b.source_run_id) AS bucket_source_run_id
  FROM ladder AS l
  INNER JOIN basis_buckets AS b
    ON b.grain = l.grain
    AND b.click_date = l.click_date
    AND b.account_id = l.account_id
    AND b.campaign_id = l.campaign_id
    AND b.asset_group_id IS NOT DISTINCT FROM l.asset_group_id
    AND b.ad_network_type IS NOT DISTINCT FROM l.ad_network_type
    AND b.metric_basis = l.metric_basis
    AND b.conversion_action_resource_name IS NOT DISTINCT FROM
      l.conversion_action_resource_name
  GROUP BY l.grain, l.click_date, l.account_id, l.campaign_id,
    l.asset_group_id, l.ad_network_type, l.metric_basis,
    l.conversion_action_id, l.conversion_action_resource_name,
    l.conversion_action_name, l.window_days, l.window_provenance,
    l.cohort_day, l.is_window_rung
),
totals AS (
  SELECT
    'campaign' AS grain, date AS click_date, account_id, campaign_id,
    CAST(NULL AS INT64) AS asset_group_id, ad_network_type,
    'PRIMARY' AS metric_basis,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    conversions AS total_conversions, conversions_value AS total_value,
    DATE(v.loaded_at, COALESCE(t.time_zone, 'UTC')) AS total_refresh_date,
    v.loaded_at AS total_refresh_ts, v.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_campaign` AS v
  LEFT JOIN timezones AS t USING (account_id)
  WHERE v.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT
    'campaign', date, account_id, campaign_id, CAST(NULL AS INT64),
    ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS STRING),
    all_conversions, all_conversions_value,
    DATE(v.loaded_at, COALESCE(t.time_zone, 'UTC')), v.loaded_at,
    v.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_campaign` AS v
  LEFT JOIN timezones AS t USING (account_id)
  WHERE v.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT
    'campaign', date, account_id, campaign_id, CAST(NULL AS INT64),
    ad_network_type, 'CONVERSION_ACTION', conversion_action, conversions,
    conversions_value, DATE(v.loaded_at, COALESCE(t.time_zone, 'UTC')),
    v.loaded_at, v.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_conv_campaign` AS v
  LEFT JOIN timezones AS t USING (account_id)
  WHERE v.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT
    'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'PRIMARY', CAST(NULL AS STRING), conversions,
    conversions_value, DATE(v.loaded_at, COALESCE(t.time_zone, 'UTC')),
    v.loaded_at, v.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_asset_group` AS v
  LEFT JOIN timezones AS t USING (account_id)
  WHERE v.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT
    'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS STRING),
    all_conversions, all_conversions_value,
    DATE(v.loaded_at, COALESCE(t.time_zone, 'UTC')), v.loaded_at,
    v.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_asset_group` AS v
  LEFT JOIN timezones AS t USING (account_id)
  WHERE v.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  UNION ALL
  SELECT
    'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'CONVERSION_ACTION', conversion_action, conversions,
    conversions_value, DATE(v.loaded_at, COALESCE(t.time_zone, 'UTC')),
    v.loaded_at, v.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_conv_asset_group` AS v
  LEFT JOIN timezones AS t USING (account_id)
  WHERE v.date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
),
resolved AS (
  SELECT
    p.*,
    IF(p.is_window_rung
      AND p.window_days NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 21, 30, 45, 60, 90),
      t.total_conversions, p.prefix_conversions) AS cohorted_conversions,
    IF(p.is_window_rung
      AND p.window_days NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 21, 30, 45, 60, 90),
      t.total_value, p.prefix_value) AS cohorted_value,
    IF(p.is_window_rung
      AND p.window_days NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 21, 30, 45, 60, 90),
      t.total_refresh_date, p.bucket_refresh_date) AS source_refresh_date,
    IF(p.is_window_rung
      AND p.window_days NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 21, 30, 45, 60, 90),
      t.total_refresh_ts, p.bucket_refresh_ts) AS source_refresh_ts,
    IF(p.is_window_rung
      AND p.window_days NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 21, 30, 45, 60, 90),
      t.source_run_id, p.bucket_source_run_id) AS source_run_id
  FROM prefixes AS p
  LEFT JOIN totals AS t
    ON t.grain = p.grain
    AND t.click_date = p.click_date
    AND t.account_id = p.account_id
    AND t.campaign_id = p.campaign_id
    AND t.asset_group_id IS NOT DISTINCT FROM p.asset_group_id
    AND t.ad_network_type IS NOT DISTINCT FROM p.ad_network_type
    AND t.metric_basis = p.metric_basis
    AND t.conversion_action_resource_name IS NOT DISTINCT FROM
      p.conversion_action_resource_name
)
SELECT
  grain, click_date, account_id, campaign_id, asset_group_id,
  ad_network_type, metric_basis, conversion_action_id,
  conversion_action_resource_name, conversion_action_name, cohort_day,
  is_window_rung,
  CONCAT('D', CAST(cohort_day AS STRING), IF(is_window_rung, ' window', ''))
    AS cohort_label,
  window_days, window_provenance, cohorted_conversions, cohorted_value,
  unknown_lag_conversions, unknown_lag_value, 'measured' AS provenance,
  CAST(NULL AS STRING) AS unavailable_reason,
  IF(source_refresh_date >= DATE_ADD(click_date, INTERVAL cohort_day DAY),
    'complete', 'immature') AS maturity,
  source_refresh_ts AS observed_through, source_refresh_date, source_run_id,
  @run_id AS built_by_run_id
FROM resolved;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_lag_prefix_campaign`
WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_lag_prefix_asset_group`
WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_lag_prefix_campaign`
SELECT * EXCEPT (grain, asset_group_id)
FROM lag_prefix_cells
WHERE grain = 'campaign';

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_lag_prefix_asset_group`
SELECT * EXCEPT (grain)
FROM lag_prefix_cells
WHERE grain = 'asset_group';
COMMIT TRANSACTION;
