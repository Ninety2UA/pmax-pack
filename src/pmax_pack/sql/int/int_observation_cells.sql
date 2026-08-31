-- Resolve snapshot-derived asset cells from the latest successful observation
-- run for every account and observed date. The winning-run CTEs intentionally
-- mirror observe.selected_observations_sql, the canonical selection contract.
CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_observation_cells` (
  click_date DATE,
  account_id INT64,
  grain STRING,
  campaign_id INT64,
  asset_group_id INT64,
  asset_id INT64,
  field_type STRING,
  ad_network_type STRING,
  metric_basis STRING,
  conversion_action_id INT64,
  conversion_action_resource_name STRING,
  conversion_action_name STRING,
  cohort_day INT64,
  is_window_rung BOOL,
  cohort_label STRING,
  window_days INT64,
  window_provenance STRING,
  cohorted_conversions FLOAT64,
  cohorted_value FLOAT64,
  unknown_lag_conversions FLOAT64,
  unknown_lag_value FLOAT64,
  provenance STRING,
  unavailable_reason STRING,
  maturity STRING,
  observed_through TIMESTAMP,
  source_refresh_date DATE,
  source_run_id STRING,
  built_by_run_id STRING
)
PARTITION BY click_date
CLUSTER BY account_id, campaign_id, asset_id, metric_basis;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_observation_cells`
WHERE click_date BETWEEN
  DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_observation_cells`
WITH
observe_success AS (
  SELECT DISTINCT
    run_id,
    account_id
  FROM `{{ project }}.{{ ops_dataset }}.stages`
  WHERE stage = 'observe'
    AND status = 'SUCCESS'
    AND event_ts >= TIMESTAMP(DATE_SUB(@as_of, INTERVAL 37 MONTH))
),
winning_runs AS (
  SELECT
    o.account_id,
    o.observed_date,
    MAX(o.run_id) AS run_id
  FROM `{{ project }}.{{ raw_dataset }}.raw_observations` AS o
  INNER JOIN observe_success AS s
    ON s.run_id = o.run_id
    AND (s.account_id IS NULL OR s.account_id = o.account_id)
  WHERE o.observed_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND o.click_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY o.account_id, o.observed_date
),
selected AS (
  SELECT
    o.run_id,
    o.observed_date,
    o.account_id,
    o.click_date,
    o.grain,
    o.campaign_id,
    o.asset_group_id,
    o.asset_id,
    o.field_type,
    o.ad_network_type,
    o.metric_basis,
    o.conversion_action,
    o.conversion_action_name,
    o.conversions,
    o.conversions_value
  FROM `{{ project }}.{{ raw_dataset }}.raw_observations` AS o
  INNER JOIN winning_runs AS w
    ON o.account_id = w.account_id
    AND o.observed_date = w.observed_date
    AND o.run_id = w.run_id
  WHERE o.observed_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND o.click_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
),
account_observation_bounds AS (
  -- Account-global bound, deliberately independent of the rebuilt click
  -- window: a deep-backfill partition before the account's first observation
  -- must still classify its cells against the account's real observation
  -- reach. Future observations are excluded for deterministic rebuilds.
  SELECT
    o.account_id,
    MAX(o.observed_date) AS latest_selected_observation_date
  FROM `{{ project }}.{{ raw_dataset }}.raw_observations` AS o
  INNER JOIN observe_success AS s
    ON s.run_id = o.run_id
    AND (s.account_id IS NULL OR s.account_id = o.account_id)
  WHERE o.observed_date <= @as_of
  GROUP BY o.account_id
),
account_timezones AS (
  SELECT date AS click_date, account_id, ANY_VALUE(time_zone) AS time_zone
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset`
  WHERE date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND time_zone IS NOT NULL
  GROUP BY click_date, account_id
),
performance_keys AS (
  SELECT DISTINCT
    date AS click_date, account_id, 'asset' AS grain, campaign_id,
    asset_group_id, asset_id, field_type, ad_network_type,
    'PRIMARY' AS metric_basis, CAST(NULL AS INT64) AS conversion_action_id,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    'Primary conversions' AS conversion_action_name,
    time_zone
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset`
  WHERE date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND metric_basis = 'NETWORK'
  UNION DISTINCT
  SELECT DISTINCT
    date, account_id, 'asset', campaign_id, asset_group_id, asset_id,
    field_type, ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS INT64),
    CAST(NULL AS STRING), 'All conversions', time_zone
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset`
  WHERE date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND metric_basis = 'NETWORK'
  UNION DISTINCT
  SELECT DISTINCT
    date, account_id, 'asset', campaign_id, asset_group_id, asset_id,
    field_type, ad_network_type, 'CONVERSION_ACTION', conversion_action_id,
    conversion_action_resource_name, conversion_action_name, time_zone
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset`
  WHERE date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
  UNION DISTINCT
  SELECT DISTINCT
    date, account_id, 'asset_group', campaign_id, asset_group_id,
    CAST(NULL AS INT64), CAST(NULL AS STRING), ad_network_type,
    'PRIMARY', CAST(NULL AS INT64), CAST(NULL AS STRING),
    'Primary conversions', time_zone
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset_group`
  WHERE date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND metric_basis = 'NETWORK'
  UNION DISTINCT
  SELECT DISTINCT
    date, account_id, 'asset_group', campaign_id, asset_group_id,
    CAST(NULL AS INT64), CAST(NULL AS STRING), ad_network_type,
    'ALL_CONVERSIONS', CAST(NULL AS INT64), CAST(NULL AS STRING),
    'All conversions', time_zone
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset_group`
  WHERE date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND metric_basis = 'NETWORK'
  UNION DISTINCT
  SELECT DISTINCT
    date, account_id, 'asset_group', campaign_id, asset_group_id,
    CAST(NULL AS INT64), CAST(NULL AS STRING), ad_network_type,
    'CONVERSION_ACTION', conversion_action_id,
    conversion_action_resource_name, conversion_action_name, time_zone
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_asset_group`
  WHERE date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
),
observation_keys AS (
  SELECT DISTINCT
    click_date,
    account_id,
    grain,
    campaign_id,
    asset_group_id,
    asset_id,
    field_type,
    ad_network_type,
    metric_basis,
    SAFE_CAST(REGEXP_EXTRACT(conversion_action, r'/(\d+)$') AS INT64)
      AS conversion_action_id,
    conversion_action AS conversion_action_resource_name,
    CASE metric_basis
      WHEN 'PRIMARY' THEN 'Primary conversions'
      WHEN 'ALL_CONVERSIONS' THEN 'All conversions'
      ELSE conversion_action_name
    END AS conversion_action_name,
    t.time_zone
  FROM selected AS o
  LEFT JOIN account_timezones AS t USING (account_id, click_date)
  WHERE click_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND grain IN ('asset', 'asset_group')
),
keys AS (
  SELECT * FROM performance_keys
  UNION DISTINCT
  SELECT * FROM observation_keys
),
configured_days AS (
{% for day in cohort_days %}
  SELECT {{ day }} AS cohort_day{% if not loop.last %} UNION ALL{% endif %}
{% endfor %}
),
ladder AS (
  SELECT
    k.*,
    d.cohort_day,
    d.cohort_day = w.click_through_lookback_window_days AS is_window_rung,
    w.click_through_lookback_window_days AS window_days,
    w.window_provenance,
    DATE_ADD(k.click_date, INTERVAL d.cohort_day DAY) AS target_date,
    b.latest_selected_observation_date
  FROM keys AS k
  INNER JOIN `{{ project }}.{{ marts_dataset }}.int_lookback_windows` AS w
    ON w.click_date = k.click_date
    AND w.account_id = k.account_id
    AND w.metric_basis = k.metric_basis
    AND w.conversion_action_resource_name IS NOT DISTINCT FROM
      k.conversion_action_resource_name
  INNER JOIN account_observation_bounds AS b
    ON b.account_id = k.account_id
  CROSS JOIN configured_days AS d
  WHERE w.click_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND d.cohort_day <= w.click_through_lookback_window_days
    AND DATE_ADD(k.click_date, INTERVAL d.cohort_day DAY)
      <= b.latest_selected_observation_date
  UNION DISTINCT
  SELECT
    k.*,
    w.click_through_lookback_window_days,
    TRUE,
    w.click_through_lookback_window_days,
    w.window_provenance,
    DATE_ADD(k.click_date,
      INTERVAL w.click_through_lookback_window_days DAY),
    b.latest_selected_observation_date
  FROM keys AS k
  INNER JOIN `{{ project }}.{{ marts_dataset }}.int_lookback_windows` AS w
    ON w.click_date = k.click_date
    AND w.account_id = k.account_id
    AND w.metric_basis = k.metric_basis
    AND w.conversion_action_resource_name IS NOT DISTINCT FROM
      k.conversion_action_resource_name
  INNER JOIN account_observation_bounds AS b
    ON b.account_id = k.account_id
  WHERE w.click_date BETWEEN
      DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND DATE_ADD(k.click_date,
      INTERVAL w.click_through_lookback_window_days DAY)
    <= b.latest_selected_observation_date
),
first_snapshots AS (
  SELECT
    f.account_id,
    MIN(f.first_snapshot_date) AS first_snapshot_date
  FROM `{{ project }}.{{ ops_dataset }}.first_snapshot` AS f
  INNER JOIN observe_success AS s
    ON s.run_id = f.run_id
    AND s.account_id = f.account_id
  GROUP BY f.account_id
),
ranked AS (
  SELECT
    l.*,
    f.first_snapshot_date,
    o.observed_date,
    o.conversions,
    o.conversions_value,
    o.run_id AS source_run_id
  FROM ladder AS l
  LEFT JOIN first_snapshots AS f
    ON f.account_id = l.account_id
  LEFT JOIN selected AS o
    ON o.click_date = l.click_date
    AND o.account_id = l.account_id
    AND o.grain = l.grain
    AND o.campaign_id = l.campaign_id
    AND o.asset_group_id IS NOT DISTINCT FROM l.asset_group_id
    AND o.asset_id IS NOT DISTINCT FROM l.asset_id
    AND o.field_type IS NOT DISTINCT FROM l.field_type
    AND o.ad_network_type IS NOT DISTINCT FROM l.ad_network_type
    AND o.metric_basis = l.metric_basis
    AND o.conversion_action IS NOT DISTINCT FROM
      l.conversion_action_resource_name
    AND o.observed_date <= l.target_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY l.click_date, l.account_id, l.grain, l.campaign_id,
      l.asset_group_id, l.asset_id, l.field_type, l.ad_network_type,
      l.metric_basis, l.conversion_action_resource_name, l.cohort_day
    ORDER BY o.observed_date DESC, o.run_id DESC
  ) = 1
),
resolved AS (
  SELECT
    r.*,
    CASE
      WHEN r.target_date < r.first_snapshot_date THEN 'unavailable'
      WHEN r.observed_date = r.target_date THEN 'measured'
      WHEN r.observed_date = r.first_snapshot_date THEN 'unavailable'
      WHEN DATE_DIFF(r.target_date, r.observed_date, DAY) BETWEEN 1 AND 5
        THEN 'carried'
      ELSE 'unavailable'
    END AS resolved_provenance,
    CASE
      WHEN r.target_date < r.first_snapshot_date THEN 'before first snapshot'
      WHEN r.observed_date = r.target_date THEN NULL
      WHEN r.observed_date = r.first_snapshot_date THEN 'seed only'
      WHEN DATE_DIFF(r.target_date, r.observed_date, DAY) BETWEEN 1 AND 5
        THEN NULL
      ELSE 'gap exceeded'
    END AS resolved_reason
  FROM ranked AS r
)
SELECT
  click_date,
  account_id,
  grain,
  campaign_id,
  asset_group_id,
  asset_id,
  field_type,
  ad_network_type,
  metric_basis,
  conversion_action_id,
  conversion_action_resource_name,
  conversion_action_name,
  cohort_day,
  is_window_rung,
  CONCAT('D', CAST(cohort_day AS STRING), IF(is_window_rung, ' window', ''))
    AS cohort_label,
  window_days,
  window_provenance,
  IF(resolved_provenance IN ('measured', 'carried'), conversions, NULL)
    AS cohorted_conversions,
  IF(resolved_provenance IN ('measured', 'carried'), conversions_value, NULL)
    AS cohorted_value,
  CAST(NULL AS FLOAT64) AS unknown_lag_conversions,
  CAST(NULL AS FLOAT64) AS unknown_lag_value,
  resolved_provenance AS provenance,
  resolved_reason AS unavailable_reason,
  IF(latest_selected_observation_date >= target_date,
    'complete', 'immature') AS maturity,
  TIMESTAMP(observed_date, COALESCE(time_zone, 'UTC')) AS observed_through,
  observed_date AS source_refresh_date,
  source_run_id,
  @run_id AS built_by_run_id
FROM resolved;
COMMIT TRANSACTION;
