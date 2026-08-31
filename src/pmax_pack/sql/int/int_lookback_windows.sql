-- Resolve the click-through window in force on each click day. Pre-snapshot
-- click days use the first entity snapshot and state that assumption.
CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.int_lookback_windows` (
  click_date DATE,
  account_id INT64,
  metric_basis STRING,
  conversion_action_id INT64,
  conversion_action_resource_name STRING,
  conversion_action_name STRING,
  click_through_lookback_window_days INT64,
  window_provenance STRING,
  include_in_conversions_metric BOOL,
  source_snapshot_date DATE,
  source_run_id STRING,
  built_by_run_id STRING
)
PARTITION BY click_date
CLUSTER BY account_id, metric_basis, conversion_action_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_lookback_windows`
WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_lookback_windows`
WITH
action_keys AS (
  SELECT DISTINCT
    date AS click_date,
    account_id,
    conversion_action_id,
    conversion_action_resource_name,
    conversion_action_name
  FROM `{{ project }}.{{ marts_dataset }}.int_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
  UNION DISTINCT
  SELECT DISTINCT
    date,
    account_id,
    SAFE_CAST(REGEXP_EXTRACT(conversion_action, r'/(\d+)$') AS INT64),
    conversion_action,
    conversion_action_name
  FROM `{{ project }}.{{ marts_dataset }}.stg_lag_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
),
-- During the Google Ads goals migration, campaign-goal-primary actions can
-- contribute to the Conversions metric while the legacy inclusion flag is
-- false. Nonzero family B/C conversions are therefore membership evidence.
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
entity_actions AS (
  SELECT
    snapshot_date,
    account_id,
    conversion_action_id,
    conversion_action_name,
    click_through_lookback_window_days,
    include_in_conversions_metric,
    source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.int_entities_conversion_action`
  WHERE NOT inferred_removed
    AND click_through_lookback_window_days IS NOT NULL
),
account_defaults AS (
  SELECT
    account_id,
    MAX(click_through_lookback_window_days)
      AS click_through_lookback_window_days
  FROM entity_actions
  GROUP BY account_id
),
chosen_action AS (
  SELECT
    k.click_date,
    k.account_id,
    COALESCE(k.conversion_action_id, e.conversion_action_id) AS conversion_action_id,
    k.conversion_action_resource_name,
    COALESCE(k.conversion_action_name, e.conversion_action_name) AS conversion_action_name,
    COALESCE(e.click_through_lookback_window_days,
      d.click_through_lookback_window_days)
      AS click_through_lookback_window_days,
    CASE
      WHEN e.snapshot_date IS NOT NULL AND e.snapshot_date <= k.click_date
        THEN 'observed'
      ELSE 'assumed-current'
    END AS window_provenance,
    e.include_in_conversions_metric,
    COALESCE(p.has_primary_conversions, FALSE) AS has_primary_conversions,
    e.snapshot_date AS source_snapshot_date,
    e.source_run_id
  FROM action_keys AS k
  LEFT JOIN entity_actions AS e
    ON e.account_id = k.account_id
    AND e.conversion_action_id = k.conversion_action_id
  LEFT JOIN account_defaults AS d
    ON d.account_id = k.account_id
  LEFT JOIN primary_action_evidence AS p
    ON p.click_date = k.click_date
    AND p.account_id = k.account_id
    AND p.conversion_action_resource_name IS NOT DISTINCT FROM
      k.conversion_action_resource_name
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY k.click_date, k.account_id,
      k.conversion_action_resource_name
    ORDER BY
      IF(e.snapshot_date <= k.click_date, 0, 1),
      IF(e.snapshot_date <= k.click_date, e.snapshot_date, NULL) DESC,
      e.snapshot_date ASC
  ) = 1
),
action_windows AS (
  SELECT
    click_date,
    account_id,
    'CONVERSION_ACTION' AS metric_basis,
    conversion_action_id,
    conversion_action_resource_name,
    conversion_action_name,
    click_through_lookback_window_days,
    window_provenance,
    include_in_conversions_metric,
    include_in_conversions_metric OR has_primary_conversions
      AS contributes_to_primary,
    source_snapshot_date,
    source_run_id
  FROM chosen_action
  WHERE click_through_lookback_window_days IS NOT NULL
),
aggregate_windows AS (
  SELECT
    click_date,
    account_id,
    'PRIMARY' AS metric_basis,
    CAST(NULL AS INT64) AS conversion_action_id,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    'Primary conversions' AS conversion_action_name,
    MAX(IF(contributes_to_primary,
      click_through_lookback_window_days, NULL)) AS click_through_lookback_window_days,
    IF(COUNTIF(contributes_to_primary
      AND window_provenance = 'assumed-current') > 0,
      'assumed-current', 'observed') AS window_provenance,
    CAST(NULL AS BOOL) AS include_in_conversions_metric,
    MIN(IF(contributes_to_primary, source_snapshot_date, NULL))
      AS source_snapshot_date,
    MIN(IF(contributes_to_primary, source_run_id, NULL)) AS source_run_id
  FROM action_windows
  GROUP BY click_date, account_id
  HAVING COUNTIF(contributes_to_primary) > 0
  UNION ALL
  SELECT
    click_date,
    account_id,
    'ALL_CONVERSIONS',
    CAST(NULL AS INT64),
    CAST(NULL AS STRING),
    'All conversions',
    MAX(click_through_lookback_window_days),
    IF(COUNTIF(window_provenance = 'assumed-current') > 0,
      'assumed-current', 'observed'),
    CAST(NULL AS BOOL),
    MIN(source_snapshot_date),
    MIN(source_run_id)
  FROM action_windows
  GROUP BY click_date, account_id
)
SELECT
  click_date,
  account_id,
  metric_basis,
  conversion_action_id,
  conversion_action_resource_name,
  conversion_action_name,
  click_through_lookback_window_days,
  window_provenance,
  include_in_conversions_metric,
  source_snapshot_date,
  source_run_id,
  @run_id AS built_by_run_id
FROM action_windows
UNION ALL
SELECT
  click_date,
  account_id,
  metric_basis,
  conversion_action_id,
  conversion_action_resource_name,
  conversion_action_name,
  click_through_lookback_window_days,
  window_provenance,
  include_in_conversions_metric,
  source_snapshot_date,
  source_run_id,
  @run_id
FROM aggregate_windows;
COMMIT TRANSACTION;
