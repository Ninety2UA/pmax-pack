WITH
performance AS (
  SELECT
    'campaign' AS grain,
    date AS click_date,
    account_id,
    campaign_id,
    CAST(NULL AS INT64) AS asset_group_id,
    ad_network_type,
    'PRIMARY' AS metric_basis,
    CAST(NULL AS STRING) AS conversion_action_resource_name,
    SUM(network_conversions) AS conversions,
    SUM(network_conversions_value) AS conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, ad_network_type
  UNION ALL
  SELECT 'campaign', date, account_id, campaign_id, CAST(NULL AS INT64),
    ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS STRING),
    SUM(network_all_conversions), SUM(network_all_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, ad_network_type
  UNION ALL
  SELECT 'campaign', date, account_id, campaign_id, CAST(NULL AS INT64),
    ad_network_type, 'CONVERSION_ACTION', conversion_action_resource_name,
    SUM(action_conversions), SUM(action_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
  GROUP BY date, account_id, campaign_id, ad_network_type,
    conversion_action_resource_name
  UNION ALL
  SELECT 'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'PRIMARY', CAST(NULL AS STRING),
    SUM(network_conversions), SUM(network_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, asset_group_id, ad_network_type
  UNION ALL
  SELECT 'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'ALL_CONVERSIONS', CAST(NULL AS STRING),
    SUM(network_all_conversions), SUM(network_all_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, asset_group_id, ad_network_type
  UNION ALL
  SELECT 'asset_group', date, account_id, campaign_id, asset_group_id,
    ad_network_type, 'CONVERSION_ACTION', conversion_action_resource_name,
    SUM(action_conversions), SUM(action_conversions_value)
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY)
      AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
  GROUP BY date, account_id, campaign_id, asset_group_id, ad_network_type,
    conversion_action_resource_name
),
-- Derive capped expectations from source buckets, independently of cohort prefixes.
source_buckets AS (
  SELECT
    'campaign' AS grain,
    date AS click_date,
    account_id,
    campaign_id,
    CAST(NULL AS INT64) AS asset_group_id,
    ad_network_type,
    conversion_action AS conversion_action_resource_name,
    conversion_lag_bucket,
    conversions,
    conversions_value,
    all_conversions,
    all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.stg_lag_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
  UNION ALL
  SELECT
    'asset_group' AS grain,
    date AS click_date,
    account_id,
    campaign_id,
    asset_group_id AS asset_group_id,
    ad_network_type,
    conversion_action AS conversion_action_resource_name,
    conversion_lag_bucket,
    conversions,
    conversions_value,
    all_conversions,
    all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.stg_lag_asset_group`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
),
primary_evidence AS (
  SELECT
    click_date,
    account_id,
    conversion_action_resource_name
  FROM performance
  WHERE grain = 'campaign'
    AND metric_basis = 'CONVERSION_ACTION'
    AND COALESCE(conversions, 0) != 0
  UNION DISTINCT
  SELECT
    click_date,
    account_id,
    conversion_action_resource_name
  FROM source_buckets
  WHERE grain = 'campaign'
    AND COALESCE(conversions, 0) != 0
),
action_windows AS (
  SELECT
    click_date,
    account_id,
    conversion_action_resource_name,
    click_through_lookback_window_days,
    include_in_conversions_metric
  FROM `{{ project }}.{{ marts_dataset }}.int_lookback_windows`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND metric_basis = 'CONVERSION_ACTION'
),
classified_buckets AS (
  SELECT
    b.grain,
    b.click_date,
    b.account_id,
    b.campaign_id,
    b.asset_group_id,
    b.ad_network_type,
    b.conversion_action_resource_name,
    b.conversions,
    b.conversions_value,
    b.all_conversions,
    b.all_conversions_value,
    COALESCE(w.click_through_lookback_window_days, 90) AS action_window_days,
    COALESCE(w.include_in_conversions_metric, FALSE)
      OR p.account_id IS NOT NULL AS contributes_to_primary,
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
    END AS bucket_upper_day
  FROM source_buckets AS b
  LEFT JOIN action_windows AS w
    ON w.click_date = b.click_date
    AND w.account_id = b.account_id
    AND w.conversion_action_resource_name = b.conversion_action_resource_name
  LEFT JOIN primary_evidence AS p
    ON p.click_date = b.click_date
    AND p.account_id = b.account_id
    AND p.conversion_action_resource_name = b.conversion_action_resource_name
),
capped_bucket_totals AS (
  SELECT
    b.grain,
    b.click_date,
    b.account_id,
    b.campaign_id,
    b.asset_group_id,
    b.ad_network_type,
    basis AS metric_basis,
    IF(basis = 'CONVERSION_ACTION', b.conversion_action_resource_name, NULL)
      AS conversion_action_resource_name,
    SUM(IF(b.bucket_upper_day IS NULL OR b.bucket_upper_day
      <= LEAST({{ reporting_window_days }}, b.action_window_days),
      IF(basis = 'ALL_CONVERSIONS', b.all_conversions, b.conversions), 0))
      AS conversions,
    SUM(IF(b.bucket_upper_day IS NULL OR b.bucket_upper_day
      <= LEAST({{ reporting_window_days }}, b.action_window_days),
      IF(basis = 'ALL_CONVERSIONS', b.all_conversions_value, b.conversions_value), 0))
      AS conversions_value
  FROM classified_buckets AS b
  CROSS JOIN UNNEST(['PRIMARY', 'ALL_CONVERSIONS', 'CONVERSION_ACTION']) AS basis
  WHERE basis != 'PRIMARY' OR b.contributes_to_primary
  GROUP BY b.grain, b.click_date, b.account_id, b.campaign_id,
    b.asset_group_id, b.ad_network_type, basis,
    IF(basis = 'CONVERSION_ACTION', b.conversion_action_resource_name, NULL)
),
cohort_cells AS (
  SELECT
    'campaign' AS grain,
    click_date,
    account_id,
    campaign_id,
    CAST(NULL AS INT64) AS asset_group_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    cohorted_conversions + unknown_lag_conversions AS conversions,
    cohorted_value + unknown_lag_value AS conversions_value,
    provenance IS DISTINCT FROM 'unavailable'
      AND window_provenance IS DISTINCT FROM 'capped by reporting window' AS is_comparable,
    provenance IS DISTINCT FROM 'unavailable'
      AND window_provenance = 'capped by reporting window' AS is_capped,
    provenance = 'unavailable' AS is_unavailable
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_campaign`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND is_window_rung
  UNION ALL
  SELECT
    'asset_group' AS grain,
    click_date,
    account_id,
    campaign_id,
    asset_group_id AS asset_group_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    cohorted_conversions + unknown_lag_conversions AS conversions,
    cohorted_value + unknown_lag_value AS conversions_value,
    provenance IS DISTINCT FROM 'unavailable'
      AND window_provenance IS DISTINCT FROM 'capped by reporting window' AS is_comparable,
    provenance IS DISTINCT FROM 'unavailable'
      AND window_provenance = 'capped by reporting window' AS is_capped,
    provenance = 'unavailable' AS is_unavailable
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset_group`
  WHERE click_date BETWEEN DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND is_window_rung
),
cohort AS (
  SELECT
    grain,
    click_date,
    account_id,
    campaign_id,
    asset_group_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    SUM(IF(is_comparable, conversions, NULL)) AS conversions,
    SUM(IF(is_comparable, conversions_value, NULL)) AS conversions_value,
    SUM(IF(is_capped, conversions, NULL)) AS capped_conversions,
    SUM(IF(is_capped, conversions_value, NULL)) AS capped_conversions_value,
    COUNTIF(is_comparable) AS available_cell_count,
    COUNTIF(is_capped) AS capped_cell_count,
    COUNTIF(is_unavailable) AS unavailable_cell_count
  FROM cohort_cells
  GROUP BY grain, click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_resource_name
),
compared AS (
  SELECT
    c.unavailable_cell_count,
    -- Missing keys still fail; unavailable and capped cells are separate populations.
    ((COALESCE(c.available_cell_count, 0) > 0
      OR COALESCE(c.unavailable_cell_count, 0) + COALESCE(c.capped_cell_count, 0) = 0)
    AND (ABS(COALESCE(c.conversions, 0) - COALESCE(p.conversions, 0))
      > GREATEST(
        ABS(COALESCE(p.conversions, 0))
          * {{ tolerances.campaign_reconciliation }},
        0.000000001
      )
    OR ABS(COALESCE(c.conversions_value, 0) - COALESCE(p.conversions_value, 0))
      > GREATEST(
        ABS(COALESCE(p.conversions_value, 0))
          * {{ tolerances.campaign_reconciliation }},
        0.000000001
      )))
    OR (COALESCE(c.capped_cell_count, 0) > 0
      AND (ABS(COALESCE(c.capped_conversions, 0) - COALESCE(b.conversions, 0))
        > GREATEST(
          ABS(COALESCE(b.conversions, 0))
            * {{ tolerances.campaign_reconciliation }},
          0.000000001
        )
      OR ABS(COALESCE(c.capped_conversions_value, 0) - COALESCE(b.conversions_value, 0))
        > GREATEST(
          ABS(COALESCE(b.conversions_value, 0))
            * {{ tolerances.campaign_reconciliation }},
          0.000000001
        ))) AS is_violation
  FROM performance AS p
  FULL OUTER JOIN cohort AS c
    ON c.grain = p.grain
    AND c.click_date = p.click_date
    AND c.account_id = p.account_id
    AND c.campaign_id = p.campaign_id
    AND c.asset_group_id IS NOT DISTINCT FROM p.asset_group_id
    AND c.ad_network_type IS NOT DISTINCT FROM p.ad_network_type
    AND c.metric_basis = p.metric_basis
    AND c.conversion_action_resource_name IS NOT DISTINCT FROM p.conversion_action_resource_name
  LEFT JOIN capped_bucket_totals AS b
    ON c.grain = b.grain
    AND c.click_date = b.click_date
    AND c.account_id = b.account_id
    AND c.campaign_id = b.campaign_id
    AND c.asset_group_id IS NOT DISTINCT FROM b.asset_group_id
    AND c.ad_network_type IS NOT DISTINCT FROM b.ad_network_type
    AND c.metric_basis = b.metric_basis
    AND c.conversion_action_resource_name IS NOT DISTINCT FROM b.conversion_action_resource_name
)
SELECT
  COUNTIF(is_violation) = 0 AS passed,
  COUNTIF(is_violation) AS observed,
  0 AS expected,
  CONCAT(
    'window-rung cohort totals plus unknown lag must reconcile to performance marts; ',
    'capped window cells checked against source buckets; ',
    'unavailable window cells excluded: ',
    CAST(COALESCE(SUM(unavailable_cell_count), 0) AS STRING)
  ) AS detail
FROM compared
