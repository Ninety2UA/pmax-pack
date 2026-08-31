WITH
snapshot_cells AS (
  SELECT
    click_date,
    account_id,
    campaign_id,
    asset_group_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    cohort_day,
    SUM(cohorted_conversions) AS conversions,
    SUM(cohorted_value) AS conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.int_observation_cells`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND grain = 'asset_group'
    AND provenance IN ('measured', 'carried')
  GROUP BY click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_resource_name,
    cohort_day
),
unavailable_snapshot_keys AS (
  SELECT DISTINCT
    click_date,
    account_id,
    campaign_id,
    asset_group_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    cohort_day
  FROM `{{ project }}.{{ marts_dataset }}.int_observation_cells`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND grain = 'asset_group'
    AND provenance = 'unavailable'
),
bucket_cells AS (
  SELECT
    click_date,
    account_id,
    campaign_id,
    asset_group_id,
    ad_network_type,
    metric_basis,
    conversion_action_resource_name,
    cohort_day,
    SUM(cohorted_conversions) AS conversions,
    SUM(cohorted_value) AS conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_cohort_asset_group`
  WHERE click_date BETWEEN
    DATE_SUB(@as_of, INTERVAL {{ cohort_days|max }} DAY) AND @as_of
    AND provenance IN ('measured', 'carried')
  GROUP BY click_date, account_id, campaign_id, asset_group_id,
    ad_network_type, metric_basis, conversion_action_resource_name,
    cohort_day
),
compared AS (
  SELECT
    COALESCE(s.cohort_day, b.cohort_day) AS cohort_day,
    ABS(COALESCE(s.conversions, 0) - COALESCE(b.conversions, 0))
      > {{ tolerances.cross_grain }}
      OR ABS(
        COALESCE(s.conversions_value, 0) - COALESCE(b.conversions_value, 0)
      ) > {{ tolerances.cross_grain }} AS differs
  FROM snapshot_cells AS s
  FULL OUTER JOIN bucket_cells AS b
    ON b.click_date = s.click_date
    AND b.account_id = s.account_id
    AND b.campaign_id = s.campaign_id
    AND b.asset_group_id = s.asset_group_id
    AND b.ad_network_type IS NOT DISTINCT FROM s.ad_network_type
    AND b.metric_basis = s.metric_basis
    AND b.conversion_action_resource_name IS NOT DISTINCT FROM
      s.conversion_action_resource_name
    AND b.cohort_day = s.cohort_day
  LEFT JOIN unavailable_snapshot_keys AS u
    ON u.click_date = COALESCE(s.click_date, b.click_date)
    AND u.account_id = COALESCE(s.account_id, b.account_id)
    AND u.campaign_id = COALESCE(s.campaign_id, b.campaign_id)
    AND u.asset_group_id = COALESCE(s.asset_group_id, b.asset_group_id)
    AND u.ad_network_type IS NOT DISTINCT FROM
      COALESCE(s.ad_network_type, b.ad_network_type)
    AND u.metric_basis = COALESCE(s.metric_basis, b.metric_basis)
    AND u.conversion_action_resource_name IS NOT DISTINCT FROM
      COALESCE(
        s.conversion_action_resource_name,
        b.conversion_action_resource_name
      )
    AND u.cohort_day = COALESCE(s.cohort_day, b.cohort_day)
  WHERE u.click_date IS NULL
)
SELECT
  COUNTIF(cohort_day = 1 AND differs) = 0 AS passed,
  COUNTIF(cohort_day > 1 AND differs) AS observed,
  0 AS expected,
  'D1 cells must reconcile; observed reports deeper-day divergence' AS detail
FROM compared
