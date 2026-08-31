-- Asset metrics are participation credit, so summing assets can legitimately
-- exceed campaign truth. The invariant is one-sided per asset: no single
-- asset's credit may exceed its campaign/network truth plus tolerance.
WITH
campaign_truth AS (
  SELECT
    date,
    account_id,
    campaign_id,
    ad_network_type,
    SUM(conversions) AS conversions,
    SUM(conversions_value) AS conversions_value,
    SUM(all_conversions) AS all_conversions,
    SUM(all_conversions_value) AS all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
  WHERE date = @as_of
  GROUP BY date, account_id, campaign_id, ad_network_type
),
asset_maxima AS (
  SELECT
    date,
    account_id,
    campaign_id,
    ad_network_type,
    MAX(network_conversions) AS conversions,
    MAX(network_conversions_value) AS conversions_value,
    MAX(network_all_conversions) AS all_conversions,
    MAX(network_all_conversions_value) AS all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_asset_performance`
  WHERE date = @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, ad_network_type
),
violations AS (
  SELECT 1 AS violation
  FROM asset_maxima AS a
  FULL OUTER JOIN campaign_truth AS c
    ON c.date = a.date
    AND c.account_id = a.account_id
    AND c.campaign_id = a.campaign_id
    AND c.ad_network_type IS NOT DISTINCT FROM a.ad_network_type
  WHERE COALESCE(a.conversions, 0) > COALESCE(c.conversions, 0)
      + GREATEST(
        ABS(COALESCE(c.conversions, 0)) * {{ tolerances.asset_vs_campaign }},
        0.000000001
      )
    OR COALESCE(a.conversions_value, 0) > COALESCE(c.conversions_value, 0)
      + GREATEST(
        ABS(COALESCE(c.conversions_value, 0))
          * {{ tolerances.asset_vs_campaign }},
        0.000000001
      )
    OR COALESCE(a.all_conversions, 0) > COALESCE(c.all_conversions, 0)
      + GREATEST(
        ABS(COALESCE(c.all_conversions, 0))
          * {{ tolerances.asset_vs_campaign }},
        0.000000001
      )
    OR COALESCE(a.all_conversions_value, 0)
      > COALESCE(c.all_conversions_value, 0)
      + GREATEST(
        ABS(COALESCE(c.all_conversions_value, 0))
          * {{ tolerances.asset_vs_campaign }},
        0.000000001
      )
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'each asset participation credit must not exceed campaign truth' AS detail
FROM violations
