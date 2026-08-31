WITH
campaign_mart AS (
  SELECT
    date,
    account_id,
    campaign_id,
    ad_network_type,
    SUM(network_impressions) AS impressions,
    SUM(network_clicks) AS clicks,
    SUM(network_cost) AS cost,
    SUM(network_conversions) AS conversions,
    SUM(network_conversions_value) AS conversions_value,
    SUM(network_all_conversions) AS all_conversions,
    SUM(network_all_conversions_value) AS all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_performance_campaign`
  WHERE date = @as_of
    AND metric_basis = 'NETWORK'
  GROUP BY date, account_id, campaign_id, ad_network_type
),
google_truth AS (
  SELECT
    date,
    account_id,
    campaign_id,
    ad_network_type,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost,
    SUM(conversions) AS conversions,
    SUM(conversions_value) AS conversions_value,
    SUM(all_conversions) AS all_conversions,
    SUM(all_conversions_value) AS all_conversions_value
  FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
  WHERE date = @as_of
  GROUP BY date, account_id, campaign_id, ad_network_type
),
violations AS (
  SELECT 1 AS violation
  FROM campaign_mart AS m
  FULL OUTER JOIN google_truth AS g
    ON g.date = m.date
    AND g.account_id = m.account_id
    AND g.campaign_id = m.campaign_id
    AND g.ad_network_type IS NOT DISTINCT FROM m.ad_network_type
  WHERE COALESCE(m.impressions, 0) != COALESCE(g.impressions, 0)
    OR COALESCE(m.clicks, 0) != COALESCE(g.clicks, 0)
    OR ABS(COALESCE(m.cost, 0) - COALESCE(g.cost, 0))
      > CAST(0.000001 AS NUMERIC)
    OR ABS(COALESCE(m.conversions, 0) - COALESCE(g.conversions, 0))
      > GREATEST(
        ABS(COALESCE(g.conversions, 0))
          * {{ tolerances.campaign_reconciliation }},
        0.000000001
      )
    OR ABS(COALESCE(m.conversions_value, 0) - COALESCE(g.conversions_value, 0))
      > GREATEST(
        ABS(COALESCE(g.conversions_value, 0))
          * {{ tolerances.campaign_reconciliation }},
        0.000000001
      )
    OR ABS(COALESCE(m.all_conversions, 0) - COALESCE(g.all_conversions, 0))
      > GREATEST(
        ABS(COALESCE(g.all_conversions, 0))
          * {{ tolerances.campaign_reconciliation }},
        0.000000001
      )
    OR ABS(
      COALESCE(m.all_conversions_value, 0)
      - COALESCE(g.all_conversions_value, 0)
    ) > GREATEST(
      ABS(COALESCE(g.all_conversions_value, 0))
        * {{ tolerances.campaign_reconciliation }},
      0.000000001
    )
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'campaign mart must reconcile to the Google campaign report; cost tolerance is one micro' AS detail
FROM violations
