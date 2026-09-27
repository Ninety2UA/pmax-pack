SELECT
  date,
  account_id,
  campaign_id,
  campaign_name,
  campaign_status,
  ad_network_type,
  currency_code,
  attribute_provenance,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(cost) AS cost,
  SUM(conversions) AS conversions,
  SUM(conversions_value) AS conversions_value,
  SUM(all_conversions) AS all_conversions,
  SUM(all_conversions_value) AS all_conversions_value,
  SAFE_DIVIDE(SUM(clicks), NULLIF(SUM(impressions), 0)) AS ctr,
  SAFE_DIVIDE(SUM(cost), NULLIF(SUM(conversions), 0)) AS cpa,
  SAFE_DIVIDE(SUM(conversions_value), NULLIF(SUM(cost), 0)) AS roas
FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
GROUP BY date, account_id, campaign_id, campaign_name, campaign_status,
  ad_network_type, currency_code, attribute_provenance
