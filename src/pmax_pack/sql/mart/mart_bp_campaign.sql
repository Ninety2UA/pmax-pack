CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.mart_bp_campaign` (
  snapshot_date DATE,
  account_id INT64,
  campaign_id INT64,
  campaign_name STRING,
  campaign_status STRING,
  url_expansion_opt_out BOOL,
  url_expansion_known BOOL,
  url_expansion_score FLOAT64,
  audience_signals_count INT64,
  asset_group_count INT64,
  audience_signals_score FLOAT64,
  sitelink_count INT64,
  missing_sitelinks INT64,
  sitelink_score FLOAT64,
  positive_geo_target_type STRING,
  negative_geo_target_type STRING,
  positive_geo_target_type_configured_good STRING,
  negative_geo_target_type_configured_good STRING,
  geo_target_score FLOAT64,
  campaign_bp_score FLOAT64,
  url_expansion_parity_mode STRING,
  run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_bp_campaign`
WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_bp_campaign`
WITH
asset_group_counts AS (
  SELECT a.account_id, a.campaign_id,
    COUNT(DISTINCT a.asset_group_id) AS asset_group_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset` AS a
  JOIN `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group` AS g
    USING (snapshot_date, account_id, campaign_id, asset_group_id)
  WHERE a.snapshot_date = @as_of
    AND a.status = 'ENABLED'
    AND a.source = 'ADVERTISER'
    AND NOT COALESCE(a.inferred_removed, FALSE)
    AND g.status = 'ENABLED'
    AND NOT COALESCE(g.inferred_removed, FALSE)
  GROUP BY a.account_id, a.campaign_id
),
audience_counts AS (
  SELECT account_id, campaign_id,
    COUNT(DISTINCT audience) AS audience_signals_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group_signal`
  WHERE snapshot_date = @as_of
    AND audience IS NOT NULL
    AND audience != ''
    AND NOT COALESCE(inferred_removed, FALSE)
  GROUP BY account_id, campaign_id
),
campaign_sitelinks AS (
  SELECT account_id, campaign_id, COUNT(*) AS campaign_sitelinks
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign_asset`
  WHERE snapshot_date = @as_of
    AND field_type = 'SITELINK'
    AND NOT COALESCE(inferred_removed, FALSE)
  GROUP BY account_id, campaign_id
),
account_sitelinks AS (
  SELECT account_id, COUNT(*) AS account_sitelinks
  FROM `{{ project }}.{{ marts_dataset }}.int_entities_customer_asset`
  WHERE snapshot_date = @as_of
    AND field_type = 'SITELINK'
  GROUP BY account_id
),
components AS (
  SELECT c.snapshot_date, c.account_id, c.campaign_id, c.campaign_name,
    c.status AS campaign_status, c.url_expansion_opt_out,
    c.url_expansion_opt_out IS NOT NULL AS url_expansion_known,
    CASE
      WHEN c.url_expansion_opt_out IS NULL THEN CAST(NULL AS FLOAT64)
      WHEN c.url_expansion_opt_out THEN 1.0
      ELSE 0.0
    END AS url_expansion_score,
    COALESCE(aud.audience_signals_count, 0) AS audience_signals_count,
    COALESCE(ag.asset_group_count, 0) AS asset_group_count,
    CASE
      WHEN aud.audience_signals_count IS NULL THEN 0.0
      ELSE SAFE_DIVIDE(aud.audience_signals_count, ag.asset_group_count)
    END AS audience_signals_score,
    COALESCE(cs.campaign_sitelinks, 0)
      + COALESCE(acs.account_sitelinks, 0) AS sitelink_count,
    GREATEST(
      4 - COALESCE(cs.campaign_sitelinks, 0)
        - COALESCE(acs.account_sitelinks, 0),
      0
    ) AS missing_sitelinks,
    c.positive_geo_target_type,
    c.negative_geo_target_type,
    IF(
      c.positive_geo_target_type != 'PRESENCE_OR_INTEREST',
      'X',
      'Yes'
    ) AS positive_geo_target_type_configured_good,
    IF(
      c.negative_geo_target_type != 'PRESENCE_OR_INTEREST',
      'X',
      'Yes'
    ) AS negative_geo_target_type_configured_good
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign` AS c
  LEFT JOIN asset_group_counts AS ag USING (account_id, campaign_id)
  LEFT JOIN audience_counts AS aud USING (account_id, campaign_id)
  LEFT JOIN campaign_sitelinks AS cs USING (account_id, campaign_id)
  LEFT JOIN account_sitelinks AS acs USING (account_id)
  WHERE c.snapshot_date = @as_of
    AND c.advertising_channel_type = 'PERFORMANCE_MAX'
    AND NOT COALESCE(c.inferred_removed, FALSE)
),
scored AS (
  SELECT *,
    IF(
      missing_sitelinks = 0,
      1.0,
      SAFE_DIVIDE(CAST(missing_sitelinks AS FLOAT64), 4.0)
    ) AS sitelink_score,
    CASE
      WHEN positive_geo_target_type_configured_good = 'X'
        AND negative_geo_target_type_configured_good = 'X' THEN 0.0
      WHEN positive_geo_target_type_configured_good = 'Yes'
        AND negative_geo_target_type_configured_good = 'X' THEN 0.5
      WHEN positive_geo_target_type_configured_good = 'X'
        AND negative_geo_target_type_configured_good = 'Yes' THEN 0.5
      WHEN positive_geo_target_type_configured_good = 'Yes'
        AND negative_geo_target_type_configured_good = 'Yes' THEN 1.0
    END AS geo_target_score
  FROM components
)
SELECT snapshot_date, account_id, campaign_id, campaign_name, campaign_status,
  url_expansion_opt_out, url_expansion_known, url_expansion_score,
  audience_signals_count, asset_group_count, audience_signals_score,
  sitelink_count, missing_sitelinks, sitelink_score,
  positive_geo_target_type, negative_geo_target_type,
  positive_geo_target_type_configured_good,
  negative_geo_target_type_configured_good, geo_target_score,
  CASE
    WHEN url_expansion_known THEN ROUND(
      SAFE_DIVIDE(
        url_expansion_score + audience_signals_score
          + sitelink_score + geo_target_score,
        4.0
      ),
      2
    )
    ELSE ROUND(
      SAFE_DIVIDE(
        audience_signals_score + sitelink_score + geo_target_score,
        3.0
      ),
      2
    )
  END AS campaign_bp_score,
  IF(
    url_expansion_known,
    'KNOWN',
    'PARITY_NEUTRAL_UNKNOWN'
  ) AS url_expansion_parity_mode,
  @run_id
FROM scored;
COMMIT TRANSACTION;
