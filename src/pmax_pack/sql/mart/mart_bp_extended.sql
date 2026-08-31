CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.mart_bp_extended` (
  snapshot_date DATE,
  account_id INT64,
  campaign_id INT64,
  asset_group_id INT64,
  google_parity_score FLOAT64,
  ad_strength_action_item_count INT64,
  action_item_clear_score FLOAT64,
  asset_primary_status_reason_count INT64,
  primary_status_clear_score FLOAT64,
  covered_feed_type_count INT64,
  feed_type_coverage_score FLOAT64,
  text_guidelines_present BOOL,
  text_guidelines_present_score FLOAT64,
  extended_score FLOAT64,
  run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_group_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_bp_extended`
WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_bp_extended`
WITH
asset_detail AS (
  SELECT snapshot_date, account_id, campaign_id, asset_group_id,
    SUM(ARRAY_LENGTH(COALESCE(primary_status_reasons, [])))
      AS asset_primary_status_reason_count,
    COUNT(DISTINCT IF(
      field_type IN (
        'HEADLINE', 'LONG_HEADLINE', 'DESCRIPTION', 'MARKETING_IMAGE',
        'SQUARE_MARKETING_IMAGE', 'PORTRAIT_MARKETING_IMAGE', 'LOGO',
        'YOUTUBE_VIDEO'
      ),
      field_type,
      NULL
    )) AS covered_feed_type_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset`
  WHERE snapshot_date = @as_of
    AND status = 'ENABLED'
    AND source = 'ADVERTISER'
    AND NOT COALESCE(inferred_removed, FALSE)
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id
),
components AS (
  SELECT b.snapshot_date, b.account_id, b.campaign_id, b.asset_group_id,
    b.asset_group_bp_score AS google_parity_score,
    ARRAY_LENGTH(COALESCE(g.ad_strength_action_items, []))
      AS ad_strength_action_item_count,
    IF(
      ARRAY_LENGTH(COALESCE(g.ad_strength_action_items, [])) = 0,
      1.0,
      0.0
    ) AS action_item_clear_score,
    COALESCE(a.asset_primary_status_reason_count, 0)
      AS asset_primary_status_reason_count,
    IF(COALESCE(a.asset_primary_status_reason_count, 0) = 0, 1.0, 0.0)
      AS primary_status_clear_score,
    COALESCE(a.covered_feed_type_count, 0) AS covered_feed_type_count,
    SAFE_DIVIDE(COALESCE(a.covered_feed_type_count, 0), 8.0)
      AS feed_type_coverage_score,
    b.count_short_headlines > 0
      AND b.count_long_headlines > 0
      AND b.count_descriptions > 0 AS text_guidelines_present,
    IF(
      b.count_short_headlines > 0
        AND b.count_long_headlines > 0
        AND b.count_descriptions > 0,
      1.0,
      0.0
    ) AS text_guidelines_present_score
  FROM `{{ project }}.{{ marts_dataset }}.mart_bp_asset_group` AS b
  JOIN `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group` AS g
    USING (snapshot_date, account_id, campaign_id, asset_group_id)
  LEFT JOIN asset_detail AS a
    USING (snapshot_date, account_id, campaign_id, asset_group_id)
  WHERE b.snapshot_date = @as_of
)
SELECT snapshot_date, account_id, campaign_id, asset_group_id,
  google_parity_score, ad_strength_action_item_count,
  action_item_clear_score, asset_primary_status_reason_count,
  primary_status_clear_score, covered_feed_type_count,
  feed_type_coverage_score, text_guidelines_present,
  text_guidelines_present_score,
  ROUND(
    SAFE_DIVIDE(
      action_item_clear_score + primary_status_clear_score
        + feed_type_coverage_score + text_guidelines_present_score,
      4.0
    ),
    2
  ) AS extended_score,
  @run_id
FROM components;
COMMIT TRANSACTION;
