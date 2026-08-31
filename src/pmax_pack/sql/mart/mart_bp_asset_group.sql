CREATE TABLE IF NOT EXISTS `{{ project }}.{{ marts_dataset }}.mart_bp_asset_group` (
  snapshot_date DATE,
  account_id INT64,
  campaign_id INT64,
  campaign_name STRING,
  campaign_status STRING,
  asset_group_id INT64,
  asset_group_name STRING,
  asset_group_status STRING,
  ad_strength STRING,
  count_images INT64,
  count_logos INT64,
  count_landscape INT64,
  count_square INT64,
  count_portrait INT64,
  count_square_logos INT64,
  count_landscape_logos INT64,
  count_headlines INT64,
  count_short_headlines INT64,
  count_long_headlines INT64,
  count_descriptions INT64,
  count_short_descriptions INT64,
  count_videos INT64,
  is_video_uploaded STRING,
  audience_signals STRING,
  video_score FLOAT64,
  text_score FLOAT64,
  image_score FLOAT64,
  asset_group_bp_score FLOAT64,
  run_id STRING
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id, asset_group_id;

BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_bp_asset_group`
WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_bp_asset_group`
WITH
asset_counts AS (
  SELECT snapshot_date, account_id, campaign_id, asset_group_id,
    COUNTIF(asset_type = 'IMAGE') AS count_images,
    COUNTIF(field_type = 'LOGO') AS count_logos,
    COUNTIF(field_type = 'MARKETING_IMAGE') AS count_landscape,
    COUNTIF(field_type = 'SQUARE_MARKETING_IMAGE') AS count_square,
    COUNTIF(field_type = 'PORTRAIT_MARKETING_IMAGE') AS count_portrait,
    COUNTIF(field_type = 'LOGO' AND image_width_pixels = image_height_pixels)
      AS count_square_logos,
    COUNTIF(
      field_type = 'LOGO'
      AND ROUND(SAFE_DIVIDE(image_width_pixels, image_height_pixels), 2) = 4
    ) AS count_landscape_logos,
    COUNTIF(field_type IN ('HEADLINE', 'LONG_HEADLINE')) AS count_headlines,
    COUNTIF(field_type = 'HEADLINE') AS count_short_headlines,
    COUNTIF(field_type = 'LONG_HEADLINE') AS count_long_headlines,
    COUNTIF(field_type = 'DESCRIPTION') AS count_descriptions,
    COUNTIF(field_type = 'DESCRIPTION' AND LENGTH(text) <= 60)
      AS count_short_descriptions,
    COUNTIF(asset_type = 'YOUTUBE_VIDEO') AS count_videos,
    COUNTIF(
      asset_type = 'YOUTUBE_VIDEO'
      AND video_id IS NOT NULL
      AND video_id != ''
    ) > 0 AS has_uploaded_video
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset`
  WHERE snapshot_date = @as_of
    AND status = 'ENABLED'
    AND source = 'ADVERTISER'
    AND NOT COALESCE(inferred_removed, FALSE)
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id
),
audience_signals AS (
  SELECT snapshot_date, account_id, campaign_id, asset_group_id,
    COUNT(DISTINCT audience) AS audience_signal_count
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group_signal`
  WHERE snapshot_date = @as_of
    AND audience IS NOT NULL
    AND NOT COALESCE(inferred_removed, FALSE)
  GROUP BY snapshot_date, account_id, campaign_id, asset_group_id
),
components AS (
  SELECT g.snapshot_date, g.account_id, g.campaign_id, c.campaign_name,
    c.status AS campaign_status, g.asset_group_id, g.asset_group_name,
    g.status AS asset_group_status, g.ad_strength,
    COALESCE(a.count_images, 0) AS count_images,
    COALESCE(a.count_logos, 0) AS count_logos,
    COALESCE(a.count_landscape, 0) AS count_landscape,
    COALESCE(a.count_square, 0) AS count_square,
    COALESCE(a.count_portrait, 0) AS count_portrait,
    COALESCE(a.count_square_logos, 0) AS count_square_logos,
    COALESCE(a.count_landscape_logos, 0) AS count_landscape_logos,
    COALESCE(a.count_headlines, 0) AS count_headlines,
    COALESCE(a.count_short_headlines, 0) AS count_short_headlines,
    COALESCE(a.count_long_headlines, 0) AS count_long_headlines,
    COALESCE(a.count_descriptions, 0) AS count_descriptions,
    COALESCE(a.count_short_descriptions, 0) AS count_short_descriptions,
    COALESCE(a.count_videos, 0) AS count_videos,
    IF(COALESCE(a.has_uploaded_video, FALSE), 'Yes', 'X')
      AS is_video_uploaded,
    IF(s.audience_signal_count IS NOT NULL, 'Yes', 'X') AS audience_signals
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_asset_group` AS g
  JOIN `{{ project }}.{{ marts_dataset }}.mart_entities_campaign` AS c
    USING (snapshot_date, account_id, campaign_id)
  LEFT JOIN asset_counts AS a
    USING (snapshot_date, account_id, campaign_id, asset_group_id)
  LEFT JOIN audience_signals AS s
    USING (snapshot_date, account_id, campaign_id, asset_group_id)
  WHERE g.snapshot_date = @as_of
    AND NOT COALESCE(g.inferred_removed, FALSE)
    AND NOT COALESCE(c.inferred_removed, FALSE)
),
scored AS (
  SELECT *,
    IF(count_videos < 3, 0.0, 1.0) AS video_score,
    SAFE_DIVIDE(
      IF(count_descriptions >= 4, 1.0, 0.0)
        + IF(count_headlines >= 11, 1.0, 0.0)
        + IF(count_long_headlines >= 2, 1.0, 0.0),
      3.0
    ) AS text_score,
    SAFE_DIVIDE(
      IF(count_landscape >= 4, 1.0, 0.0)
        + IF(count_square >= 4, 1.0, 0.0)
        + IF(count_portrait >= 4, 1.0, 0.0)
        + IF(count_landscape_logos >= 1, 1.0, 0.0)
        + IF(count_square_logos >= 1, 1.0, 0.0),
      5.0
    ) AS image_score
  FROM components
)
SELECT snapshot_date, account_id, campaign_id, campaign_name, campaign_status,
  asset_group_id, asset_group_name, asset_group_status, ad_strength,
  count_images, count_logos, count_landscape, count_square, count_portrait,
  count_square_logos, count_landscape_logos, count_headlines,
  count_short_headlines, count_long_headlines, count_descriptions,
  count_short_descriptions, count_videos, is_video_uploaded,
  audience_signals, video_score, text_score, image_score,
  ROUND(SAFE_DIVIDE(video_score + text_score + image_score, 3.0), 2)
    AS asset_group_bp_score,
  @run_id
FROM scored;
COMMIT TRANSACTION;
