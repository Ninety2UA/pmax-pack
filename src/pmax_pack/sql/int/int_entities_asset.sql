-- Rebuild int_entities_asset with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_asset` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_asset`
WITH
observed AS (
  SELECT l.snapshot_date, l.account_id, l.campaign_id, l.asset_group_id,
    l.asset_id, l.field_type, l.status, l.primary_status,
    l.primary_status_reasons, l.source, a.asset_name, a.asset_type,
    a.orientation, a.text, a.image_url, a.image_height_pixels,
    a.image_width_pixels, a.video_id, a.video_title, l.source_run_id
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_asset` AS l
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.stg_entities_asset` AS a
    USING (snapshot_date, account_id, campaign_id, asset_group_id, asset_id)
),
bounds AS (
  SELECT account_id, campaign_id, asset_group_id, asset_id, field_type,
    MIN(snapshot_date) AS first_seen_date,
    MAX(snapshot_date) AS last_seen_date
  FROM observed
  WHERE snapshot_date <= @as_of
  GROUP BY account_id, campaign_id, asset_group_id, asset_id, field_type
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.asset_group_id, b.asset_id,
    b.field_type,
    b.first_seen_date, b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.asset_group_id, b.asset_id,
    b.field_type,
    b.first_seen_date, b.last_seen_date
)
SELECT o.snapshot_date, o.account_id, o.campaign_id, o.asset_group_id,
  o.asset_id, o.field_type, o.status, o.primary_status,
  o.primary_status_reasons, o.source, o.asset_name, o.asset_type,
  o.orientation, o.text, o.image_url, o.image_height_pixels,
  o.image_width_pixels, o.video_id, o.video_title,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  o.source_run_id, @run_id
FROM observed AS o
JOIN bounds AS b
  USING (account_id, campaign_id, asset_group_id, asset_id, field_type)
WHERE o.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id, n.asset_group_id,
  n.asset_id, n.field_type, 'REMOVED', 'REMOVED',
  ['ENTITY_NOT_PRESENT_ON_COMPLETE_SNAPSHOT'], CAST(NULL AS STRING),
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING),
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS INT64),
  CAST(NULL AS INT64), CAST(NULL AS STRING), CAST(NULL AS STRING),
  CAST(NULL AS DATE), CAST(NULL AS DATE), TRUE, 'inferred-removed',
  CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;
COMMIT TRANSACTION;
