-- Rebuild int_entities_campaign_asset with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_campaign_asset` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_campaign_asset`
WITH
bounds AS (
  SELECT s.account_id, s.campaign_id, s.asset_resource_name, s.field_type,
    MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.campaign_id, s.asset_resource_name, s.field_type
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.asset_resource_name, b.field_type,
    b.first_seen_date, b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.asset_resource_name, b.field_type,
    b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.campaign_id, s.asset_id,
  s.asset_resource_name, s.field_type, s.status, s.primary_status,
  s.primary_status_reasons, CAST(NULL AS DATE), CAST(NULL AS DATE),
  FALSE, 'observed', s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign_asset` AS s
JOIN bounds AS b USING (account_id, campaign_id, asset_resource_name, field_type)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id, CAST(NULL AS INT64),
  n.asset_resource_name, n.field_type, 'REMOVED', 'REMOVED',
  ['ENTITY_NOT_PRESENT_ON_COMPLETE_SNAPSHOT'], CAST(NULL AS DATE),
  CAST(NULL AS DATE), TRUE, 'inferred-removed', CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;
COMMIT TRANSACTION;
