-- Rebuild int_entities_asset_group_signal with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_asset_group_signal` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_asset_group_signal`
WITH
bounds AS (
  SELECT s.account_id, s.campaign_id, s.asset_group_id,
    s.signal_resource_name, MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.campaign_id, s.asset_group_id,
    s.signal_resource_name
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.asset_group_id, b.signal_resource_name,
    b.first_seen_date, b.last_seen_date, MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.asset_group_id,
    b.signal_resource_name, b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.campaign_id, s.asset_group_id,
  s.signal_resource_name, s.approval_status, s.audience, s.search_theme,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_asset_group_signal` AS s
JOIN bounds AS b USING (account_id, campaign_id, asset_group_id, signal_resource_name)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id, n.asset_group_id,
  n.signal_resource_name, 'REMOVED', CAST(NULL AS STRING), CAST(NULL AS STRING),
  CAST(NULL AS DATE), CAST(NULL AS DATE), TRUE, 'inferred-removed',
  CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;
COMMIT TRANSACTION;
