-- Rebuild int_entities_campaign with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_campaign` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_campaign`
WITH
bounds AS (
  SELECT s.account_id, s.campaign_id,
    MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id, s.campaign_id
),
next_complete AS (
  SELECT b.account_id, b.campaign_id, b.first_seen_date, b.last_seen_date,
    MIN(c.snapshot_date) AS next_complete_date
  FROM bounds AS b
  LEFT JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    ON c.account_id = b.account_id AND c.snapshot_date > b.last_seen_date
  GROUP BY b.account_id, b.campaign_id, b.first_seen_date, b.last_seen_date
)
SELECT s.snapshot_date, s.account_id, s.campaign_id, s.campaign_name,
  s.status, s.primary_status, s.primary_status_reasons,
  s.advertising_channel_type, s.asset_automation_settings,
  s.positive_geo_target_type, s.negative_geo_target_type,
  s.start_date_time, s.end_date_time, s.budget_id, s.budget_amount_micros,
  s.budget_explicitly_shared, s.budget_period, s.url_expansion_opt_out,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date, FALSE AS inferred_removed,
  'observed' AS attribute_provenance,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_campaign` AS s
JOIN bounds AS b USING (account_id, campaign_id)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of
UNION ALL
SELECT n.next_complete_date, n.account_id, n.campaign_id,
  CAST(NULL AS STRING), 'REMOVED', 'REMOVED',
  ['ENTITY_NOT_PRESENT_ON_COMPLETE_SNAPSHOT'], CAST(NULL AS STRING),
  CAST(NULL AS ARRAY<STRUCT<asset_automation_type STRING, asset_automation_status STRING>>),
  CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS DATETIME),
  CAST(NULL AS DATETIME), CAST(NULL AS INT64), CAST(NULL AS INT64),
  CAST(NULL AS BOOL), CAST(NULL AS STRING), CAST(NULL AS BOOL),
  CAST(NULL AS DATE), CAST(NULL AS DATE), TRUE, 'inferred-removed',
  CAST(NULL AS STRING), @run_id
FROM next_complete AS n
WHERE n.next_complete_date = @as_of;
COMMIT TRANSACTION;
