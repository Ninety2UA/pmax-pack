WITH bounds AS (
  SELECT e.account_id, e.campaign_id, e.asset_group_id, e.asset_id,
    e.field_type, MIN(e.snapshot_date) AS first_seen_date,
    MAX(e.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.int_entities_asset` AS e
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE NOT e.inferred_removed
  GROUP BY e.account_id, e.campaign_id, e.asset_group_id, e.asset_id,
    e.field_type
)
SELECT e.* REPLACE (
  b.first_seen_date AS first_seen_date,
  b.last_seen_date AS last_seen_date
)
FROM `{{ project }}.{{ marts_dataset }}.int_entities_asset` AS e
JOIN bounds AS b
  USING (account_id, campaign_id, asset_group_id, asset_id, field_type)
