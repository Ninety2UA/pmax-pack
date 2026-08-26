WITH bounds AS (
  SELECT e.account_id, MIN(e.snapshot_date) AS first_seen_date,
    MAX(e.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.int_entities_customer` AS e
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  GROUP BY e.account_id
)
SELECT e.* REPLACE (
  b.first_seen_date AS first_seen_date,
  b.last_seen_date AS last_seen_date
)
FROM `{{ project }}.{{ marts_dataset }}.int_entities_customer` AS e
JOIN bounds AS b USING (account_id)
