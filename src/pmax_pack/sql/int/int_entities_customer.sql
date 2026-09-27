-- Rebuild int_entities_customer with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_customer` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_customer`
WITH bounds AS (
  SELECT s.account_id, MIN(s.snapshot_date) AS first_seen_date,
    MAX(s.snapshot_date) AS last_seen_date
  FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer` AS s
  JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
    USING (account_id, snapshot_date)
  WHERE s.snapshot_date <= @as_of
  GROUP BY s.account_id
)
SELECT s.snapshot_date, s.account_id, s.descriptive_name, s.currency_code,
  s.time_zone, s.status, s.manager, CAST(NULL AS DATE), CAST(NULL AS DATE),
  'observed', s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer` AS s
JOIN bounds AS b USING (account_id)
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of;
COMMIT TRANSACTION;
