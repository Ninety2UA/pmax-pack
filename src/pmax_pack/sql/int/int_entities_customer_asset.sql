-- Rebuild int_entities_customer_asset with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_entities_customer_asset` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_entities_customer_asset`
SELECT s.snapshot_date, s.account_id, s.asset_id, s.asset_resource_name,
  s.field_type, s.status, s.primary_status, s.primary_status_reasons,
  s.source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer_asset` AS s
JOIN `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` AS c
  USING (account_id, snapshot_date)
WHERE s.snapshot_date = @as_of;
COMMIT TRANSACTION;
