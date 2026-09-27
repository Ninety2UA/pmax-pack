-- Rebuild int_complete_snapshot_days with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.int_complete_snapshot_days`
SELECT account_id, snapshot_date, source_run_id, @run_id
FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
