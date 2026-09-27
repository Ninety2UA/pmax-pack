-- Rebuild stg_entities_customer with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_customer` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_customer`
SELECT
  run_id, loaded_at, query_hash, account_id, snapshot_date,
  descriptive_name, currency_code, time_zone, status, manager
FROM `{{ project }}.{{ raw_dataset }}.entities_customer`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
