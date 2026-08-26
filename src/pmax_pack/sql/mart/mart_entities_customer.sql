BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_entities_customer` WHERE snapshot_date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_entities_customer`
SELECT snapshot_date, account_id, descriptive_name, currency_code,
  time_zone, status, manager, first_seen_date, last_seen_date,
  attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.v_int_entities_customer`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
