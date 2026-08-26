BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_entities_conversion_action` WHERE snapshot_date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_entities_conversion_action`
SELECT snapshot_date, account_id, conversion_action_id,
  conversion_action_name, category, counting_type, status,
  click_through_lookback_window_days, view_through_lookback_window_days,
  include_in_conversions_metric, conversion_action_type, first_seen_date,
  last_seen_date, inferred_removed, attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.v_int_entities_conversion_action`
WHERE snapshot_date = @as_of;
COMMIT TRANSACTION;
