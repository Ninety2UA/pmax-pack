-- Rebuild stg_entities_conversion_action with one atomic table transaction.
BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.stg_entities_conversion_action` WHERE snapshot_date = @as_of;

INSERT INTO `{{ project }}.{{ marts_dataset }}.stg_entities_conversion_action`
SELECT
  run_id, loaded_at, query_hash, account_id, conversion_action_id,
  snapshot_date, conversion_action_name, category, counting_type, status,
  click_through_lookback_window_days, view_through_lookback_window_days,
  include_in_conversions_metric, conversion_action_type
FROM `{{ project }}.{{ raw_dataset }}.entities_conversion_action`
WHERE snapshot_date = @as_of
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY snapshot_date, account_id, conversion_action_id
  ORDER BY loaded_at DESC, run_id DESC
) = 1;
COMMIT TRANSACTION;
