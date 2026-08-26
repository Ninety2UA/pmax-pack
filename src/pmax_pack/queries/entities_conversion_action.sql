SELECT
  customer.id AS account_id,
  conversion_action.id AS conversion_action_id,
  conversion_action.name AS conversion_action_name,
  conversion_action.category AS category,
  conversion_action.counting_type AS counting_type,
  conversion_action.status AS status,
  conversion_action.click_through_lookback_window_days AS click_through_lookback_window_days,
  conversion_action.view_through_lookback_window_days AS view_through_lookback_window_days,
  conversion_action.include_in_conversions_metric AS include_in_conversions_metric,
  conversion_action.type AS conversion_action_type
FROM conversion_action
