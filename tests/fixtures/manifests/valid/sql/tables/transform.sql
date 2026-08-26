-- step: transform
-- write_mode: {{ write_mode }}
SELECT
  event_date,
  account_id,
  @run_id AS run_id
FROM `{{ project }}.{{ marts_dataset }}.base`
WHERE event_date = @as_of
