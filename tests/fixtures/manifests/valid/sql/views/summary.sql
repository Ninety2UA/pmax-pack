-- step: summary
SELECT
  event_date,
  account_id
FROM `{{ project }}.{{ marts_dataset }}.transform`
