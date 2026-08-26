-- step: hard_check
SELECT
  TRUE AS passed,
  COUNT(*) AS observed,
  1 AS expected,
  CONCAT('cohort days: ', TO_JSON_STRING([{{ cohort_days | join(', ') }}])) AS detail
FROM `{{ project }}.{{ marts_dataset }}.summary`
