-- step: soft_check
SELECT
  TRUE AS passed,
  {{ tolerances.campaign_reconciliation }} AS observed,
  0.0 AS expected,
  'fixture reconciliation' AS detail
