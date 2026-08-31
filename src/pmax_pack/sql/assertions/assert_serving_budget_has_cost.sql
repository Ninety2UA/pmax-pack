WITH
campaigns AS (
  SELECT
    account_id,
    campaign_id,
    ANY_VALUE(status) AS status,
    MAX(budget_amount) AS budget_amount
  FROM `{{ project }}.{{ marts_dataset }}.mart_entities_campaign`
  WHERE snapshot_date = @as_of
    AND NOT inferred_removed
  GROUP BY account_id, campaign_id
),
cost AS (
  SELECT account_id, campaign_id, SUM(cost) AS cost
  FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
  WHERE date = @as_of
  GROUP BY account_id, campaign_id
),
violations AS (
  SELECT 1 AS violation
  FROM campaigns AS c
  LEFT JOIN cost AS p USING (account_id, campaign_id)
  WHERE c.status = 'ENABLED'
    AND c.budget_amount > 0
    AND COALESCE(p.cost, 0) = 0
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'serving campaigns with budget should not report zero cost' AS detail
FROM violations
