WITH observed_by_day AS (
  SELECT
    date,
    COUNT(*) AS observed
  FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY date
), expected_by_day AS (
  SELECT
    date,
    COUNT(*) AS expected
  FROM `{{ project }}.{{ marts_dataset }}.stg_volume_campaign`
  WHERE date BETWEEN DATE_SUB(@as_of, INTERVAL {{ window_days }} DAY) AND @as_of
  GROUP BY date
), violations AS (
  SELECT COALESCE(o.date, e.date) AS date
  FROM observed_by_day AS o
  FULL OUTER JOIN expected_by_day AS e USING (date)
  WHERE COALESCE(o.observed, 0) != COALESCE(e.expected, 0)
)
SELECT
  COUNT(*) = 0 AS passed,
  COUNT(*) AS observed,
  0 AS expected,
  'campaign truth row count must equal staged campaign-network grain per click day' AS detail
FROM violations
