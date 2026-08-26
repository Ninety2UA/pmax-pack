WITH counts AS (
  SELECT
    (SELECT COUNT(*)
     FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
     WHERE date = @as_of) AS observed,
    (SELECT COUNT(*)
     FROM `{{ project }}.{{ marts_dataset }}.stg_volume_campaign`
     WHERE date = @as_of) AS expected
)
SELECT
  observed = expected AS passed,
  observed,
  expected,
  'campaign truth row count must equal staged campaign-network grain' AS detail
FROM counts
