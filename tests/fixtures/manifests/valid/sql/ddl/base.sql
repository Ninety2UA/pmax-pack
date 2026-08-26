-- step: base
SELECT
  CAST(NULL AS DATE) AS event_date,
  CAST(1 AS INT64) AS account_id,
  CAST(NULL AS STRING) AS run_id
WHERE FALSE
