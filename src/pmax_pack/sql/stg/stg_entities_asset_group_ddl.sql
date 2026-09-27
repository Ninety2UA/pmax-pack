SELECT
  CAST(NULL AS STRING) AS source_run_id,
  CAST(NULL AS TIMESTAMP) AS loaded_at,
  CAST(NULL AS STRING) AS query_hash,
  CAST(NULL AS INT64) AS account_id,
  CAST(NULL AS INT64) AS campaign_id,
  CAST(NULL AS INT64) AS asset_group_id,
  CAST(NULL AS DATE) AS snapshot_date,
  CAST(NULL AS STRING) AS asset_group_name,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS STRING) AS primary_status,
  CAST(NULL AS ARRAY<STRING>) AS primary_status_reasons,
  CAST(NULL AS STRING) AS ad_strength,
  CAST(NULL AS ARRAY<STRUCT<action_item_type STRING, add_asset_details STRUCT<asset_field_type STRING, asset_count INT64, video_aspect_ratio_requirement STRING>>>) AS ad_strength_action_items,
  CAST(NULL AS ARRAY<STRING>) AS final_urls
LIMIT 0
