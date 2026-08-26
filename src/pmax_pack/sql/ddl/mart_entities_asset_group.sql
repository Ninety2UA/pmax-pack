SELECT
  CAST(NULL AS DATE) AS snapshot_date,
  CAST(NULL AS INT64) AS account_id,
  CAST(NULL AS INT64) AS campaign_id,
  CAST(NULL AS INT64) AS asset_group_id,
  CAST(NULL AS STRING) AS asset_group_name,
  CAST(NULL AS STRING) AS status,
  CAST(NULL AS STRING) AS primary_status,
  CAST(NULL AS ARRAY<STRING>) AS primary_status_reasons,
  CAST(NULL AS STRING) AS ad_strength,
  CAST(NULL AS ARRAY<STRUCT<action_item_type STRING, add_asset_details STRUCT<asset_field_type STRING, asset_count INT64, video_aspect_ratio_requirement STRING>>>) AS ad_strength_action_items,
  CAST(NULL AS ARRAY<STRING>) AS final_urls,
  CAST(NULL AS DATE) AS first_seen_date,
  CAST(NULL AS DATE) AS last_seen_date,
  CAST(NULL AS BOOL) AS inferred_removed,
  CAST(NULL AS STRING) AS attribute_provenance,
  CAST(NULL AS STRING) AS run_id
LIMIT 0
