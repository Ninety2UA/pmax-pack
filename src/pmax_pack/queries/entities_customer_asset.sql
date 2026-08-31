SELECT
  customer.id AS account_id,
  asset.id AS asset_id,
  customer_asset.asset AS asset_resource_name,
  customer_asset.field_type AS field_type,
  customer_asset.status AS status,
  customer_asset.primary_status AS primary_status,
  customer_asset.primary_status_reasons AS primary_status_reasons
FROM customer_asset
