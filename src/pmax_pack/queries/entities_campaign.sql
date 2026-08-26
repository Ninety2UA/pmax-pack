SELECT
  customer.id AS account_id,
  campaign.id AS campaign_id,
  campaign.name AS campaign_name,
  campaign.status AS status,
  campaign.primary_status AS primary_status,
  campaign.primary_status_reasons AS primary_status_reasons,
  campaign.advertising_channel_type AS advertising_channel_type,
  campaign.asset_automation_settings AS asset_automation_settings,
  campaign.geo_target_type_setting.positive_geo_target_type AS positive_geo_target_type,
  campaign.geo_target_type_setting.negative_geo_target_type AS negative_geo_target_type,
  campaign.start_date_time AS start_date_time,
  campaign.end_date_time AS end_date_time,
  campaign_budget.id AS budget_id,
  campaign_budget.amount_micros AS budget_amount_micros,
  campaign_budget.explicitly_shared AS budget_explicitly_shared,
  campaign_budget.period AS budget_period
FROM campaign
WHERE campaign.advertising_channel_type = 'PERFORMANCE_MAX'
