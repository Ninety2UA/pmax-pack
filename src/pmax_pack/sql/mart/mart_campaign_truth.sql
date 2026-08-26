BEGIN TRANSACTION;
DELETE FROM `{{ project }}.{{ marts_dataset }}.mart_campaign_truth` WHERE date = @as_of;
INSERT INTO `{{ project }}.{{ marts_dataset }}.mart_campaign_truth`
SELECT date, account_id, campaign_id, campaign_name, campaign_status,
  ad_network_type, network_impressions, network_clicks, network_cost,
  network_conversions, network_conversions_value, network_all_conversions,
  network_all_conversions_value, currency_code, attribute_provenance, @run_id
FROM `{{ project }}.{{ marts_dataset }}.int_performance_campaign`
WHERE date = @as_of AND metric_basis = 'NETWORK';
COMMIT TRANSACTION;
