SELECT
  campaign.id
FROM campaign
WHERE segments.date DURING LAST_30_DAYS
