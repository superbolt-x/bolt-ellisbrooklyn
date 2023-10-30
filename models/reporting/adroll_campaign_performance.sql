{{ config (
    alias = target.database + '_adroll_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_type = 'prospecting' THEN 'Campaign Type: Prospecting'
    WHEN campaign_type = 'retargeting' THEN 'Campaign Type: Retargeting'
END as campaign_type_default,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
view_through_revenue+click_through_revenue as revenue
FROM {{ ref('adroll_performance_by_campaign') }} 
