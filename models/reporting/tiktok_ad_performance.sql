{{ config (
    alias = target.database + '_tiktok_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
adgroup_name,
adgroup_id,
adgroup_status,
CASE WHEN adgroup_name ~* 'Ulta' THEN 'Ulta'
    WHEN adgroup_name ~* 'Sephora' THEN 'Sephora'
END as adgroup_type_custom,
audience,
ad_name,
ad_id,
ad_status,
visual,
date,
date_granularity,
cost as spend,
impressions,
clicks,
complete_payment_events as purchases,
complete_payment_value as revenue
FROM {{ ref('tiktok_performance_by_ad') }}
