{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
CASE WHEN campaign_name ~* 'Ulta' THEN 'Ulta'
    WHEN campaign_name ~* 'Sephora' THEN 'Sephora'
    ELSE 'DTC'
END as campaign_type_custom,
CASE WHEN campaign_name ~* '- US' THEN 'US'
WHEN campaign_name ~* '- CA' THEN 'CA'
END as region,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue
FROM {{ ref('facebook_performance_by_ad') }}
