{{ config (
    alias = target.database + '_blended'
)}}

(SELECT 'Facebook - DTC' as channel, date, date_granularity, 
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(link_clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting', 'facebook_ad_performance') }} 
    WHERE campaign_type_custom = 'DTC'
    GROUP BY channel, date, date_granularity)
    
    UNION ALL
    
    (SELECT 'Facebook - Ulta' as channel, date, date_granularity, 
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(link_clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting', 'facebook_ad_performance') }} 
    WHERE campaign_type_custom = 'Ulta'
    GROUP BY channel, date, date_granularity)
    
    UNION ALL
    
    (SELECT 'Facebook - Sephora' as channel, date, date_granularity, 
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(link_clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting', 'facebook_ad_performance') }} 
    WHERE campaign_type_custom = 'Sephora'
    GROUP BY channel, date, date_granularity)
    
    UNION ALL
    
    (SELECT 'TikTok - Ulta' as channel, date, date_granularity, 
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting', 'tiktok_ad_performance') }} 
    WHERE adgroup_type_custom = 'Ulta'
    GROUP BY channel, date, date_granularity)
    
    UNION ALL
    
    (SELECT 'TikTok - Sephora' as channel, date, date_granularity, 
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting', 'tiktok_ad_performance') }}
    WHERE adgroup_type_custom = 'Sephora'
    GROUP BY channel, date, date_granularity)
    
    UNION ALL
    
    (SELECT 'Adroll' as channel, date, date_granularity, 
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting', 'adroll_campaign_performance') }}
    GROUP BY channel, date, date_granularity)
    
    /*UNION ALL
    
    (SELECT 'Google' as channel, date, date_granularity, 
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM reporting.ellisbrooklyn_googleads_campaign_performance
    WHERE campaign_type_custom = 'DTC'
    GROUP BY channel, date, date_granularity)*/
