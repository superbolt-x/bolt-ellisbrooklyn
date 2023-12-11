{{ config (
    alias = target.database + '_adroll_performance'
)}}

SELECT DATE_TRUNC('day',date) as date, 'day' as date_granularity,
spend,
impressions,
clicks,
purchases,
revenue
FROM {{ source('gsheet_raw','adroll_daily_performance') }} 

UNION ALL

SELECT DATE_TRUNC('week',date) as date, 'week' as date_granularity,
spend,
impressions,
clicks,
purchases,
revenue
FROM {{ source('gsheet_raw','adroll_daily_performance') }} 

UNION ALL

SELECT DATE_TRUNC('month',date) as date, 'month' as date_granularity,
spend,
impressions,
clicks,
purchases,
revenue
FROM {{ source('gsheet_raw','adroll_daily_performance') }} 

UNION ALL

SELECT DATE_TRUNC('quarter',date) as date, 'quarter' as date_granularity,
spend,
impressions,
clicks,
purchases,
revenue
FROM {{ source('gsheet_raw','adroll_daily_performance') }} 

UNION ALL

SELECT DATE_TRUNC('year',date) as date, 'year' as date_granularity,
spend,
impressions,
clicks,
purchases,
revenue
FROM {{ source('gsheet_raw','adroll_daily_performance') }} 
