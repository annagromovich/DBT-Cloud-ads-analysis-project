WITH facebook_data AS (
    SELECT *
    FROM {{ ref('cleaned_facebook') }} 
),
bing_data AS (
    SELECT *
    FROM {{ ref('src_ads_bing_all_data') }}  
),
tiktok_data AS (
    SELECT *
    FROM {{ ref('src_ads_tiktok_ads_all_data') }} 
),
twitter_data AS (
    SELECT *
    FROM {{ ref('src_promoted_tweets_twitter_all_data') }} 
)

SELECT 
    CAST(ad_id AS STRING) AS ad_id,
    add_to_cart,
    CAST(adset_id AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    clicks,
    comments,
    CAST(creative_id AS STRING) AS creative_id,
    date,
    engagements,
    impressions,
    mobile_app_install AS installs,
    likes,
    inline_link_clicks AS link_clicks,
    CAST(NULL AS STRING) AS placement_id,
    CAST(CASE WHEN clicks > 0 THEN conv/clicks ELSE 0 END AS INT64) AS post_click_conversions,
    CAST(post_view_conversions AS INT64) AS post_view_conversions,
    NULL AS posts,
    purchase,
    registrations,
    revenue,
    shares,
    spend,
    total_conversions,
    NULL AS video_views
FROM facebook_data

UNION ALL

SELECT 
    CAST(ad_id AS STRING) AS ad_id,
    NULL AS add_to_cart,
    CAST(adset_id AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    clicks,
    NULL AS comments,
    CAST(NULL AS STRING) AS creative_id,
    date,
    NULL AS engagements,
    imps AS impressions,
    NULL AS installs,   
    NULL AS likes,      
    NULL AS link_clicks,
    CAST(NULL AS STRING) AS placement_id,
    CAST(CASE WHEN clicks > 0 THEN conv/clicks ELSE 0 END AS INT64) AS post_click_conversions,
    CAST(NULL AS INT64) AS post_view_conversions,
    NULL AS posts,
    NULL AS purchase,
    NULL AS registrations,
    revenue,
    NULL AS shares,
    spend,
    conv AS total_conversions,
    NULL AS video_views
FROM bing_data

UNION ALL

SELECT 
    CAST(ad_id AS STRING) AS ad_id,
    add_to_cart,
    CAST(adgroup_id AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    clicks,
    NULL AS comments,
    CAST(NULL AS STRING) AS creative_id,
    date,
    NULL AS engagements,
    impressions,
    (rt_installs + skan_app_install) AS installs,
    NULL AS likes,      
    NULL AS link_clicks,
    CAST(NULL AS STRING) AS placement_id,
    CAST(CASE WHEN clicks > 0 THEN (conversions + COALESCE(skan_conversion, 0))/clicks ELSE 0 END AS INT64) AS post_click_conversions,
    CAST(CASE WHEN video_views > 0 THEN (conversions + COALESCE(skan_conversion, 0))/video_views ELSE 0 END AS INT64) AS post_view_conversions,
    NULL AS posts,
    purchase,
    registrations,
    NULL AS revenue,    
    NULL AS shares,     
    spend,
    (conversions + COALESCE(skan_conversion, 0)) AS total_conversions,
    video_views
FROM tiktok_data

UNION ALL

SELECT 
    CAST(campaign_id AS STRING) AS ad_id,
    NULL AS add_to_cart,
    CAST(NULL AS STRING) AS adset_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    channel,
    clicks,
    comments,
    CAST(NULL AS STRING) AS creative_id,
    date,
    engagements,
    impressions,
    NULL AS installs,   
    likes,
    url_clicks AS link_clicks,
    CAST(NULL AS STRING) AS placement_id,
    NULL AS post_click_conversions,
    NULL AS post_view_conversions,
    NULL AS posts,
    NULL AS purchase,
    NULL AS registrations,
    NULL AS revenue,
    retweets AS shares,
    spend,
    NULL AS total_conversions,
    video_total_views AS video_views
FROM twitter_data


