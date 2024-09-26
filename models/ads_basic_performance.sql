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

SELECT ad_id,
    add_to_cart,
    adset_id,
    campaign_id,
    channel,
    clicks,
    comments,
    creative_id,
    date,
    impressions,
    mobile_app_install AS installs,
    likes,
    inline_link_clicks AS link_clicks,
    NULL AS placement_id,
    engagements,
    post_click_conversions,
    post_view_conversions,
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

SELECT ad_id,
    NULL AS add_to_cart,
    adset_id,
    campaign_id,
    channel,
    clicks,
    NULL AS comments,
    NULL AS creative_id,
    date,
    imps AS impressions,
    NULL AS installs,   
    NULL AS likes,      
    NULL AS link_clicks,
    NULL AS placement_id,
    NULL AS engagements,
    ROUND(CASE WHEN clicks > 0 THEN conv/clicks ELSE 0 END, 2) AS post_click_conversions,
    NULL AS post_view_conversions,
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

SELECT ad_id,
    add_to_cart,
    adgroup_id AS adset_id,
    campaign_id,
    channel,
    clicks,
    NULL AS comments,
    NULL AS creative_id,
    date,
    impressions,
    (rt_installs + skan_app_install) AS installs,
    NULL AS likes,      
    NULL AS link_clicks,
    NULL AS placement_id,
    NULL AS engagements,
    ROUND(CASE WHEN clicks > 0 THEN (conversions + COALESCE(skan_conversion, 0))/clicks ELSE 0 END, 2) AS post_click_conversions,
    ROUND(CASE WHEN video_views > 0 THEN (conversions + COALESCE(skan_conversion, 0))/video_views ELSE 0 END, 2) AS post_view_conversions,
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

SELECT campaign_id AS ad_id,
    NULL AS add_to_cart,
    NULL AS adset_id,
    campaign_id,
    channel,
    clicks,
    comments,
    NULL AS creative_id,
    date,
    impressions,
    NULL AS installs,   
    likes,
    url_clicks AS link_clicks,
    NULL AS placement_id,
    engagements,
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

