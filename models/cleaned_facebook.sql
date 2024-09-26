{{ config(materialized='table') }}  

with raw_data as (
    select *
    from {{ ref('src_ads_creative_facebook_all_data') }}  
)

SELECT 
    ad_id,
    add_to_cart,
    adset_id,
    campaign_id,
    channel,
    clicks,
    comments,
    creative_id,
    date,
    impressions,
    mobile_app_install,
    likes,
    inline_link_clicks,
    (clicks + likes + comments + shares + views) AS engagements,
    ROUND(CASE WHEN clicks > 0 THEN purchase / clicks
            ELSE 0
        END, 2) AS post_click_conversions,
    ROUND(CASE WHEN views > 0 THEN purchase / views
            ELSE 0
        END, 2) AS post_view_conversions,
    purchase,
    complete_registration AS registrations,
    purchase_value AS revenue,
    shares,
    spend,
    purchase AS total_conversions
from raw_data



    
    
    