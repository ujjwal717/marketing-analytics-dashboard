-- 1) SQL queries for Exploration and Analysis


-- Best platform by ROI for each channel type

WITH best_channel AS (

SELECT channel_type, platform_name, 
(SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd) * 100 AS roi, -- calculating ROI

DENSE_RANK() OVER(PARTITION BY channel_type ORDER BY (SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd) * 100 DESC) AS ranking -- Ranking platforms by ROI across channel type

FROM ad_metrics_daily -- table storing daily aggregations of ad metrics
JOIN ads USING (ad_id) -- bridge table between ad_metrics_daily and ad_groups
JOIN ad_groups AS adg USING (ad_group_id) -- further connection to marketing campaigns 
JOIN marketing_campaigns AS mcg USING (campaign_id) -- connection for marketing channels 
JOIN marketing_channels AS mch USING (channel_id) -- table storing channel type, platforms 

GROUP BY channel_type, platform_name) -- grouped by channel_type and platform to get ROI based on this grouping

SELECT channel_type, platform_name, round(roi::NUMERIC,2) AS roi -- Using CTE to fetch platforms with best ROI per channel type
FROM best_channel
WHERE ranking = 1
ORDER BY roi DESC


-- Best campaign by ROI for each platform

WITH best_campaign AS (

SELECT platform_name, campaign_name,
((SUM(platform_revenue) - SUM(platform_cost))/SUM(platform_cost)) * 100 AS roi, -- calculating ROI

DENSE_RANK() OVER(PARTITION BY platform_name ORDER BY ((SUM(platform_revenue) - SUM(platform_cost))/SUM(platform_cost)) * 100 DESC) AS ranking -- Ranking campaigns by ROI across platform

FROM fct_platform_campaign

GROUP BY platform_name, campaign_name) -- grouped by platform_name and campaign_name to get ROI based on this grouping

SELECT platform_name, campaign_name, round(roi::NUMERIC,2) AS roi -- Using CTE to fetch best campaigns by ROI for each marketing platform
FROM best_campaign
WHERE ranking = 1
ORDER BY roi DESC



-- Which Creative Type and Copy Variant has highest CTR:

WITH best_ctr_creative_copy_variant AS (

SELECT creative_type, copy_variant, SUM(impressions) AS total_impressions, -- calculated various aggregations
SUM(clicks) AS total_clicks, 
SUM(conversions) AS total_conversions, 
SUM(cost_usd) AS total_cost, 
SUM(revenue_usd) AS total_revenue, 

(SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd) * 100 AS roi, -- calculated roi, roas, ctr
SUM(revenue_usd)/SUM(cost_usd) AS roas, 
SUM(clicks)::FLOAT/SUM(impressions)::FLOAT * 100 AS ctr, 

DENSE_RANK() OVER(PARTITION BY creative_type ORDER BY SUM(clicks)::FLOAT/SUM(impressions)::FLOAT * 100 DESC) AS ranking -- ranked copy variant by ctr for each creative type

FROM ad_metrics_daily AS amd 
JOIN ads AS ads USING (ad_id) -- performed necessary joins
JOIN ad_groups AS adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY creative_type, copy_variant)


SELECT * 
FROM best_ctr_creative_copy_variant
WHERE ranking = 1 -- filtered best copy variant by ctr for each creative type



-- Which Call to Action performed best in CTR for each creative type:

WITH best_ctr_creative_cta AS (

SELECT creative_type, call_to_action, SUM(impressions) AS total_impressions, -- calculated various aggregations

SUM(clicks) AS total_clicks, 
SUM(conversions) AS total_conversions, 
SUM(cost_usd) AS total_cost, 
SUM(revenue_usd) AS total_revenue, 

(SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd) * 100 AS roi, -- calculated roi, roas, ctr
SUM(revenue_usd)/SUM(cost_usd) AS roas, 
SUM(clicks)::FLOAT/SUM(impressions)::FLOAT * 100 AS ctr, 

DENSE_RANK() OVER(PARTITION BY creative_type ORDER BY SUM(clicks)::FLOAT/SUM(impressions)::FLOAT * 100 DESC) AS ranking -- ranked call to action by ctr for each creative type

FROM ad_metrics_daily AS amd 
JOIN ads AS ads USING (ad_id) -- performed necessary joins
JOIN ad_groups AS adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY creative_type, call_to_action) -- grouped by creative type and call to action


SELECT * 
FROM best_ctr_creative_cta

WHERE ranking = 1 -- fetched best call to action for each creative type by ctr



-- Best channel_type in terms of ROI for each campaign objective

WITH best_channel_by_roi_for_objective AS (

SELECT objective, channel_type, 

ROUND(((SUM(revenue_usd)::NUMERIC - SUM(cost_usd)::NUMERIC)/SUM(cost_usd)::NUMERIC)*100,2) AS ROI, -- calculated ROI

DENSE_RANK() OVER(PARTITION BY OBJECTIVE ORDER BY (SUM(revenue_usd) - SUM(cost_usd)/SUM(cost_usd))*100 DESC) AS ranking -- ranked channel by roi for campaign objective

FROM ad_metrics_daily AS amd

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) -- performed necessary joins 
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY objective, channel_type) -- grouped by objective and channel_type to get accurate results

SELECT objective, channel_type, roi

FROM best_channel_by_roi_for_objective
WHERE ranking = 1 -- fetched best channel for each campaign objective by roi
ORDER BY roi DESC





-- Months with Lower 2025 Channel Performance vs 2024 in ROI by marketing channels

WITH roi_2024 AS (

SELECT EXTRACT(MONTH FROM date) AS "month",channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2024 -- calculated roi for each month in 2024 

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) -- performed necessary joins 
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2024 -- filtered to get only year = 2024

GROUP BY EXTRACT (MONTH FROM date), channel_id, platform_name),  -- grouped by month, marketing channel id and name

roi_2025 AS (

SELECT EXTRACT(MONTH FROM date) AS "month", 
channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2025 -- calculated roi for each month in 2025

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) -- performed necessary joins
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2025 -- filtered to get only year = 2025

GROUP BY EXTRACT(MONTH FROM date), channel_id, platform_name -- grouped by month, marketing channel id and name
), 

increase_decrease AS (
SELECT r_2025."month", platform_name, roi_2024, 

CASE WHEN roi_2025 IS NULL THEN 0 ELSE roi_2025 END AS roi_2025, -- handled months where we don't have ROI for 2025 as year hasn't passed yet

CASE WHEN roi_2025 IS NOT NULL THEN ((roi_2025 - roi_2024)/roi_2024) * 100 ELSE 0 END AS percentage_increase_decrease -- calculated percentage increase/decrease

FROM roi_2024 AS r_2024
LEFT JOIN roi_2025 AS r_2025 USING (channel_id,"month",platform_name)) -- performed left join to get all the data from roi_2024 CTE


SELECT platform_name, "month", percentage_increase_decrease AS roi_percentage_decrease

FROM increase_decrease 
WHERE percentage_increase_decrease < 0 -- fetched months where the roi_percentage is less than 0 (2025 performed lower than 2024 by ROI)
ORDER BY roi_percentage_decrease DESC





-- Months with higher 2025 Channel Performance vs 2024 in ROI by marketing channels

WITH roi_2024 AS (

SELECT EXTRACT(MONTH FROM date) AS "month",channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2024 -- calculated roi for each month in 2024 

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) -- performed necessary joins
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2024 -- filtered to get only year = 2024

GROUP BY EXTRACT (MONTH FROM date), channel_id, platform_name), -- grouped by month, marketing channel id and name

roi_2025 AS (

SELECT EXTRACT(MONTH FROM date) AS "month", 
channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2025 -- calculated roi for each month in 2025

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) -- performed necessary joins
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2025 -- filtered to get only year = 2025

GROUP BY EXTRACT(MONTH FROM date), channel_id, platform_name -- grouped by month, marketing channel id and name
), 

increase_decrease AS (
SELECT r_2025."month", platform_name, roi_2024, 

CASE WHEN roi_2025 IS NULL THEN 0 ELSE roi_2025 END AS roi_2025, -- handled months where we don't have ROI for 2025 as year hasn't passed yet

CASE WHEN roi_2025 IS NOT NULL THEN ((roi_2025 - roi_2024)/roi_2024) * 100 ELSE 0 END AS percentage_increase_decrease -- calculated percentage increase/decrease

FROM roi_2024 AS r_2024
LEFT JOIN roi_2025 AS r_2025 USING (channel_id,"month",platform_name)) -- performed left join to get all the data from roi_2024 CTE


SELECT platform_name, "month", percentage_increase_decrease AS roi_percentage_increase

FROM increase_decrease 
WHERE percentage_increase_decrease > 0 -- fetched months where the roi_percentage is more than 0 (2025 performed higher than 2024 by ROI)
ORDER BY roi_percentage_increase DESC




-- Quarters with higher 2025 Channel Performance vs 2024 in ROI by marketing channels

WITH roi_2024 AS (

SELECT EXTRACT(QUARTER FROM date) AS "quarter",channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2024 -- calculated roi for each quarter in 2024

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) --Performed necessary joins 
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2024 -- filtered to get only year = 2024

GROUP BY EXTRACT (QUARTER FROM date), channel_id, platform_name), -- grouped by quarter, marketing channel id and name

roi_2025 AS (

SELECT EXTRACT(QUARTER FROM date) AS "quarter", 
channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2025 -- calculated roi for each quarter onth in 2025

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) --Performed necessary joins 
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2025 -- filtered to get only year = 2025

GROUP BY EXTRACT(QUARTER FROM date), channel_id, platform_name -- grouped by quarter, marketing channel id and name
), 

increase_decrease AS (
SELECT r_2025."quarter", platform_name, roi_2024, 

CASE WHEN roi_2025 IS NULL THEN 0 ELSE roi_2025 END AS roi_2025, -- handled quarters nths where we don't have ROI for 2025 as year hasn't passed yet

CASE WHEN roi_2025 IS NOT NULL THEN ((roi_2025 - roi_2024)/roi_2024) * 100 ELSE 0 END AS percentage_increase_decrease -- calculated percentage increase/decrease

FROM roi_2024 AS r_2024
LEFT JOIN roi_2025 AS r_2025 USING (channel_id,"quarter",platform_name)) -- performed left join to get all the data from roi_2024 CTE


SELECT platform_name, "quarter", percentage_increase_decrease AS roi_percentage_increase

FROM increase_decrease 
WHERE percentage_increase_decrease > 0 -- fetched quarters where the roi_percentage is more than 0 (2025 performed higher than 2024 by ROI)
ORDER BY roi_percentage_increase DESC



-- Quarters with lower 2025 Channel Performance vs 2024 in ROI by marketing channels

WITH roi_2024 AS (

SELECT EXTRACT(QUARTER FROM date) AS "quarter",channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2024 -- calculated roi for each quarter in 2024

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) -- performed necessary joins
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2024 -- filtered to get year = 2024

GROUP BY EXTRACT (QUARTER FROM date), channel_id, platform_name), -- grouped by quarter, marketing channel and name

roi_2025 AS (

SELECT EXTRACT(QUARTER FROM date) AS "quarter", 
channel_id, platform_name, 

((SUM(revenue_usd) - SUM(cost_usd))/SUM(cost_usd)) * 100 AS roi_2025 -- calculated roi for each quarter in 2025

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_groups AS adg USING (ad_group_id) -- performed necessary joins 
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

WHERE EXTRACT(YEAR FROM date) = 2025 -- filtered to get year = 2025

GROUP BY EXTRACT(QUARTER FROM date), channel_id, platform_name -- grouped by quarter, marketing channel and name
), 

increase_decrease AS (
SELECT r_2025."quarter", platform_name, roi_2024, 

CASE WHEN roi_2025 IS NULL THEN 0 ELSE roi_2025 END AS roi_2025, -- handled quarters nths where we don't have ROI for 2025 as year hasn't passed yet

CASE WHEN roi_2025 IS NOT NULL THEN ((roi_2025 - roi_2024)/roi_2024) * 100 ELSE 0 END AS percentage_increase_decrease -- calculated percentage increase/decrease

FROM roi_2024 AS r_2024
LEFT JOIN roi_2025 AS r_2025 USING (channel_id,"quarter",platform_name)) -- performed left join to get all the data from roi_2024 CTE


SELECT platform_name, "quarter", percentage_increase_decrease AS roi_percentage_decrease

FROM increase_decrease 
WHERE percentage_increase_decrease < 0 -- fetched quarters where the roi_percentage is less  than 0 (2025 performed high lower than 2024 by ROI)
ORDER BY roi_percentage_decrease





-- 2) Below are the queries for Views generated to use in Power BI for dashboard building:


-- DIMESNION TABLES



-- dates level dimension table materialized views:

CREATE MATERIALIZED VIEW dim_date AS (
SELECT DISTINCT date 
FROM ad_metrics_daily)



-- user type level dimension table materialized view:

CREATE MATERIALIZED VIEW dim_user_type AS (

WITH categorized_audience AS(
SELECT *,
CASE WHEN audience_name LIKE '%Free%' THEN 'Free Users' ELSE 'Paid Users' END AS user_type
FROM audiences )

SELECT DISTINCT user_type 
FROM categorized_audience)



-- region level dimension table materialized view:

CREATE MATERIALIZED VIEW dim_region AS (

WITH categorized_audience AS(
SELECT *,
CASE WHEN audience_name LIKE '%Free%' THEN 'free_user' ELSE 'paid_users' END AS user_type
FROM audiences )

SELECT DISTINCT region 
FROM categorized_audience)



-- marketing platform level dimension table materialized view:

CREATE MATERIALIZED VIEW dim_platform AS (
SELECT DISTINCT channel_id, platform_name
FROM marketing_channels)



-- Campaign level dimension table materialized view:

CREATE MATERIALIZED VIEW dim_campaigns AS (

SELECT DISTINCT CAMPAIGN_ID, campaign_name
FROM MARKETING_CAMPAIGNS)



-- Objective level dimension table materialized view:

CREATE MATERIALIZED VIEW dim_objective
AS (
SELECT DISTINCT objective
FROM marketing_campaigns)



-- Targeting type level dimension table materialized view:


CREATE MATERIALIZED VIEW dim_targeting_type AS (

SELECT DISTINCT targeting_type
FROM ad_groups)


-- Bridge dimension table for campaigns, channels, targeting type, and objective materialized view:

CREATE MATERIALIZED VIEW dim_bridge_campaign AS (
SELECT channel_id, platform_name, campaign_id, campaign_name, objective, targeting_type
FROM ad_groups AS adg 
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id))



-- FACT TABLES:


--Platform aggregations {Fact Table} Materialized View along with user type and region:

CREATE MATERIALIZED VIEW fct_platform AS ( 
WITH categorized_audience AS(
SELECT *,
CASE WHEN audience_name LIKE '%Free%' THEN 'Free User' ELSE 'Paid User' END AS user_type
FROM audiences )

SELECT date, channel_id, platform_name, user_type, region, SUM(impressions) AS platform_impressions, 
SUM(clicks) AS platform_clicks,
SUM(conversions) AS platform_conversions, 
SUM(cost_usd) AS platform_cost, 
SUM(revenue_usd) AS platform_revenue

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_audience_mapping AS aam USING (ad_id)
JOIN categorized_audience AS cad USING (audience_id)
JOIN ad_groups adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY date, channel_id, platform_name, user_type, region)



-- Overall aggregations {Fact Table} Materialized Views along with user type and region:

CREATE MATERIALIZED VIEW fct_overall AS (
WITH categorized_audience AS(
SELECT *,
CASE WHEN audience_name LIKE '%Free%' THEN 'Free Users' ELSE 'Paid Users' END AS user_type
FROM audiences )

SELECT date, user_type, region, 
SUM(impressions) AS overall_impressions, 
SUM(clicks) AS overall_clicks,
SUM(conversions) AS overall_conversions, 
SUM(cost_usd) AS overall_cost, 
SUM(revenue_usd) AS overall_revenue

FROM ad_metrics_daily 

JOIN ad_audience_mapping AS aam USING (ad_id)
JOIN categorized_audience AS cad USING (audience_id)

GROUP BY date, user_type, region)



-- Platform Campaign level aggregation {FACT TABLE} MATERIALIZED VIEW:

CREATE MATERIALIZED VIEW fct_platform_campaign AS (

WITH categorized_audience_region AS (
SELECT audience_id, region, CASE WHEN audience_name LIKE '%Free%' THEN 'Free Users' ELSE 'Paid Users' END AS user_type
FROM audiences)

SELECT date, channel_id, campaign_id, campaign_name, objective, platform_name, user_type, region, targeting_type, SUM(impressions) AS platform_impressions, 
SUM(clicks) AS platform_clicks,
SUM(conversions) AS platform_conversions, 
SUM(cost_usd) AS platform_cost, 
SUM(revenue_usd) AS platform_revenue

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_audience_mapping AS aam USING (ad_id)
JOIN categorized_audience_region AS cad USING (audience_id)
JOIN ad_groups adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY date, channel_id, platform_name, campaign_id, campaign_name, objective, user_type, targeting_type, region)



-- Created fact conversion funnel and saved it as a MATERIALZIED VIEW:


CREATE MATERIALIZED VIEW fact_conversion_funnel AS (

SELECT date, channel_id, campaign_id, campaign_name, objective, platform_name, region, targeting_type,
'funnel_impressions' AS conversion_funnel,
SUM(impressions) AS VALUES

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_audience_mapping AS aam USING (ad_id)
JOIN audiences AS aud USING (audience_id)
JOIN ad_groups adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY date, channel_id, platform_name, campaign_id, campaign_name, objective, targeting_type, region

UNION ALL 

SELECT date, channel_id, campaign_id, campaign_name, objective, platform_name, region, targeting_type,
'funnel_clicks' AS conversion_funnel,
SUM(clicks) AS VALUES

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_audience_mapping AS aam USING (ad_id)
JOIN audiences AS aud USING (audience_id)
JOIN ad_groups adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY date, channel_id, platform_name, campaign_id, campaign_name, objective, targeting_type, region

UNION ALL 


SELECT date, channel_id, campaign_id, campaign_name, objective, platform_name, region, targeting_type,
'funnel_conversions' AS conversion_funnel,
SUM(conversions) AS VALUES

FROM ad_metrics_daily AS amd 

JOIN ads USING (ad_id)
JOIN ad_audience_mapping AS aam USING (ad_id)
JOIN audiences AS aud USING (audience_id)
JOIN ad_groups adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY date, channel_id, platform_name, campaign_id, campaign_name, objective, targeting_type, region)




-- CREATIVE LEVEL FACT TABLE MATERIALIZED VIEW:


CREATE MATERIALIZED VIEW fact_creative AS (
SELECT date, channel_id,platform_name,campaign_id, campaign_name, region, objective, creative_type, call_to_action, resolution , 
SUM(impressions) AS total_impressions, 
SUM(clicks) AS total_clicks, 
SUM(conversions) AS total_conversions, 
SUM(cost_usd) AS total_cost, 
SUM(revenue_usd) AS total_revenue 

FROM ad_metrics_daily AS amd 
JOIN creative_assets AS cas USING (ad_id)
JOIN ads AS ads USING (ad_id)
JOIN ad_audience_mapping AS aam USING (ad_id)
JOIN audiences AS aud USING (audience_id)
JOIN ad_groups AS adg USING (ad_group_id)
JOIN marketing_campaigns AS mcg USING (campaign_id)
JOIN marketing_channels AS mch USING (channel_id)

GROUP BY date, channel_id, platform_name, campaign_name, campaign_id, creative_type, region, objective, call_to_action, resolution)
