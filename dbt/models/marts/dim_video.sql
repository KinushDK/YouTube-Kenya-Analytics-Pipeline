-- =============================================================
-- models/marts/dim_video.sql
--
-- PURPOSE:
--   Video dimension table — one row per unique video.
--   Contains video metadata that rarely changes:
--   title, category, published date, duration.
--
--   Stats (views, likes, comments) are NOT here —
--   those change daily and belong in fact_video_performance.
--
--   This separation follows the star schema pattern:
--   dim_video answers "what is this video?"
--   fact_video_performance answers "how did it perform?"
--
-- SOURCE:  stg_videos
-- OUTPUT:  youtube_kenya_marts.dim_video (table)
-- GRAIN:   One row per unique video_id
-- =============================================================

{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

WITH latest AS (
    SELECT
        video_id,
        channel_id,
        title,
        description,
        published_at,
        published_date,
        published_hour,
        published_day_number,
        published_day_of_week,
        category_id,
        category_name,
        duration_seconds,
        duration_minutes,
        thumbnail_url,

        -- Label the time of day when video was published
        CASE
            WHEN published_hour BETWEEN 6  AND 11 THEN 'Morning (6am-12pm)'
            WHEN published_hour BETWEEN 12 AND 17 THEN 'Afternoon (12pm-6pm)'
            WHEN published_hour BETWEEN 18 AND 21 THEN 'Evening (6pm-10pm)'
            ELSE 'Night (10pm-6am)'
        END AS publish_time_of_day,

        -- Is it a weekend upload?
        CASE
            WHEN published_day_number IN (1, 7) THEN TRUE
            ELSE FALSE
        END AS is_weekend_upload,

        -- Video length category
        CASE
            WHEN duration_minutes < 1   THEN 'Short (<1 min)'
            WHEN duration_minutes < 5   THEN 'Brief (1-5 min)'
            WHEN duration_minutes < 15  THEN 'Standard (5-15 min)'
            WHEN duration_minutes < 30  THEN 'Long (15-30 min)'
            ELSE 'Extended (30+ min)'
        END AS duration_category,

        ROW_NUMBER() OVER (
            PARTITION BY video_id
            ORDER BY ingested_at DESC
        ) AS row_num

    FROM {{ ref('stg_videos') }}
)

SELECT
    video_id,
    channel_id,
    title,
    description,
    published_at,
    published_date,
    published_hour,
    published_day_number,
    published_day_of_week,
    publish_time_of_day,
    is_weekend_upload,
    category_id,
    category_name,
    duration_seconds,
    duration_minutes,
    duration_category,
    thumbnail_url

FROM latest
WHERE row_num = 1
