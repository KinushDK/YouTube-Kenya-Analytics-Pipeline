-- =============================================================
-- models/intermediate/int_video_engagement.sql
--
-- PURPOSE:
--   Computes engagement metrics for every video snapshot.
--   This is the business logic layer -- sits between staging
--   (clean raw data) and marts (analytics-ready tables).
--
--   Engagement rate = (likes + comments) / views
--   Like rate       = likes / views
--   Comment rate    = comments / views
--
--   SAFE_DIVIDE is used throughout to avoid division by zero
--   on videos with 0 views (newly published or unlisted).
--
-- SOURCE:  stg_videos
-- OUTPUT:  youtube_kenya_staging.int_video_engagement (view)
-- GRAIN:   One row per video per snapshot date
-- =============================================================

{{
    config(
        materialized = 'view',
        schema       = 'staging'
    )
}}

SELECT
    video_id,
    channel_id,
    title,
    published_date,
    published_hour,
    published_day_of_week,
    category_name,
    duration_minutes,
    snapshot_date,
    view_count,
    like_count,
    comment_count,

    -- Engagement metrics
    ROUND(
        SAFE_DIVIDE(like_count, view_count) * 100, 4
    )                                               AS like_rate_pct,

    ROUND(
        SAFE_DIVIDE(comment_count, view_count) * 100, 4
    )                                               AS comment_rate_pct,

    ROUND(
        SAFE_DIVIDE(like_count + comment_count, view_count) * 100, 4
    )                                               AS engagement_rate_pct,

    -- Performance tiers based on engagement rate
    CASE
        WHEN SAFE_DIVIDE(like_count + comment_count, view_count) >= 0.05
            THEN 'VIRAL'
        WHEN SAFE_DIVIDE(like_count + comment_count, view_count) >= 0.02
            THEN 'HIGH'
        WHEN SAFE_DIVIDE(like_count + comment_count, view_count) >= 0.005
            THEN 'MEDIUM'
        ELSE 'LOW'
    END                                             AS engagement_tier,

    -- View performance tiers
    CASE
        WHEN view_count >= 1000000 THEN 'MEGA'
        WHEN view_count >= 100000  THEN 'VIRAL'
        WHEN view_count >= 10000   THEN 'HIGH'
        WHEN view_count >= 1000    THEN 'MEDIUM'
        ELSE 'LOW'
    END                                             AS view_tier,

    -- Is it a short video (YouTube Shorts = under 60 seconds)
    CASE
        WHEN duration_minutes < 1 THEN TRUE
        ELSE FALSE
    END                                             AS is_short

FROM {{ ref('stg_videos') }}
WHERE view_count > 0   -- only include videos with at least 1 view
