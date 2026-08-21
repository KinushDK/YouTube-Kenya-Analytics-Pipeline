-- =============================================================
-- models/marts/fact_video_performance.sql
--
-- PURPOSE:
--   Central fact table — the heart of the star schema.
--   One row per video per snapshot date, containing all
--   performance metrics and engagement calculations.
--
--   This table answers the key business questions:
--   - Which videos got the most views this week?
--   - Which channel has the highest engagement rate?
--   - What category performs best?
--   - Does posting time affect performance?
--
--   It joins to dimensions for context:
--   - dim_channel  (who made this video?)
--   - dim_video    (what is this video?)
--
-- SOURCE:  int_video_engagement + dim_channel + dim_video
-- OUTPUT:  youtube_kenya_marts.fact_video_performance (table)
-- GRAIN:   One row per video per snapshot date
-- =============================================================

{{
    config(
        materialized = 'table',
        schema       = 'marts',
        partition_by = {
            "field": "snapshot_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by   = ["channel_id", "category_name"]
    )
}}

SELECT
    -- Surrogate key
    FARM_FINGERPRINT(
        CONCAT(e.video_id, CAST(e.snapshot_date AS STRING))
    )                                               AS snapshot_id,

    -- Foreign keys to dimensions
    e.video_id,
    e.channel_id,
    e.snapshot_date,

    -- Channel context (denormalized for query performance)
    c.channel_name,
    c.niche,
    c.subscriber_rank,
    c.audience_tier,

    -- Video context
    v.title,
    v.published_date,
    v.published_day_of_week,
    v.publish_time_of_day,
    v.is_weekend_upload,
    v.category_name,
    v.duration_minutes,
    v.duration_category,

    -- Raw performance metrics
    e.view_count,
    e.like_count,
    e.comment_count,

    -- Engagement metrics (from intermediate model)
    e.like_rate_pct,
    e.comment_rate_pct,
    e.engagement_rate_pct,
    e.engagement_tier,
    e.view_tier,
    e.is_short,

    -- Views per subscriber (reach efficiency)
    ROUND(
        SAFE_DIVIDE(e.view_count, c.subscriber_count) * 100, 4
    )                                               AS views_per_subscriber_pct

FROM {{ ref('int_video_engagement') }} e
LEFT JOIN {{ ref('dim_channel') }} c
    ON c.channel_id = e.channel_id
LEFT JOIN {{ ref('dim_video') }} v
    ON v.video_id = e.video_id
