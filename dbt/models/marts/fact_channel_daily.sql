-- =============================================================
-- models/marts/fact_channel_daily.sql
--
-- PURPOSE:
--   Daily channel performance fact table.
--   Tracks how each channel's metrics change over time —
--   essential for the subscriber growth trend chart in
--   Looker Studio.
--
--   Unlike dim_channel (which only keeps the latest snapshot),
--   this table keeps EVERY daily snapshot so you can plot:
--   - Subscriber growth over 30 days
--   - View count milestones
--   - Video upload frequency
--
--   Day-over-day changes are calculated using LAG() window
--   function so the dashboard can show "gained X subscribers
--   since yesterday."
--
-- SOURCE:  stg_channels
-- OUTPUT:  youtube_kenya_marts.fact_channel_daily (table)
-- GRAIN:   One row per channel per snapshot date
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
        cluster_by   = ["channel_id"]
    )
}}

WITH daily_snapshots AS (
    SELECT
        channel_id,
        channel_name,
        niche,
        subscriber_count,
        total_view_count,
        video_count,
        snapshot_date
    FROM {{ ref('stg_channels') }}
),

with_changes AS (
    SELECT
        channel_id,
        channel_name,
        niche,
        snapshot_date,
        subscriber_count,
        total_view_count,
        video_count,

        -- Day-over-day subscriber change
        subscriber_count - LAG(subscriber_count) OVER (
            PARTITION BY channel_id
            ORDER BY snapshot_date
        )                                               AS subscriber_change,

        -- Day-over-day view count change
        total_view_count - LAG(total_view_count) OVER (
            PARTITION BY channel_id
            ORDER BY snapshot_date
        )                                               AS views_added,

        -- New videos uploaded since yesterday
        video_count - LAG(video_count) OVER (
            PARTITION BY channel_id
            ORDER BY snapshot_date
        )                                               AS new_videos

    FROM daily_snapshots
)

SELECT
    -- Surrogate key
    FARM_FINGERPRINT(
        CONCAT(channel_id, CAST(snapshot_date AS STRING))
    )                                               AS daily_snapshot_id,

    channel_id,
    channel_name,
    niche,
    snapshot_date,
    subscriber_count,
    total_view_count,
    video_count,

    -- Changes (NULL on first snapshot — expected)
    COALESCE(subscriber_change, 0)                  AS subscriber_change,
    COALESCE(views_added, 0)                        AS views_added,
    COALESCE(new_videos, 0)                         AS new_videos,

    -- Growth rate percentage (day over day)
    ROUND(
        SAFE_DIVIDE(
            COALESCE(subscriber_change, 0),
            LAG(subscriber_count) OVER (
                PARTITION BY channel_id ORDER BY snapshot_date
            )
        ) * 100, 4
    )                                               AS subscriber_growth_rate_pct

FROM with_changes
