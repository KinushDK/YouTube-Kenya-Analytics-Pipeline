-- =============================================================
-- models/marts/dim_channel.sql
--
-- PURPOSE:
--   Channel dimension table — one row per unique channel.
--   Used by fact tables as a lookup for channel attributes.
--
--   Takes the LATEST snapshot for each channel so the
--   dimension always reflects current subscriber counts
--   and channel metadata.
--
--   This is a Type 1 SCD (Slowly Changing Dimension) —
--   we overwrite old values with new ones on each dbt run.
--   For historical tracking, use fact_channel_daily instead.
--
-- SOURCE:  stg_channels
-- OUTPUT:  youtube_kenya_marts.dim_channel (table)
-- GRAIN:   One row per channel (9 rows total)
-- =============================================================

{{
    config(
        materialized = 'table',
        schema       = 'marts'
    )
}}

WITH latest_snapshot AS (
    SELECT
        channel_id,
        channel_name,
        handle,
        niche,
        country,
        description,
        channel_created_at,
        subscriber_count,
        total_view_count,
        video_count,
        snapshot_date   AS last_updated_date,

        -- Rank channels by subscriber count for leaderboard
        RANK() OVER (ORDER BY subscriber_count DESC) AS subscriber_rank,

        -- Row number to get only the most recent snapshot
        ROW_NUMBER() OVER (
            PARTITION BY channel_id
            ORDER BY snapshot_date DESC
        ) AS row_num

    FROM {{ ref('stg_channels') }}
)

SELECT
    channel_id,
    channel_name,
    handle,
    niche,
    country,
    description,
    channel_created_at,
    subscriber_count,
    total_view_count,
    video_count,
    last_updated_date,
    subscriber_rank,

    -- Audience size tier for dashboard filtering
    CASE
        WHEN subscriber_count >= 1000000 THEN 'Mega (1M+)'
        WHEN subscriber_count >= 500000  THEN 'Large (500K+)'
        WHEN subscriber_count >= 100000  THEN 'Medium (100K+)'
        ELSE 'Small (<100K)'
    END AS audience_tier

FROM latest_snapshot
WHERE row_num = 1
