-- =============================================================
-- models/staging/stg_videos.sql
--
-- PURPOSE:
--   Reads raw video data from BigQuery raw dataset and applies
--   light cleaning:
--   - Casts all data types correctly
--   - Parses published_at into useful time parts
--   - Removes duplicate video snapshots per day
--   - Filters out clearly invalid records (negative views)
--   - Maps category IDs to human-readable names
--   - No engagement rate calculations -- that's intermediate
--
-- SOURCE:  youtube_kenya_raw.video_snapshots
-- OUTPUT:  youtube_kenya_staging.stg_videos (view)
-- GRAIN:   One row per video per snapshot date
-- =============================================================

{{
    config(
        materialized = 'view',
        schema       = 'staging'
    )
}}

WITH raw AS (
    SELECT
        video_id,
        channel_id,
        NULLIF(TRIM(title), '')             AS title,
        NULLIF(TRIM(description), '')       AS description,
        CAST(published_at AS TIMESTAMP)     AS published_at,
        category_id,
        category_name,
        CAST(duration_seconds AS INT64)     AS duration_seconds,
        thumbnail_url,
        CAST(view_count AS INT64)           AS view_count,
        CAST(like_count AS INT64)           AS like_count,
        CAST(comment_count AS INT64)        AS comment_count,
        CAST(favorite_count AS INT64)       AS favorite_count,
        CAST(snapshot_date AS DATE)         AS snapshot_date,
        CAST(ingested_at AS TIMESTAMP)      AS ingested_at,

        -- Derived time fields for Metabase/Looker Studio grouping
        EXTRACT(HOUR FROM CAST(published_at AS TIMESTAMP))    AS published_hour,
        EXTRACT(DAYOFWEEK FROM CAST(published_at AS TIMESTAMP)) AS published_day_number,
        FORMAT_DATE(
            '%A', DATE(CAST(published_at AS TIMESTAMP))
        )                                                      AS published_day_of_week,
        DATE(CAST(published_at AS TIMESTAMP))                  AS published_date,

        -- Duration in minutes for readability
        ROUND(CAST(duration_seconds AS INT64) / 60.0, 1)      AS duration_minutes,

        -- Row number to deduplicate
        ROW_NUMBER() OVER (
            PARTITION BY video_id, snapshot_date
            ORDER BY ingested_at DESC
        ) AS row_num

    FROM {{ source('youtube_kenya_raw', 'video_snapshots') }}
    WHERE video_id    IS NOT NULL
      AND channel_id  IS NOT NULL
      AND snapshot_date IS NOT NULL
      AND CAST(view_count AS INT64) >= 0   -- remove clearly invalid rows
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
    category_id,
    category_name,
    duration_seconds,
    duration_minutes,
    thumbnail_url,
    view_count,
    like_count,
    comment_count,
    favorite_count,
    snapshot_date,
    ingested_at

FROM raw
WHERE row_num = 1   -- keep only latest ingestion per video per day
