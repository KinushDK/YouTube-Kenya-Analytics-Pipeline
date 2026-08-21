-- =============================================================
-- models/staging/stg_channels.sql
--
-- PURPOSE:
--   Reads raw channel data from BigQuery raw dataset and
--   applies light cleaning only:
--   - Casts data types correctly
--   - Standardises column names
--   - Removes duplicate snapshots per channel per day
--   - No business logic here -- that belongs in marts
--
-- SOURCE:  youtube_kenya_raw.channel_snapshots
-- OUTPUT:  youtube_kenya_staging.stg_channels (view)
-- GRAIN:   One row per channel per snapshot date
-- =============================================================

{{
    config(
        materialized = 'view',
        schema       = 'staging'
    )
}}

WITH raw AS (
    SELECT
        channel_id,
        channel_name,
        handle,
        niche,
        country,
        description,
        CAST(published_at AS TIMESTAMP)    AS channel_created_at,
        CAST(subscriber_count AS INT64)    AS subscriber_count,
        CAST(total_view_count AS INT64)    AS total_view_count,
        CAST(video_count AS INT64)         AS video_count,
        CAST(snapshot_date AS DATE)        AS snapshot_date,
        CAST(ingested_at AS TIMESTAMP)     AS ingested_at,

        -- Row number to deduplicate multiple runs on same day
        ROW_NUMBER() OVER (
            PARTITION BY channel_id, snapshot_date
            ORDER BY ingested_at DESC
        ) AS row_num

    FROM {{ source('youtube_kenya_raw', 'channel_snapshots') }}
    WHERE channel_id IS NOT NULL
      AND snapshot_date IS NOT NULL
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
    snapshot_date,
    ingested_at

FROM raw
WHERE row_num = 1   -- keep only latest ingestion per channel per day
