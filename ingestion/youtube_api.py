"""
YouTube Kenya Analytics Pipeline
=================================
Ingestion script: Fetches channel and video data
from YouTube Data API v3 for 10 Kenyan channels.

Outputs:
  - Raw JSON → Google Cloud Storage (partitioned by date)
  - Structured data → BigQuery raw dataset

Usage:
  python youtube_api.py                    # fetch today's data
  python youtube_api.py --backfill 30      # fetch last 30 days
  python youtube_api.py --test             # test API key only
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

from google.cloud import storage, bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")
load_dotenv()  # also check current directory as fallback

# ─────────────────────────────────────────
# Logging
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Config from environment
# ─────────────────────────────────────────
YOUTUBE_API_KEY   = os.getenv("YOUTUBE_API_KEY")
GCP_PROJECT_ID    = os.getenv("GCP_PROJECT_ID")
GCP_BUCKET_NAME   = os.getenv("GCP_BUCKET_NAME")
BQ_DATASET_RAW    = os.getenv("BIGQUERY_DATASET_RAW", "youtube_kenya_raw")
CREDENTIALS_PATH  = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "gcp_credentials.json")

# ─────────────────────────────────────────
# Target Kenyan YouTube Channels
# ─────────────────────────────────────────
KENYA_CHANNELS = [
    {
        "name":       "Citizen TV Kenya",
        "channel_id": "UChBQgieUidXV1CmDxSdRm3g",
        "handle":     "@CitizenTVKenya",
        "niche":      "News & Media",
    },
    {
        "name":       "NTV Kenya",
        "channel_id": "UCqBJ47FjJcl61fmSbcadAVg",
        "handle":     "@NTVKenya",
        "niche":      "News & Media",
    },
    {
        "name":       "KTN News Kenya",
        "channel_id": "UCKVsdeoHExltrWMuK0hOWmg",
        "handle":     "@KTNNewsKenya",
        "niche":      "News & Media",
    },
    {
        "name":       "Churchill Show",
        "channel_id": "UC0YG5UA1s2gLb5tDfUCIWtg",
        "handle":     "@ChurchillShow",
        "niche":      "Comedy & Entertainment",
    },
    {
        "name":       "Jalango TV",
        "channel_id": "UCFG1zHs55s1my124O3Nk9DQ",
        "handle":     "@JalangoTV",
        "niche":      "Talk Show & Entertainment",
    },
    {
        "name":       "Oga Obinna",
        "channel_id": "UCe68ABxGwMZO3J8y_gerZ6A",
        "handle":     "@OgaObinna",
        "niche":      "Comedy & Entertainment",
    },
    {
        "name":       "Switch TV Kenya",
        "channel_id": "UCUhrpGr_luwUzVaxiW5Jkhw",
        "handle":     "@SwitchTVKenya",
        "niche":      "Lifestyle & Entertainment",
    },
    {
        "name":       "KBC Channel 1",
        "channel_id": "UCypNjM5hP1qcUqQZe57jNfg",
        "handle":     "@KBCChannel1",
        "niche":      "News & Media",
    },
    {
        "name":       "Willy Paul",
        "channel_id": "UCgdVgtJQXxebSiSAzlhYczw",
        "handle":     "@WillyPaulMsafi",
        "niche":      "Music & Gospel",
    },
]

# YouTube video category names
CATEGORY_MAP = {
    "1":  "Film & Animation",
    "2":  "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "How-to & Style",
    "27": "Education",
    "28": "Science & Technology",
    "29": "Nonprofits & Activism",
}


# ─────────────────────────────────────────
# GCP Clients
# ─────────────────────────────────────────
def get_gcp_credentials():
    """Loads GCP service account credentials."""
    return service_account.Credentials.from_service_account_file(
        CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def get_storage_client() -> storage.Client:
    return storage.Client(
        project=GCP_PROJECT_ID,
        credentials=get_gcp_credentials()
    )


def get_bigquery_client() -> bigquery.Client:
    return bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=get_gcp_credentials()
    )


def get_youtube_client():
    """Builds the YouTube Data API v3 client."""
    import googleapiclient.discovery
    # cache_discovery=False prevents file_cache warning
    # static_discovery=False prevents credential lookup for API key auth
    return build(
        "youtube",
        "v3",
        developerKey=YOUTUBE_API_KEY,
        cache_discovery=False,
    )


# ─────────────────────────────────────────
# YouTube API — Channel Stats
# ─────────────────────────────────────────
def fetch_channel_stats(youtube, channel: dict, snapshot_date: str) -> dict:
    """
    Fetches channel-level statistics from YouTube API.
    Uses 1 quota unit per channel.
    """
    try:
        response = youtube.channels().list(
            part="snippet,statistics,brandingSettings",
            id=channel["channel_id"]
        ).execute()

        if not response.get("items"):
            logger.warning(f"No data found for channel: {channel['name']}")
            return {}

        item       = response["items"][0]
        snippet    = item.get("snippet", {})
        statistics = item.get("statistics", {})

        return {
            "channel_id":       channel["channel_id"],
            "channel_name":     channel["name"],
            "handle":           channel["handle"],
            "niche":            channel["niche"],
            "country":          snippet.get("country", "KE"),
            "description":      snippet.get("description", "")[:500],
            "published_at":     snippet.get("publishedAt", ""),
            "subscriber_count": int(statistics.get("subscriberCount", 0)),
            "total_view_count": int(statistics.get("viewCount", 0)),
            "video_count":      int(statistics.get("videoCount", 0)),
            "snapshot_date":    snapshot_date,
            "ingested_at":      datetime.now(timezone.utc).isoformat(),
        }

    except HttpError as e:
        logger.error(f"YouTube API error for {channel['name']}: {e}")
        return {}


# ─────────────────────────────────────────
# YouTube API — Video List
# ─────────────────────────────────────────
def fetch_video_ids(youtube, channel_id: str, published_after: Optional[str] = None) -> list:
    """
    Fetches all video IDs for a channel using search.list.
    Uses 100 quota units per page — use sparingly.
    Returns list of video IDs.
    """
    video_ids = []
    page_token = None

    params = {
        "part":       "id",
        "channelId":  channel_id,
        "type":       "video",
        "order":      "date",
        "maxResults": 50,
    }

    if published_after:
        params["publishedAfter"] = published_after

    while True:
        if page_token:
            params["pageToken"] = page_token

        try:
            response = youtube.search().list(**params).execute()
            for item in response.get("items", []):
                video_ids.append(item["id"]["videoId"])

            page_token = response.get("nextPageToken")
            if not page_token:
                break

            # Respect rate limits
            time.sleep(0.5)

        except HttpError as e:
            logger.error(f"Error fetching video IDs for {channel_id}: {e}")
            break

    return video_ids


# ─────────────────────────────────────────
# YouTube API — Video Details + Stats
# ─────────────────────────────────────────
def fetch_video_details(youtube, video_ids: list, snapshot_date: str) -> list:
    """
    Fetches detailed metadata and statistics for a list of video IDs.
    Processes in batches of 50 (API maximum).
    Uses 1 quota unit per batch.
    Returns list of video records.
    """
    videos = []

    # Process in batches of 50
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]

        try:
            response = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=",".join(batch)
            ).execute()

            for item in response.get("items", []):
                snippet    = item.get("snippet", {})
                statistics = item.get("statistics", {})
                content    = item.get("contentDetails", {})

                # Parse ISO 8601 duration to seconds
                duration_seconds = parse_duration(content.get("duration", "PT0S"))

                # Get category name
                category_id   = snippet.get("categoryId", "0")
                category_name = CATEGORY_MAP.get(category_id, "Unknown")

                video_record = {
                    "video_id":        item["id"],
                    "channel_id":      snippet.get("channelId", ""),
                    "title":           snippet.get("title", ""),
                    "description":     snippet.get("description", "")[:500],
                    "published_at":    snippet.get("publishedAt", ""),
                    "category_id":     category_id,
                    "category_name":   category_name,
                    "tags":            snippet.get("tags", []),
                    "duration_seconds": duration_seconds,
                    "thumbnail_url":   snippet.get("thumbnails", {}).get("high", {}).get("url", ""),
                    "view_count":      int(statistics.get("viewCount", 0)),
                    "like_count":      int(statistics.get("likeCount", 0)),
                    "comment_count":   int(statistics.get("commentCount", 0)),
                    "favorite_count":  int(statistics.get("favoriteCount", 0)),
                    "snapshot_date":   snapshot_date,
                    "ingested_at":     datetime.now(timezone.utc).isoformat(),
                }
                videos.append(video_record)

            time.sleep(0.2)

        except HttpError as e:
            logger.error(f"Error fetching video details for batch {i}: {e}")
            continue

    return videos


# ─────────────────────────────────────────
# Helper — Parse ISO 8601 Duration
# ─────────────────────────────────────────
def parse_duration(duration: str) -> int:
    """
    Converts ISO 8601 duration (PT1H2M3S) to total seconds.
    Example: PT1H2M3S → 3723
    """
    import re
    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    match   = re.match(pattern, duration)
    if not match:
        return 0
    hours   = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


# ─────────────────────────────────────────
# GCS Upload
# ─────────────────────────────────────────
def upload_to_gcs(data: list, gcs_path: str) -> bool:
    """
    Uploads a list of records as newline-delimited JSON to GCS.
    Path example:
      channels/date=2026-08-20/all_channels.json
      videos/date=2026-08-20/channel_id=UCXxx/videos.json
    """
    if not data:
        logger.warning(f"No data to upload to {gcs_path}")
        return False

    try:
        client = get_storage_client()
        bucket = client.bucket(GCP_BUCKET_NAME)
        blob   = bucket.blob(gcs_path)

        # Newline-delimited JSON (BigQuery-friendly)
        ndjson = "\n".join(json.dumps(record) for record in data)
        blob.upload_from_string(ndjson, content_type="application/json")

        logger.info(f"✅ Uploaded {len(data)} records → gs://{GCP_BUCKET_NAME}/{gcs_path}")
        return True

    except Exception as e:
        logger.error(f"❌ GCS upload failed for {gcs_path}: {e}")
        return False


# ─────────────────────────────────────────
# BigQuery Load
# ─────────────────────────────────────────
def load_to_bigquery(gcs_uri: str, table_id: str, schema: list) -> bool:
    """
    Loads a GCS JSON file into a BigQuery table.
    Uses WRITE_APPEND so daily snapshots accumulate.
    """
    try:
        client = get_bigquery_client()

        job_config                        = bigquery.LoadJobConfig()
        job_config.source_format          = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.write_disposition      = bigquery.WriteDisposition.WRITE_APPEND
        job_config.schema                 = schema
        job_config.ignore_unknown_values  = True
        job_config.max_bad_records        = 10

        full_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.{table_id}"
        load_job      = client.load_table_from_uri(gcs_uri, full_table_id, job_config=job_config)
        load_job.result()  # Wait for completion

        table = client.get_table(full_table_id)
        logger.info(f"✅ BigQuery: {table.num_rows} total rows in {full_table_id}")
        return True

    except Exception as e:
        logger.error(f"❌ BigQuery load failed for {table_id}: {e}")
        return False


# ─────────────────────────────────────────
# BigQuery Schemas
# ─────────────────────────────────────────
CHANNEL_SCHEMA = [
    bigquery.SchemaField("channel_id",       "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("channel_name",      "STRING"),
    bigquery.SchemaField("handle",            "STRING"),
    bigquery.SchemaField("niche",             "STRING"),
    bigquery.SchemaField("country",           "STRING"),
    bigquery.SchemaField("description",       "STRING"),
    bigquery.SchemaField("published_at",      "TIMESTAMP"),
    bigquery.SchemaField("subscriber_count",  "INTEGER"),
    bigquery.SchemaField("total_view_count",  "INTEGER"),
    bigquery.SchemaField("video_count",       "INTEGER"),
    bigquery.SchemaField("snapshot_date",     "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",       "TIMESTAMP"),
]

VIDEO_SCHEMA = [
    bigquery.SchemaField("video_id",          "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("channel_id",        "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("title",             "STRING"),
    bigquery.SchemaField("description",       "STRING"),
    bigquery.SchemaField("published_at",      "TIMESTAMP"),
    bigquery.SchemaField("category_id",       "STRING"),
    bigquery.SchemaField("category_name",     "STRING"),
    bigquery.SchemaField("tags",              "STRING",    mode="REPEATED"),
    bigquery.SchemaField("duration_seconds",  "INTEGER"),
    bigquery.SchemaField("thumbnail_url",     "STRING"),
    bigquery.SchemaField("view_count",        "INTEGER"),
    bigquery.SchemaField("like_count",        "INTEGER"),
    bigquery.SchemaField("comment_count",     "INTEGER"),
    bigquery.SchemaField("favorite_count",    "INTEGER"),
    bigquery.SchemaField("snapshot_date",     "DATE",      mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",       "TIMESTAMP"),
]


# ─────────────────────────────────────────
# BigQuery Dataset Setup
# ─────────────────────────────────────────
def ensure_bigquery_datasets():
    """Creates BigQuery datasets if they don't exist."""
    client   = get_bigquery_client()
    datasets = [
        os.getenv("BIGQUERY_DATASET_RAW",     "youtube_kenya_raw"),
        os.getenv("BIGQUERY_DATASET_STAGING", "youtube_kenya_staging"),
        os.getenv("BIGQUERY_DATASET_MARTS",   "youtube_kenya_marts"),
    ]
    for dataset_id in datasets:
        full_id = f"{GCP_PROJECT_ID}.{dataset_id}"
        try:
            client.get_dataset(full_id)
            logger.info(f"✅ Dataset exists: {full_id}")
        except Exception:
            dataset = bigquery.Dataset(full_id)
            dataset.location = "US"
            client.create_dataset(dataset, exists_ok=True)
            logger.info(f"✅ Created dataset: {full_id}")


# ─────────────────────────────────────────
# Main Pipeline
# ─────────────────────────────────────────
def run_pipeline(snapshot_date: str, backfill_days: int = 0):
    """
    Main ingestion pipeline:
    1. Fetch channel stats for all 10 channels
    2. Fetch video IDs published recently
    3. Fetch video details + stats in batches
    4. Upload everything to GCS
    5. Load from GCS into BigQuery
    """
    logger.info("=" * 60)
    logger.info("🚀 YouTube Kenya Analytics Pipeline Starting")
    logger.info(f"   Snapshot date : {snapshot_date}")
    logger.info(f"   Channels      : {len(KENYA_CHANNELS)}")
    logger.info(f"   GCS bucket    : {GCP_BUCKET_NAME}")
    logger.info(f"   BQ project    : {GCP_PROJECT_ID}")
    logger.info("=" * 60)

    # Validate config
    if not YOUTUBE_API_KEY:
        raise ValueError("❌ YOUTUBE_API_KEY is not set in .env")
    if not GCP_PROJECT_ID:
        raise ValueError("❌ GCP_PROJECT_ID is not set in .env")
    if not GCP_BUCKET_NAME:
        raise ValueError("❌ GCP_BUCKET_NAME is not set in .env")

    # Ensure BigQuery datasets exist
    ensure_bigquery_datasets()

    youtube = get_youtube_client()

    # ── Step 1: Channel Stats ──────────────────────────────
    logger.info("\n📊 Step 1: Fetching channel statistics...")
    all_channel_stats = []

    for channel in KENYA_CHANNELS:
        logger.info(f"   Fetching: {channel['name']}")
        stats = fetch_channel_stats(youtube, channel, snapshot_date)
        if stats:
            all_channel_stats.append(stats)
            logger.info(
                f"   ✅ {channel['name']}: "
                f"{stats['subscriber_count']:,} subscribers, "
                f"{stats['video_count']:,} videos"
            )
        time.sleep(0.3)

    # Upload channel stats to GCS
    channel_gcs_path = f"channels/date={snapshot_date}/all_channels.json"
    upload_to_gcs(all_channel_stats, channel_gcs_path)

    # Load to BigQuery
    load_to_bigquery(
        gcs_uri  = f"gs://{GCP_BUCKET_NAME}/{channel_gcs_path}",
        table_id = "channel_snapshots",
        schema   = CHANNEL_SCHEMA,
    )

    # ── Step 2 + 3: Videos per Channel ────────────────────
    logger.info("\n🎬 Step 2+3: Fetching video metadata and stats...")

    # For backfill, fetch videos published in the last N days
    published_after = None
    if backfill_days > 0:
        cutoff_date    = datetime.now(timezone.utc) - timedelta(days=backfill_days)
        published_after = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(f"   Backfill mode: fetching videos since {published_after}")
    else:
        # Daily run: fetch videos from last 7 days
        cutoff_date    = datetime.now(timezone.utc) - timedelta(days=7)
        published_after = cutoff_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    all_videos = []

    for channel in KENYA_CHANNELS:
        logger.info(f"\n   Channel: {channel['name']}")

        # Get video IDs
        video_ids = fetch_video_ids(
            youtube,
            channel["channel_id"],
            published_after=published_after
        )
        logger.info(f"   Found {len(video_ids)} videos")

        if not video_ids:
            continue

        # Get video details + stats
        videos = fetch_video_details(youtube, video_ids, snapshot_date)
        all_videos.extend(videos)

        # Upload this channel's videos to GCS
        channel_video_path = f"videos/date={snapshot_date}/channel_id={channel['channel_id']}/videos.json"
        upload_to_gcs(videos, channel_video_path)

        logger.info(f"   ✅ {len(videos)} videos fetched and uploaded")
        time.sleep(0.5)

    # Load all videos to BigQuery
    if all_videos:
        all_videos_path = f"videos/date={snapshot_date}/all_videos.json"
        upload_to_gcs(all_videos, all_videos_path)
        load_to_bigquery(
            gcs_uri  = f"gs://{GCP_BUCKET_NAME}/{all_videos_path}",
            table_id = "video_snapshots",
            schema   = VIDEO_SCHEMA,
        )

    # ── Summary ────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("✅ Pipeline Complete!")
    logger.info(f"   Channels processed : {len(all_channel_stats)}")
    logger.info(f"   Videos processed   : {len(all_videos)}")
    logger.info(f"   Snapshot date      : {snapshot_date}")
    logger.info(f"   GCS bucket         : gs://{GCP_BUCKET_NAME}/")
    logger.info(f"   BigQuery dataset   : {GCP_PROJECT_ID}.{BQ_DATASET_RAW}")
    logger.info("=" * 60)


# ─────────────────────────────────────────
# API Key Test
# ─────────────────────────────────────────
def test_api_connection():
    """Quick test to verify YouTube API key and GCP credentials work."""
    logger.info("🧪 Testing YouTube API connection...")

    youtube  = get_youtube_client()
    channel  = KENYA_CHANNELS[0]
    response = youtube.channels().list(
        part="snippet,statistics",
        id=channel["channel_id"]
    ).execute()

    if response.get("items"):
        item  = response["items"][0]
        stats = item["statistics"]
        logger.info(f"✅ YouTube API working!")
        logger.info(f"   Channel : {channel['name']}")
        logger.info(f"   Subscribers: {int(stats.get('subscriberCount', 0)):,}")
        logger.info(f"   Total views: {int(stats.get('viewCount', 0)):,}")
    else:
        logger.error("❌ No data returned — check channel ID or API key")
        return False

    logger.info("\n🧪 Testing GCP connection...")
    try:
        client = get_storage_client()
        bucket = client.bucket(GCP_BUCKET_NAME)
        bucket.reload()
        logger.info(f"✅ GCS bucket accessible: gs://{GCP_BUCKET_NAME}/")
    except Exception as e:
        logger.error(f"❌ GCS connection failed: {e}")
        return False

    logger.info("\n🧪 Testing BigQuery connection...")
    try:
        client = get_bigquery_client()
        client.list_datasets()
        logger.info(f"✅ BigQuery accessible: project={GCP_PROJECT_ID}")
    except Exception as e:
        logger.error(f"❌ BigQuery connection failed: {e}")
        return False

    logger.info("\n🚀 All checks passed! Ready to run the pipeline.")
    return True


# ─────────────────────────────────────────
# Entry Point
# ─────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="YouTube Kenya Analytics Pipeline")
    parser.add_argument("--test",     action="store_true", help="Test API connections only")
    parser.add_argument("--backfill", type=int, default=0, help="Backfill N days of history")
    parser.add_argument("--date",     type=str, default=None, help="Snapshot date YYYY-MM-DD")
    args = parser.parse_args()

    snapshot_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if args.test:
        test_api_connection()
    else:
        run_pipeline(snapshot_date=snapshot_date, backfill_days=args.backfill)
