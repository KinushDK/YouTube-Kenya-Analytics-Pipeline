"""
YouTube Kenya Analytics Pipeline — Airflow DAG
================================================
Orchestrates daily ingestion + dbt transformation:

Task Order:
    pipeline_start
          ↓
    fetch_youtube_data       ← YouTube API → GCS → BigQuery raw
          ↓
    dbt_run_staging          ← flatten raw tables → staging views
          ↓
    dbt_run_marts            ← build fact + dim tables (Gold layer)
          ↓
    dbt_test                 ← data quality checks
          ↓
    notify_success           ← log record counts
          ↓
    pipeline_end

Schedule: Daily at 06:00 EAT (03:00 UTC)
"""

from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Default DAG arguments
# ─────────────────────────────────────────
default_args = {
    "owner":            "youtube_kenya_pipeline",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 8, 20),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ─────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────
with DAG(
    dag_id="youtube_kenya_analytics_pipeline",
    default_args=default_args,
    description="Daily YouTube Kenya analytics — GCS → BigQuery → dbt",
    schedule_interval="0 3 * * *",   # 03:00 UTC = 06:00 EAT daily
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "kenya", "gcp", "bigquery", "dbt"],
) as dag:

    # ─────────────────────────────────────
    # TASK 1 — Pipeline Start
    # ─────────────────────────────────────
    pipeline_start = EmptyOperator(
        task_id="pipeline_start",
    )

    # ─────────────────────────────────────
    # TASK 2 — Fetch YouTube Data
    # Runs ingestion script inside container
    # YouTube API → GCS → BigQuery raw
    # ─────────────────────────────────────
    fetch_youtube_data = BashOperator(
        task_id="fetch_youtube_data",
        bash_command="""
            echo "📺 Fetching YouTube data for {{ ds }}..."
            docker exec youtube_ingestion python /app/youtube_api.py \
                --date {{ ds }}
            echo "✅ YouTube data fetch complete"
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # ─────────────────────────────────────
    # TASK 3 — dbt Run Staging
    # Flattens raw BigQuery tables into
    # clean staging views
    # ─────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="""
            echo "🔧 Running dbt staging models..."
            cd /opt/airflow/dags/../dbt && \
            dbt run \
                --select staging \
                --profiles-dir /opt/airflow/dags/../dbt \
                --project-dir /opt/airflow/dags/../dbt
            echo "✅ dbt staging complete"
        """,
        execution_timeout=timedelta(minutes=15),
    )

    # ─────────────────────────────────────
    # TASK 4 — dbt Run Marts
    # Builds fact + dimension tables
    # in BigQuery marts dataset (Gold layer)
    # ─────────────────────────────────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="""
            echo "🏆 Running dbt mart models (Gold layer)..."
            cd /opt/airflow/dags/../dbt && \
            dbt run \
                --select marts \
                --profiles-dir /opt/airflow/dags/../dbt \
                --project-dir /opt/airflow/dags/../dbt
            echo "✅ dbt marts complete"
        """,
        execution_timeout=timedelta(minutes=15),
    )

    # ─────────────────────────────────────
    # TASK 5 — dbt Test
    # Runs all data quality tests:
    # unique, not_null, accepted_range
    # ─────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            echo "🧪 Running dbt data quality tests..."
            cd /opt/airflow/dags/../dbt && \
            dbt test \
                --profiles-dir /opt/airflow/dags/../dbt \
                --project-dir /opt/airflow/dags/../dbt
            echo "✅ All dbt tests passed"
        """,
        execution_timeout=timedelta(minutes=10),
    )

    # ─────────────────────────────────────
    # TASK 6 — Log Pipeline Summary
    # Queries BigQuery and logs record counts
    # per layer for monitoring
    # ─────────────────────────────────────
    def log_pipeline_summary(**context):
        """Queries BigQuery and logs summary of records per layer."""
        from google.cloud import bigquery
        from google.oauth2 import service_account

        project_id      = os.getenv("GCP_PROJECT_ID")
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        try:
            credentials = service_account.Credentials.from_service_account_file(
                credentials_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            client = bigquery.Client(project=project_id, credentials=credentials)

            queries = {
                "Raw — channel_snapshots":  f"SELECT COUNT(*) FROM `{project_id}.youtube_kenya_raw.channel_snapshots`",
                "Raw — video_snapshots":    f"SELECT COUNT(*) FROM `{project_id}.youtube_kenya_raw.video_snapshots`",
            }

            logger.info("=" * 55)
            logger.info("📊 PIPELINE SUMMARY")
            logger.info("=" * 55)

            for label, sql in queries.items():
                try:
                    result = client.query(sql).result()
                    count  = list(result)[0][0]
                    logger.info(f"   {label}: {count:,} records")
                except Exception as e:
                    logger.warning(f"   {label}: could not query ({e})")

            logger.info("=" * 55)
            logger.info("✅ Pipeline run complete for {{ ds }}")

        except Exception as e:
            logger.error(f"❌ Could not connect to BigQuery: {e}")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=log_pipeline_summary,
        provide_context=True,
    )

    # ─────────────────────────────────────
    # TASK 7 — Pipeline End
    # ─────────────────────────────────────
    pipeline_end = EmptyOperator(
        task_id="pipeline_end",
    )

    # ─────────────────────────────────────
    # TASK DEPENDENCIES
    # ─────────────────────────────────────
    (
        pipeline_start
        >> fetch_youtube_data
        >> dbt_run_staging
        >> dbt_run_marts
        >> dbt_test
        >> notify_success
        >> pipeline_end
    )
