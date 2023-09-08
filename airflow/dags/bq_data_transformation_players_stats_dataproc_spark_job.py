import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
    DataprocCreateClusterOperator,
)
from google.cloud import storage
from nba_api.stats.endpoints import playergamelogs, teamyearbyyearstats, teamgamelogs
from nba_api.stats.static import teams
from nba_api.stats.library.parameters import (
    SeasonTypeAllStar,
    PerModeSimple,
    SeasonNullable,
)
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

parquet_file = "players.parquet"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "nba_stats_all")
REGION = "us-south1"
DATAPROC_CLUSTER = os.getenv("GCP_DATAPROC_CLUSTER", "dataproc")

TEMP_STORAGE_PATH = os.getenv("TEMP_STORAGE_PATH", "not-found")
SOURCES_PATH = os.getenv("SOURCES_PATH", "not-found")
source_file_name = "/process_year_tables.py"
source_path_file = SOURCES_PATH + source_file_name
gcs_path_file = f"sources{source_file_name}"
gcs_source_uri = f"gs://{BUCKET}/{gcs_path_file}"

# Getting current season first game date
teamlogdata = teamgamelogs.TeamGameLogs(season_nullable=SeasonNullable.current_season)
teamlogdata = teamlogdata.team_game_logs.get_data_frame()
start_date = pd.to_datetime(teamlogdata["GAME_DATE"])
start_date = start_date.min().to_pydatetime()


default_args = {
    "owner": "airflow",
    "start_date": start_date,
    "depends_on_past": False,
    "retries": 0,
}


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "secondary_worker_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 1024,
        },
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="bq_data_transformation_players_stats_dataproc_spark_job",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["dtc-nba-de"],
) as dag:
    create_dataproc_cluster_task = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster_task",
        project_id=PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
    )

    submit_dataproc_spark_job_task = DataprocSubmitPySparkJobOperator(
        task_id="submit_dataproc_spark_job_task",
        main=gcs_source_uri,
        arguments=[],
        region=REGION,
        cluster_name=DATAPROC_CLUSTER,
        dataproc_jars=[
            "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.0.jar"
        ],
    )

    delete_dataproc_cluster_task = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster_task",
        project_id=PROJECT_ID,
        cluster_name=DATAPROC_CLUSTER,
        region=REGION,
        trigger_rule="all_done",
    )

    (
        create_dataproc_cluster_task
        >> submit_dataproc_spark_job_task
        >> delete_dataproc_cluster_task
    )
