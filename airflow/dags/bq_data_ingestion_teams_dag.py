import os
import logging
import gzip

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
import pyarrow.csv as pv
import pyarrow.parquet as pq

from nba_api.stats.static import teams
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


parquet_file = "teams.parquet"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "STG_NBA_STATS_ALL")


def format_to_parquet():
    teams_table = pd.DataFrame(teams.get_teams())
    teams_table.to_parquet(parquet_file)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="bq_teams_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["dtc-nba-de"],
) as dag:
    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "nba_teams",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    delay_python_task = PythonOperator(
        task_id="delay_python_task",
        provide_context=True,
        python_callable=lambda: time.sleep(60),
    )

    trigger_dbt_cloud_job_run = TriggerDagRunOperator(
        task_id="trigger_dbt_cloud_job_run",
        trigger_dag_id="dbt_dim_teams_job",  # Ensure this equals the dag_id of the DAG to trigger
    )

    (
        format_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
        >> delay_python_task
        >> trigger_dbt_cloud_job_run
    )
