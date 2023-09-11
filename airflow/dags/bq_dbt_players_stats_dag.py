import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import (
    DbtCloudRunJobOperator,
)

DBT_ACCOUNT_ID = int(os.environ.get("DBT_ACCOUNT_ID"))
DBT_PROJECT_ID = int(os.environ.get("DBT_PROJECT_ID"))
DBT_JOB_ID = int(os.environ.get("DBT_JOB_ID"))
DBT_API_KEY_FILE = str(os.environ.get("DBT_API_KEY_FILE"))

with DAG(
    dag_id="dbt_players_stats_job",
    default_args={"dbt_cloud_conn_id": "dbt_cloud", "account_id": DBT_ACCOUNT_ID},
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt-dtc-nba-de"],
) as dag:
    trigger_dbt_cloud_job_run = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job_run",
        job_id=DBT_JOB_ID,
        check_interval=10,
        timeout=300,
    )
