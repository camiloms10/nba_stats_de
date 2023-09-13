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
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from nba_api.stats.endpoints import playergamelogs, teamyearbyyearstats, teamgamelogs
from nba_api.stats.static import teams
from nba_api.stats.library.parameters import (
    SeasonTypeAllStar,
    PerModeSimple,
    SeasonNullable,
)
import pandas as pd

from datetime import datetime, date
import time
from dateutil.relativedelta import relativedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")


parquet_file = "players_game_stats_current_year.parquet"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "STG_NBA_STATS_ALL")
BIGQUERY_DATASET_DBT_PROD = os.environ.get(
    "BIGQUERY_DATASET_DBT_DEV", "PROD_NBA_STATS_ALL"
)


# Getting current season first game date
teamlogdata = teamgamelogs.TeamGameLogs(season_nullable=SeasonNullable.current_season)
teamlogdata = teamlogdata.team_game_logs.get_data_frame()
start_date = pd.to_datetime(teamlogdata["GAME_DATE"])
start_date = start_date.min().to_pydatetime()


def format_to_parquet():
    teamlogdata = teamgamelogs.TeamGameLogs(
        season_nullable=SeasonNullable.current_season
    )
    teamlogdata = teamlogdata.team_game_logs.get_data_frame()

    # Getting current season first game date
    current_season_first_game_date = pd.to_datetime(teamlogdata["GAME_DATE"])
    current_season_first_game_date = (
        current_season_first_game_date.min().to_pydatetime()
    )
    current_season_first_game_date = date(
        current_season_first_game_date.year,
        current_season_first_game_date.month,
        current_season_first_game_date.day,
    )

    player_history_data = pd.DataFrame()
    season_type_nullable_list = ["Regular Season", "Playoffs"]
    for ss_type in season_type_nullable_list:
        playerlogdata = playergamelogs.PlayerGameLogs(
            season_nullable=SeasonNullable.current_season,
            season_type_nullable=ss_type,
        )
        playerlogdata = playerlogdata.player_game_logs.get_data_frame()
        playerlogdata = pd.DataFrame(playerlogdata)
        playerlogdata["GAME_TYPE"] = ss_type
        playerlogdata["SEASON"] = SeasonNullable.current_season
        playerlogdata[["FTA", "REB", "AST", "PF"]] = (
            playerlogdata[["FTA", "REB", "AST", "PF"]].fillna(0).astype(int)
        )
        playerlogdata[["GAME_DATE"]] = playerlogdata[["GAME_DATE"]].apply(
            pd.to_datetime
        )
        player_history_data = pd.concat(
            [player_history_data, playerlogdata], ignore_index=True
        )
    print(
        SeasonNullable.current_season + " - " + str(len(player_history_data)) + " rows"
    )
    player_history_data = player_history_data.loc[
        (player_history_data["GAME_DATE"].dt.date >= current_season_first_game_date)
    ]
    player_history_data.to_parquet(parquet_file)


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
    "start_date": start_date,
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="bq_players_stats_current_dag",
    schedule_interval="0 0 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["dbt-dtc-nba-de"],
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
                "tableId": "players_game_stats_current_season",
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
        trigger_dag_id="dbt_players_stats_job",  # Ensure this equals the dag_id of the DAG to trigger
    )

    (
        format_to_parquet_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
        >> delay_python_task
        >> trigger_dbt_cloud_job_run
    )
