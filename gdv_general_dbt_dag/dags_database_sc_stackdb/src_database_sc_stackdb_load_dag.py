"""
DAG de carga: toma el archivo exportado desde Postgres en GCS y lo carga a BigQuery (bronze).
Agrupa sus tareas en un TaskGroup llamado 'brone'.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils import timezone

import os
import sys


def add_project_root_to_path():
    """Asegura que la carpeta del proyecto (donde vive modules) esté en sys.path."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while current_dir != "/":
        modules_dir = os.path.join(current_dir, "modules")
        if os.path.exists(modules_dir):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            return
        current_dir = os.path.dirname(current_dir)
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)


add_project_root_to_path()

from modules.config import (  # noqa: E402
    CONF,
    DEFAULT_BUCKET_NAME,
    DATASET_ID_BRONZE,
    PROJECT_ID,
)

DB_CFG = CONF.database_sc_stackdb

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

target_dataset = getattr(DB_CFG, "target_dataset", DATASET_ID_BRONZE) or DATASET_ID_BRONZE
target_table = DB_CFG.target_table
gcs_object = f"{DB_CFG.gcs_base_folder}/{DB_CFG.export_filename}"


with DAG(
    dag_id="src_database_sc_stackdb_load_dag",
    default_args=DEFAULT_ARGS,
    description="Carga el archivo de GCS a BigQuery (bronze)",
    schedule=getattr(DB_CFG, "schedule_interval", None),
    start_date=timezone.datetime(2024, 1, 1),
    catchup=False,
    tags=["database", "gcs", "bigquery", "bronze"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="brone") as brone:
        load_to_bq = GCSToBigQueryOperator(
            task_id="load_database_test_raw_data",
            bucket=DEFAULT_BUCKET_NAME,
            source_objects=[gcs_object],
            destination_project_dataset_table=f"{PROJECT_ID}.{target_dataset}.{target_table}",
            source_format=DB_CFG.source_format,
            write_disposition=DB_CFG.write_disposition,
            skip_leading_rows=DB_CFG.skip_leading_rows,
            field_delimiter=DB_CFG.field_delimiter,
            autodetect=DB_CFG.autodetect,
            allow_quoted_newlines=True,
        )

    start >> brone >> end

