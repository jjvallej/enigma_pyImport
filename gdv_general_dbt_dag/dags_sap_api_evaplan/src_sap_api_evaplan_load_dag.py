"""
DAG de carga: toma el archivo JSONL de SAP API en GCS (dpt_planeacion_municipal/sap_api_evaplan)
y lo carga en BigQuery bronze (bronze_dpt_planeacion_municipal).
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
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(current_dir).startswith("dags_"):
        project_root = os.path.dirname(current_dir)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        return
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

CFG = CONF.sap_api_evaplan

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

target_dataset = getattr(CFG, "target_dataset", None) or DATASET_ID_BRONZE
target_table = CFG.target_table
gcs_object = f"{CFG.gcs_base_folder}/{CFG.export_filename}"

with DAG(
    dag_id="src_sap_api_evaplan_load_dag",
    default_args=DEFAULT_ARGS,
    description="Carga SAP API Evaplan desde GCS a BigQuery (bronze) - planeación municipal",
    schedule=getattr(CFG, "schedule_interval", None),
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["planeacion_municipal", "sap_api_evaplan", "gcs", "bigquery", "bronze"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="bronze") as bronze:
        load_to_bq = GCSToBigQueryOperator(
            task_id="load_sap_api_evaplan_raw_data",
            bucket=DEFAULT_BUCKET_NAME,
            source_objects=[gcs_object],
            destination_project_dataset_table=f"{PROJECT_ID}.{target_dataset}.{target_table}",
            source_format=CFG.source_format,
            write_disposition=CFG.write_disposition,
            autodetect=CFG.autodetect,
        )

    start >> bronze >> end
