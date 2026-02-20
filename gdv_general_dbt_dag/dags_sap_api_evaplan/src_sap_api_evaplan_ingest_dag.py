"""
DAG de ingesta: consulta la API SAP (zimportdata) con params ini/fin (YYYYMM),
guarda la respuesta como JSONL en GCS (data_staging/dpt_planeacion_municipal/sap_api_evaplan).
Al finalizar dispara el DAG de load.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
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

from modules.config import CONF, DEFAULT_BUCKET_NAME  # noqa: E402
from modules.sap_api_evaplan.sap_api_evaplan_ingest import run_ingest, ensure_gcs_folder  # noqa: E402

CFG = CONF.sap_api_evaplan


def _do_ingest(**context):
    ds_nodash = context.get("ds_nodash", "latest")
    gcs_uri = run_ingest(
        ds_nodash=ds_nodash,
        bucket_name=DEFAULT_BUCKET_NAME,
    )
    print(f"[OK] Exportado a {gcs_uri}")
    return gcs_uri


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="src_sap_api_evaplan_ingest_dag",
    default_args=DEFAULT_ARGS,
    description="Ingesta API SAP (zimportdata) a GCS - planeación municipal",
    schedule=getattr(CFG, "schedule_interval", None),
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["planeacion_municipal", "sap_api_evaplan", "api", "gcs"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="bronze") as bronze:
        ensure_folder = PythonOperator(
            task_id="ensure_gcs_folder",
            python_callable=ensure_gcs_folder,
            op_kwargs={"bucket_name": DEFAULT_BUCKET_NAME},
        )

        export_to_gcs = PythonOperator(
            task_id="export_sap_api_to_gcs",
            python_callable=_do_ingest,
        )

        ensure_folder >> export_to_gcs

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_sap_api_evaplan",
        trigger_dag_id="src_sap_api_evaplan_load_dag",
        wait_for_completion=False,
    )

    start >> bronze >> trigger_load_dag >> end
