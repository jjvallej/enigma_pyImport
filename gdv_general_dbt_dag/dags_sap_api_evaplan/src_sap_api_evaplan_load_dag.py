"""
DAG de carga: toma el archivo JSONL de SAP API en GCS (dpt_planeacion_municipal/sap_api_evaplan)
y lo carga en BigQuery bronze.

IMPORTANTE: Este DAG usa PythonOperator + run_load() (módulo sap_api_evaplan_load).
Si en los logs de Composer ves "autodetect: True" o el traceback en gcs_to_bigquery.py,
Composer está ejecutando una versión antigua. Re-sincroniza al bucket:
  - dags_sap_api_evaplan/src_sap_api_evaplan_load_dag.py
  - modules/sap_api_evaplan/sap_api_evaplan_load.py
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
from modules.sap_api_evaplan.sap_api_evaplan_load import run_load  # noqa: E402

CFG = CONF.sap_api_evaplan

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _do_load(**context):
    # Marca para verificar en logs que se ejecuta esta versión (PythonOperator + run_load).
    # Si en los logs ves "autodetect: True" o el traceback en gcs_to_bigquery.py, Composer
    # está usando una versión antigua del DAG; hay que re-sincronizar el código al bucket.
    print("[SAP_EVAPLAN_LOAD] Usando PythonOperator + run_load() (esquema desde tabla o primera línea)")
    ds_nodash = context.get("ds_nodash", "")
    run_load(ds_nodash=ds_nodash)


with DAG(
    dag_id="src_sap_api_evaplan_load_dag",
    default_args=DEFAULT_ARGS,
    description="Carga SAP API Evaplan desde GCS a BigQuery (bronze) - planeación municipal",
    # Solo por trigger desde ingest (no programar aquí: evita corridas sin archivo nuevo).
    schedule=None,
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["planeacion_municipal", "sap_api_evaplan", "gcs", "bigquery", "bronze"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="bronze") as bronze:
        load_to_bq = PythonOperator(
            task_id="load_sap_api_evaplan_raw_data",
            python_callable=_do_load,
        )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_sap_api_evaplan",
        trigger_dag_id="src_sap_api_evaplan_transform_dag",
        wait_for_completion=False,
    )

    start >> bronze >> trigger_transform >> end
