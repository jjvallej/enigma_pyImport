"""
DAG de transform: toma la última ejecución en Bronze (sap_api_evaplan_raw_data)
y actualiza Gold (sap_api_evaplan_final_data): borra solo el bloque (periodo_ini, periodo_fin)
de esa ejecución y vuelve a insertar sus registros. Mantiene el resto de periodos en Gold.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils import timezone

import os
import sys

from google.cloud import bigquery


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
    PROJECT_ID,
    LOCATION,
    DATASET_ID_SILVER,
    DATASET_ID_GOLD,
    get_dbt_command,
)
from modules.gcp_utils import get_bq_client  # noqa: E402

CFG = CONF.sap_api_evaplan

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _ensure_dataset(dataset_id: str, description: str) -> None:
    client = get_bq_client()
    ds_fqn = f"{PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(ds_fqn)
    except Exception:
        ds = bigquery.Dataset(ds_fqn)
        ds.location = LOCATION
        ds.description = description
        client.create_dataset(ds)


def _ensure_dataset_silver_task() -> None:
    dataset_id = getattr(CFG, "silver_dataset", None) or DATASET_ID_SILVER
    _ensure_dataset(dataset_id, "Silver layer SAP API Evaplan")


def _ensure_dataset_gold_task() -> None:
    dataset_id = getattr(CFG, "gold_dataset", None) or DATASET_ID_GOLD
    _ensure_dataset(dataset_id, "Gold layer SAP API Evaplan")


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt")

with DAG(
    dag_id="src_sap_api_evaplan_transform_dag",
    default_args=DEFAULT_ARGS,
    description="Actualiza Gold (sap_api_evaplan_final_data) con la última ejecución en Bronze",
    # Solo por trigger desde load.
    schedule=None,
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["planeacion_municipal", "sap_api_evaplan", "bigquery", "gold", "transform"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="silver") as silver:
        ensure_silver = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_silver_task,
        )
        dbt_silver = BashOperator(
            task_id="dbt_sap_api_evaplan_transformed_data",
            bash_command=get_dbt_command(
                "dbt run --select sap_api_evaplan_transformed_data",
                DBT_PROJECT_DIR,
            ),
            append_env=True,
        )
        ensure_silver >> dbt_silver

    with TaskGroup(group_id="gold") as gold:
        ensure_gold = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )
        dbt_gold = BashOperator(
            task_id="dbt_sap_api_evaplan_final_data",
            bash_command=get_dbt_command(
                "dbt run --select sap_api_evaplan_final_data",
                DBT_PROJECT_DIR,
            ),
            append_env=True,
        )
        ensure_gold >> dbt_gold

    start >> silver >> gold >> end
