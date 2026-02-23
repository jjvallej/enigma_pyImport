"""
DAG de transform: toma la última ejecución en Bronze (sap_api_evaplan_raw_data)
y actualiza Gold (sap_api_evaplan_final_data): borra solo el bloque (periodo_ini, periodo_fin)
de esa ejecución y vuelve a insertar sus registros. Mantiene el resto de periodos en Gold.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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

from modules.config import CONF  # noqa: E402
from modules.sap_api_evaplan.sap_api_evaplan_transform import run_transform  # noqa: E402

CFG = CONF.sap_api_evaplan

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="src_sap_api_evaplan_transform_dag",
    default_args=DEFAULT_ARGS,
    description="Actualiza Gold (sap_api_evaplan_final_data) con la última ejecución en Bronze",
    schedule=getattr(CFG, "schedule_interval", None),
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["planeacion_municipal", "sap_api_evaplan", "bigquery", "gold", "transform"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="gold") as gold:
        update_gold = PythonOperator(
            task_id="update_sap_api_evaplan_final_data",
            python_callable=run_transform,
        )

    start >> gold >> end
