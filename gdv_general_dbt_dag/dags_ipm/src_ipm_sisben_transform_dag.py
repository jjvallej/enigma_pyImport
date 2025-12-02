# dags/src_ipm_sisben_transform_dag.py
"""
DAG para transformar datos de IPM SISBEN desde la capa bronze a silver y gold usando dbt.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os
import sys

# Función para encontrar la raíz del proyecto (donde está la carpeta modules)
def add_project_root_to_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while current_dir != "/":
        if os.path.exists(os.path.join(current_dir, "modules")):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            return
        current_dir = os.path.dirname(current_dir)
    
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

add_project_root_to_path()

from modules.ipm.ipm_sisben_load import ensure_dataset
from modules.config import CONF, DATASET_ID_SILVER, DATASET_ID_GOLD

# === CONFIGURACIÓN ===
# Usar project_root para encontrar la carpeta dbt dinámicamente
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt")

# Nombres de los modelos dbt a ejecutar
DBT_MODEL_SILVER = "ipm_sisben_stg"
DBT_MODEL_GOLD = "ipm_sisben_processed_data"

def _ensure_dataset_silver_task():
    """Asegura que el dataset silver exista."""
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    """Asegura que el dataset gold exista."""
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

with DAG(
    dag_id="src_planeacion_transform_ipm_sisben",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:transformacion", "fuente:ipm_sisben", "ejecución:manual"],
    description="Transforma datos de IPM SISBEN desde bronze a silver y gold usando dbt. Agrega columnas de descripción para códigos numéricos y crea vista final en gold.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la transformación (capa silver)
    with TaskGroup(group_id="silver") as silver_group:
        # Tarea para asegurar que el dataset exista
        ensure_dataset_silver_task = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_silver_task,
        )

        # Tarea dbt para ejecutar el modelo de transformación silver
        dbt_silver = BashOperator(
            task_id="dbt_ipm_sisben_stg",
            bash_command=f"set -e && cd {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select {DBT_MODEL_SILVER} 2>&1 || (echo 'DBT command failed with exit code:' $? && exit 1)",
            append_env=True,
        )

        # Dependencias dentro del grupo silver: ensure_dataset -> dbt_transform
        ensure_dataset_silver_task >> dbt_silver

    # Grupo de tareas para la transformación (capa gold)
    with TaskGroup(group_id="gold") as gold_group:
        # Tarea para asegurar que el dataset exista
        ensure_dataset_gold_task = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        # Tarea dbt para ejecutar el modelo de transformación gold
        dbt_gold = BashOperator(
            task_id="dbt_ipm_sisben_processed_data",
            bash_command=f"set -e && cd {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select {DBT_MODEL_GOLD} 2>&1 || (echo 'DBT command failed with exit code:' $? && exit 1)",
            append_env=True,
        )

        # Dependencias dentro del grupo gold: ensure_dataset -> dbt_transform
        ensure_dataset_gold_task >> dbt_gold

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> silver -> gold -> end
    start >> silver_group >> gold_group >> end

