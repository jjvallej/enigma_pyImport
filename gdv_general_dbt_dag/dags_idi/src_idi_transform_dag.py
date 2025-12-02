# dags/src_idi_transform_dag.py
"""
DAG para transformar los datos del IDI (Índice de Desempeño Institucional) 
desde la capa bronze a silver y gold utilizando modelos dbt.

TRANSFORMACIONES:
Bronze → Silver (tabla consolidada):
- Une tablas: idi_raw_data_territorio_2024 y idi_raw_data_territorio_2023
- Agrega columna 'anio' con el año correspondiente
- Normaliza nombres de columnas (automático por dbt)
- Normaliza departamento/entidad a mayúsculas sin tildes
- Reemplaza NULL/NaN por 0 en columnas numéricas
- Resultado: tabla idi_consolidated en silver

Silver → Gold (vista):
- Crea vista idi_data en gold apuntando a silver
- Disponible para análisis y reportes

TABLAS ORIGEN (bronze):
- idi_raw_data_territorio_2024
- idi_raw_data_territorio_2023
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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

from modules.idi.idi_load import ensure_dataset
from modules.config import CONF, DATASET_ID_SILVER, DATASET_ID_GOLD

# === CONFIGURACIÓN ===
# Usar project_root para encontrar la carpeta dbt dinámicamente
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt")

# Nombres de los modelos dbt a ejecutar
DBT_MODEL_2023 = "idi_transformed_data_2023"
DBT_MODEL_2024 = "idi_transformed_data_2024"
DBT_MODEL_CONSOLIDATED = "idi_transformed_data_consolidated"
DBT_MODEL_GOLD = "idi_processed_data"

def _ensure_dataset_silver_task():
    """Asegura que el dataset silver exista."""
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    """Asegura que el dataset gold exista."""
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

with DAG(
    dag_id="src_planeacion_transf_idi",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=[
        "secretaria:planeacion",
        "actividad:transformacion",
        "fuente:idi",
        "ejecución:manual"
    ],
    description="Transforma datos del IDI desde bronze a silver (tabla consolidada 2023-2024) y crea vista en gold. Normaliza columnas, agrega año, reemplaza NULL por 0 y normaliza departamento.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(task_id="start")

    # ===== CAPA SILVER =====
    with TaskGroup(group_id="silver") as silver_group:
        
        # Asegurar que dataset silver exista
        ensure_dataset_silver = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_silver_task,
        )

        # Transformar año 2023
        dbt_2023 = BashOperator(
            task_id="dbt_idi_2023",
            bash_command=f"set -e && cd {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select {DBT_MODEL_2023} 2>&1 || (echo 'DBT command failed with exit code:' $? && exit 1)",
            append_env=True,
        )

        # Transformar año 2024
        dbt_2024 = BashOperator(
            task_id="dbt_idi_2024",
            bash_command=f"set -e && cd {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select {DBT_MODEL_2024} 2>&1 || (echo 'DBT command failed with exit code:' $? && exit 1)",
            append_env=True,
        )

        # Consolidar ambas tablas
        dbt_consolidated = BashOperator(
            task_id="dbt_idi_consolidated",
            bash_command=f"set -e && cd {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select {DBT_MODEL_CONSOLIDATED} 2>&1 || (echo 'DBT command failed with exit code:' $? && exit 1)",
            append_env=True,
        )

        # Dependencias silver: ensure_dataset -> (2023 y 2024 en paralelo) -> consolidated
        ensure_dataset_silver >> [dbt_2023, dbt_2024] >> dbt_consolidated

    # ===== CAPA GOLD =====
    with TaskGroup(group_id="gold") as gold_group:
        
        # Asegurar que dataset gold exista
        ensure_dataset_gold = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        # Crear vista en gold
        dbt_gold = BashOperator(
            task_id="dbt_idi_processed_data",
            bash_command=f"set -e && cd {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select {DBT_MODEL_GOLD} 2>&1 || (echo 'DBT command failed with exit code:' $? && exit 1)",
            append_env=True,
        )

        # Dependencias gold
        ensure_dataset_gold >> dbt_gold

    # Tarea final
    end = EmptyOperator(task_id="end")

    # Flujo principal: start -> silver -> gold -> end
    start >> silver_group >> gold_group >> end