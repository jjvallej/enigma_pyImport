# dags/src_ipm_transform_dag.py
"""
DAG para transformar los datos del IPM desde la capa bronze a las capas silver y gold
utilizando modelos dbt.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os
import sys

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

from modules.ipm.ipm_transform import (
    ensure_dataset,
)

# === CONFIGURACIÓN ===
from modules.config import CONF, DATASET_ID_SILVER, DATASET_ID_GOLD
TABLE_NAME_SILVER = CONF.ipm.tables.silver
TABLE_NAME_GOLD = CONF.ipm.tables.gold
# Usar project_root (calculado arriba) para encontrar la carpeta dbt dinámicamente
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt")

def _ensure_dataset_silver_task():
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

with DAG(
    dag_id="src_planeacion_transf_ipm",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:transformacion", "fuente:ipm", "ejecución:manual"],
    description="Transforma los datos del IPM desde bronze a silver y gold utilizando modelos dbt. Espera que los datos ya estén en bronze_dpt_planeacion_municipal_dev.ipm_raw_data.",
) as dag:

    # Tarea inicial vacía
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la capa silver
    with TaskGroup(group_id="silver") as silver_group:
        s1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_silver_task,
        )

        # Tareas dbt para cada modelo
        # Usamos la ruta completa del ejecutable dbt o lo buscamos en el PATH del usuario
        s2_dbt_run_stg = BashOperator(
            task_id="dbt_run_stg",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_stg",
            append_env=True,
        )

        s3_dbt_run_normalize_text = BashOperator(
            task_id="dbt_run_normalize_text",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_normalize_text",
            append_env=True,
        )

        s4_dbt_run_transform_types = BashOperator(
            task_id="dbt_run_transform_types",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_transform_types",
            append_env=True,
            execution_timeout=timedelta(minutes=15),  # Timeout de 15 minutos
        )

        s5_dbt_run_clean_numbers = BashOperator(
            task_id="dbt_run_clean_numbers",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_clean_numbers",
            append_env=True,
        )

        s6_dbt_run_detect_negatives = BashOperator(
            task_id="dbt_run_detect_negatives",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_detect_negatives",
            append_env=True,
        )

        s7_dbt_run_apply_validations = BashOperator(
            task_id="dbt_run_apply_validations",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_apply_validations",
            append_env=True,
        )

        s8_dbt_run_clean = BashOperator(
            task_id="dbt_run_clean",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_clean",
            append_env=True,
        )

        s9_dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_transform_clean",
            append_env=True,
        )

        # Dependencias: 
        # ensure_dataset -> stg -> normalize_text -> [transform_types, clean_numbers] -> detect_negatives -> apply_validations -> clean -> test
        # transform_types y clean_numbers pueden ejecutarse en paralelo (ambos dependen de normalize_text)
        s1_ensure_dataset >> s2_dbt_run_stg >> s3_dbt_run_normalize_text
        s3_dbt_run_normalize_text >> [s4_dbt_run_transform_types, s5_dbt_run_clean_numbers]
        s5_dbt_run_clean_numbers >> s6_dbt_run_detect_negatives >> s7_dbt_run_apply_validations >> s8_dbt_run_clean >> s9_dbt_test

    # Grupo de tareas para la capa gold
    with TaskGroup(group_id="gold") as gold_group:
        g1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        # Tarea dbt para ejecutar el modelo gold
        g2_dbt_run_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_processed_data",
            append_env=True,
        )

        g3_dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select ipm_processed_data",
            append_env=True,
        )

        # Dependencias: ensure_dataset -> dbt_run_gold -> dbt_test
        g1_ensure_dataset >> g2_dbt_run_gold >> g3_dbt_test

    # Dependencias: start -> silver -> gold
    start >> silver_group >> gold_group

