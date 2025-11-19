# dags/src_ipm_transform_dag.py
"""
DAG para transformar los datos del IPM desde la capa bronze a las capas silver y gold
utilizando modelos dbt.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import os, sys

# Asegura que podamos importar el módulo local
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.ipm_transform import (
    ensure_dataset,
)

# === CONFIGURACIÓN ===
DATASET_ID_SILVER = "silver_dpt_planeacion_municipal_dev"
DATASET_ID_GOLD = "gold_dpt_planeacion_municipal_dev"
TABLE_NAME_SILVER = "ipm_transformed_data"
TABLE_NAME_GOLD = "ipm_processed_data"
DBT_PROJECT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"

def _ensure_dataset_silver_task():
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

with DAG(
    dag_id="src_planeacion_transf_ipm",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
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
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_stg || dbt run --select rawdata_ipmv2_stg",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s3_dbt_run_normalize_text = BashOperator(
            task_id="dbt_run_normalize_text",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_normalize_text || dbt run --select rawdata_ipmv2_normalize_text",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s4_dbt_run_transform_types = BashOperator(
            task_id="dbt_run_transform_types",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_transform_types || dbt run --select rawdata_ipmv2_transform_types",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s5_dbt_run_clean_numbers = BashOperator(
            task_id="dbt_run_clean_numbers",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_clean_numbers || dbt run --select rawdata_ipmv2_clean_numbers",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s6_dbt_run_detect_negatives = BashOperator(
            task_id="dbt_run_detect_negatives",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_detect_negatives || dbt run --select rawdata_ipmv2_detect_negatives",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s7_dbt_run_apply_validations = BashOperator(
            task_id="dbt_run_apply_validations",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_apply_validations || dbt run --select rawdata_ipmv2_apply_validations",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s8_dbt_run_clean = BashOperator(
            task_id="dbt_run_clean",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_clean || dbt run --select rawdata_ipmv2_clean",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s9_dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt test --select rawdata_ipmv2_clean || dbt test --select rawdata_ipmv2_clean",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
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
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_gold || dbt run --select rawdata_ipmv2_gold",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        g3_dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt test --select rawdata_ipmv2_gold || dbt test --select rawdata_ipmv2_gold",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Dependencias: ensure_dataset -> dbt_run_gold -> dbt_test
        g1_ensure_dataset >> g2_dbt_run_gold >> g3_dbt_test

    # Dependencias: start -> silver -> gold
    start >> silver_group >> gold_group

