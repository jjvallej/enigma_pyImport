# dags/load_rawdata_ipmv2_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import os, sys, tempfile
import pandas as pd

# Asegura que podamos importar el módulo local
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.load_rawdata_ipmv2 import (
    ensure_dataset,
    download_excel_from_gcs,
    transform_excel,
    load_dataframe_to_bq,
    cleanup_temp_paths,
)

# === CONFIGURACIÓN ===
GCS_URI = "gs://gdv_ipm_dane/24/10/2025/2_IPM_DANE.xlsx"   # <--- cambia si es necesario
DATASET_ID_BRONZE = "gdv_ipmv2_bronze"
DATASET_ID_SILVER = "gdv_ipmv2_silver"
DATASET_ID_GOLD = "gdv_ipmv2_gold"
TABLE_NAME = "rawdata_ipmv2"
SHEET_INDEX = 0
DBT_PROJECT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"

def _ensure_dataset_bronze_task():
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _ensure_dataset_silver_task():
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

def _download_excel_task():
    return download_excel_from_gcs(gcs_uri=GCS_URI)

def _transform_task(ti):
    local_excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    if not local_excel_path:
        raise ValueError("No se recibió la ruta del Excel en XCom (task download_excel).")
    df = transform_excel(local_path=local_excel_path, sheet_index=SHEET_INDEX)
    tmp = tempfile.NamedTemporaryFile(suffix=".pkl", delete=False)
    tmp_path = tmp.name
    tmp.close()
    df.to_pickle(tmp_path)
    return tmp_path

def _load_task(ti):
    pickle_path = ti.xcom_pull(task_ids="bronze.transform_dataframe")
    if not pickle_path:
        raise ValueError("No se recibió la ruta del DataFrame transformado en XCom (task transform_dataframe).")
    df = pd.read_pickle(pickle_path)
    load_dataframe_to_bq(df, dataset_id=DATASET_ID_BRONZE, table_name=TABLE_NAME)

def _cleanup_temp_files_task(ti):
    excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    pickle_path = ti.xcom_pull(task_ids="bronze.transform_dataframe")
    cleanup_temp_paths([excel_path, pickle_path])

with DAG(
    dag_id="scr_planeacion_transf_ipm_manual",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["planeacion", "transformacion", "ipm", "manual"],
    description="Lee Excel IPM desde GCS, quita primera fila, renombra columnas (_Abs/_Porc), agrega fecha_lectura y carga a BQ (gdv_ipmv2_bronze.rawdata_ipmv2).",
) as dag:

    # Tarea inicial vacía
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la capa bronze
    with TaskGroup(group_id="bronze") as bronze_group:
        t1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_bronze_task,
        )

        t2_download_excel = PythonOperator(
            task_id="download_excel",
            python_callable=_download_excel_task,
        )

        t3_transform_dataframe = PythonOperator(
            task_id="transform_dataframe",
            python_callable=_transform_task,
        )

        t4_load_to_bq = PythonOperator(
            task_id="load_to_bq",
            python_callable=_load_task,
        )

        t5_cleanup_temp_files = PythonOperator(
            task_id="cleanup_temp_files",
            python_callable=_cleanup_temp_files_task,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        t1_ensure_dataset >> t2_download_excel >> t3_transform_dataframe >> t4_load_to_bq >> t5_cleanup_temp_files

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

        s5_dbt_run_clean = BashOperator(
            task_id="dbt_run_clean",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_clean || dbt run --select rawdata_ipmv2_clean",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s6_dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt test --select rawdata_ipmv2_clean || dbt test --select rawdata_ipmv2_clean",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Dependencias: ensure_dataset -> stg -> normalize_text -> transform_types -> clean -> test
        s1_ensure_dataset >> s2_dbt_run_stg >> s3_dbt_run_normalize_text >> s4_dbt_run_transform_types >> s5_dbt_run_clean >> s6_dbt_test

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

    # Dependencias: start -> bronze -> silver -> gold
    start >> bronze_group >> silver_group >> gold_group
