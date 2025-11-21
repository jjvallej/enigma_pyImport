# dags/src_idc_transform_dag.py
"""
DAG para transformar los datos del IDC desde la capa bronze a la capa silver
utilizando modelos dbt. Procesa dos pasos:
1. Normalización de nombres de columnas a snake_case
2. Conversión de nombres de columnas a minúsculas

Cada tabla tiene sus propios modelos:
- idc_normalize_columns_*: Convierte nombres a snake_case
- idc_lowercase_columns_*: Convierte nombres a minúsculas
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
from modules.idc_load import (
    ensure_dataset,
)

# === CONFIGURACIÓN ===
DATASET_ID_SILVER = "test_idc_silver"
DBT_PROJECT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"

def _ensure_dataset_silver_task():
    """Asegura que el dataset silver exista."""
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

with DAG(
    dag_id="src_planeacion_transf_idc",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:transformacion", "fuente:idc", "ejecución:manual"],
    description="Normaliza los nombres de columnas del IDC a snake_case desde bronze a silver utilizando modelos dbt. Espera que los datos ya estén en test_idc_bronze.",
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

        # Paso 1: Normalizar nombres de columnas a snake_case (para las 3 tablas en paralelo)
        s2_dbt_normalize_columns_dato_original = BashOperator(
            task_id="dbt_normalize_columns_dato_original",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_normalize_columns_dato_original || dbt run --select idc_normalize_columns_dato_original",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s3_dbt_normalize_columns_valor_normalizado = BashOperator(
            task_id="dbt_normalize_columns_valor_normalizado",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_normalize_columns_valor_normalizado || dbt run --select idc_normalize_columns_valor_normalizado",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s4_dbt_normalize_columns_valor_ranking = BashOperator(
            task_id="dbt_normalize_columns_valor_ranking",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_normalize_columns_valor_ranking || dbt run --select idc_normalize_columns_valor_ranking",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Paso 2: Convertir nombres de columnas a minúsculas (depende de normalize_columns, para las 3 tablas en paralelo)
        s5_dbt_lowercase_columns_dato_original = BashOperator(
            task_id="dbt_lowercase_columns_dato_original",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_lowercase_columns_dato_original || dbt run --select idc_lowercase_columns_dato_original",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s6_dbt_lowercase_columns_valor_normalizado = BashOperator(
            task_id="dbt_lowercase_columns_valor_normalizado",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_lowercase_columns_valor_normalizado || dbt run --select idc_lowercase_columns_valor_normalizado",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s7_dbt_lowercase_columns_valor_ranking = BashOperator(
            task_id="dbt_lowercase_columns_valor_ranking",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_lowercase_columns_valor_ranking || dbt run --select idc_lowercase_columns_valor_ranking",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Dependencias: ensure_dataset -> [normalize_columns en paralelo] -> [lowercase_columns en paralelo]
        s1_ensure_dataset >> [s2_dbt_normalize_columns_dato_original, s3_dbt_normalize_columns_valor_normalizado, s4_dbt_normalize_columns_valor_ranking]
        s2_dbt_normalize_columns_dato_original >> s5_dbt_lowercase_columns_dato_original
        s3_dbt_normalize_columns_valor_normalizado >> s6_dbt_lowercase_columns_valor_normalizado
        s4_dbt_normalize_columns_valor_ranking >> s7_dbt_lowercase_columns_valor_ranking

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> silver -> end
    start >> silver_group >> end
