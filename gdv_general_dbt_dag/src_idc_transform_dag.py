# dags/src_idc_transform_dag.py
"""
DAG para transformar los datos del IDC desde la capa bronze a la capa silver
utilizando modelos dbt. Solo procesa la capa silver (no gold).

Cada transformación está en un modelo separado:
1. normalize_columns: Normaliza nombres de columnas a snake_case
2. normalize_text: Normaliza texto a mayúsculas sin caracteres especiales
3. transform_numbers: Transforma números (decimales para dato_original/valor_normalizado, enteros para valor_ranking)
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
    description="Transforma los datos del IDC desde bronze a silver utilizando modelos dbt. Espera que los datos ya estén en test_idc_bronze.",
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

        # Paso 1: Normalizar nombres de columnas (para las 3 tablas en paralelo)
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

        # Paso 2: Normalizar texto (depende de normalize_columns, para las 3 tablas en paralelo)
        s5_dbt_normalize_text_dato_original = BashOperator(
            task_id="dbt_normalize_text_dato_original",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_normalize_text_dato_original || dbt run --select idc_normalize_text_dato_original",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s6_dbt_normalize_text_valor_normalizado = BashOperator(
            task_id="dbt_normalize_text_valor_normalizado",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_normalize_text_valor_normalizado || dbt run --select idc_normalize_text_valor_normalizado",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s7_dbt_normalize_text_valor_ranking = BashOperator(
            task_id="dbt_normalize_text_valor_ranking",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_normalize_text_valor_ranking || dbt run --select idc_normalize_text_valor_ranking",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Paso 3: Transformar números (depende de normalize_text, para las 3 tablas en paralelo)
        s8_dbt_transform_numbers_dato_original = BashOperator(
            task_id="dbt_transform_numbers_dato_original",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_transform_numbers_dato_original || dbt run --select idc_transform_numbers_dato_original",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s9_dbt_transform_numbers_valor_normalizado = BashOperator(
            task_id="dbt_transform_numbers_valor_normalizado",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_transform_numbers_valor_normalizado || dbt run --select idc_transform_numbers_valor_normalizado",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s10_dbt_transform_numbers_valor_ranking = BashOperator(
            task_id="dbt_transform_numbers_valor_ranking",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select idc_transform_numbers_valor_ranking || dbt run --select idc_transform_numbers_valor_ranking",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Dependencias:
        # ensure_dataset -> [normalize_columns en paralelo] -> [normalize_text en paralelo] -> [transform_numbers en paralelo]
        s1_ensure_dataset >> [s2_dbt_normalize_columns_dato_original, s3_dbt_normalize_columns_valor_normalizado, s4_dbt_normalize_columns_valor_ranking]
        s2_dbt_normalize_columns_dato_original >> s5_dbt_normalize_text_dato_original >> s8_dbt_transform_numbers_dato_original
        s3_dbt_normalize_columns_valor_normalizado >> s6_dbt_normalize_text_valor_normalizado >> s9_dbt_transform_numbers_valor_normalizado
        s4_dbt_normalize_columns_valor_ranking >> s7_dbt_normalize_text_valor_ranking >> s10_dbt_transform_numbers_valor_ranking

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> silver -> end
    start >> silver_group >> end
