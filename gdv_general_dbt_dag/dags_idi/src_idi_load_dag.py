# dags/src_idi_load_dag.py
"""
DAG para cargar archivos CSV de IDI desde Google Cloud Storage a BigQuery.

PROCESO:
1. Busca todas las carpetas de años en GCS (2024, 2023, 2022, etc.)
2. Por cada año encontrado:
   - Descarga el CSV: gs://bucket/idi/{año}/resultados_{año}.csv
   - Carga a BigQuery en tabla: idi_raw_data_territorio_{año}
3. Todas las tablas se crean en el dataset bronze_dpt_planeacion_municipal_dev

ESTRUCTURA EN GCS:
gs://datalake_gdv_dev/
  └── data_staging/dpt_planeacion_municipal/idi/
      ├── 2024/resultados_2024.csv
      ├── 2023/resultados_2023.csv
      └── 2022/resultados_2022.csv

TABLAS EN BIGQUERY:
- bronze_dpt_planeacion_municipal_dev.idi_raw_data_territorio_2024
- bronze_dpt_planeacion_municipal_dev.idi_raw_data_territorio_2023
- bronze_dpt_planeacion_municipal_dev.idi_raw_data_territorio_2022
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
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

from modules.idi.idi_load import (
    ensure_dataset,
    load_all_years_idi_to_bq,
)
from modules.config import CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE

# === CONFIGURACIÓN ===
GCS_BUCKET_NAME = DEFAULT_BUCKET_NAME
GCS_BASE_FOLDER = CONF.idi.gcs_base_folder
TABLE_PREFIX = CONF.idi.tables.bronze_prefix


def _ensure_dataset_bronze_task():
    """Asegura que el dataset bronze exista."""
    print(f"[INFO] Verificando dataset: {DATASET_ID_BRONZE}")
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)
    print(f"[OK] Dataset listo: {DATASET_ID_BRONZE}")


def _load_all_years_task():
    """
    Carga TODOS los años disponibles de IDI desde GCS a BigQuery.
    Cada año se carga en una tabla separada.
    """
    print(f"[INFO] Iniciando carga de IDI")
    print(f"[INFO] Bucket: {GCS_BUCKET_NAME}")
    print(f"[INFO] Carpeta base: {GCS_BASE_FOLDER}")
    print(f"[INFO] Dataset destino: {DATASET_ID_BRONZE}")
    print(f"[INFO] Prefijo de tablas: {TABLE_PREFIX}")
    
    results = load_all_years_idi_to_bq(
        bucket_name=GCS_BUCKET_NAME,
        base_folder=GCS_BASE_FOLDER,
        dataset_id=DATASET_ID_BRONZE,
        table_prefix=TABLE_PREFIX
    )
    
    print(f"\n[OK] Carga completada exitosamente")
    print(f"[INFO] Total años cargados: {len(results)}")
    
    for year, table_ref in results.items():
        print(f"  ✅ Año {year} -> {table_ref}")
    
    return results


with DAG(
    dag_id="src_planeacion_load_idi",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=[
        "secretaria:planeacion",
        "actividad:carga",
        "fuente:idi",
        "ejecución:manual",
    ],
    description="Carga todos los años de IDI desde GCS (CSVs) a BigQuery como tablas separadas en bronze_dpt_planeacion_municipal_dev",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la carga (capa bronze)
    with TaskGroup(group_id="bronze") as bronze_group:
        # Asegurar que el dataset exista
        ensure_dataset_task = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_bronze_task,
        )

        # Cargar todos los años
        load_all_years = PythonOperator(
            task_id="load_all_years_to_bigquery",
            python_callable=_load_all_years_task,
        )

        # Dependencias dentro del grupo bronze
        ensure_dataset_task >> load_all_years

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> bronze -> end
    start >> bronze_group >> end