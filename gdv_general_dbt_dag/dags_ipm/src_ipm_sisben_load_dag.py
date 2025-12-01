# dags/src_ipm_sisben_load_dag.py
"""
DAG simplificado para procesar el archivo Excel IPM SISBEN desde GCS.
1. Asegura que el dataset bronze exista
2. Lee el Excel desde el bucket
3. Convierte el Excel a CSV y lo guarda en el mismo bucket
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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

from modules.ipm.ipm_sisben_load import (
    ensure_dataset,
    leer_excel_desde_bucket,
    convertir_excel_a_csv_y_guardar,
)
from modules.config import CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE

# === CONFIGURACIÓN ===
GCS_BUCKET_NAME = DEFAULT_BUCKET_NAME
EXCEL_FOLDER_PATH = CONF.ipm_sisben.excel_file_path

def _ensure_dataset_bronze_task():
    """Asegura que el dataset bronze exista."""
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _leer_y_convertir_excel_task():
    """
    Lee el Excel desde el bucket y lo convierte a CSV en una sola operación.
    Esto evita descargar el archivo dos veces.
    """
    print(f"[INFO] Leyendo Excel desde: gs://{GCS_BUCKET_NAME}/{EXCEL_FOLDER_PATH}")
    
    # Leer el Excel (descarga temporalmente, lee, y elimina el temporal)
    df = leer_excel_desde_bucket(
        bucket_name=GCS_BUCKET_NAME,
        folder_path=EXCEL_FOLDER_PATH
    )
    
    print("✓ Archivo cargado exitosamente")
    print(f"\nDimensiones del DataFrame: {df.shape[0]:,} filas x {df.shape[1]} columnas")
    
    # Convertir a CSV y guardar en el bucket (todo en la misma tarea)
    print(f"[INFO] Convirtiendo Excel a CSV...")
    csv_uri = convertir_excel_a_csv_y_guardar(
        df=df,
        bucket_name=GCS_BUCKET_NAME,
        folder_path=EXCEL_FOLDER_PATH
    )
    
    print(f"[OK] CSV guardado exitosamente en: {csv_uri}")
    return csv_uri

with DAG(
    dag_id="src_planeacion_load_ipm_sisben",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=6),
    tags=["secretaria:planeacion", "actividad:carga", "fuente:ipm_sisben", "ejecución:manual"],
    description="Procesa el archivo Excel IPM SISBEN desde GCS: lee el Excel y lo convierte a CSV.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea para asegurar que el dataset exista
    ensure_dataset_task = PythonOperator(
        task_id="ensure_dataset",
        python_callable=_ensure_dataset_bronze_task,
    )

    # Tarea para leer el Excel y convertirlo a CSV (todo en una sola operación)
    leer_y_convertir_task = PythonOperator(
        task_id="leer_y_convertir_excel",
        python_callable=_leer_y_convertir_excel_task,
        execution_timeout=timedelta(hours=4),  # Más tiempo porque hace ambas operaciones
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> ensure_dataset_task >> leer_y_convertir_task >> end
