# dags/src_ipm_sisben_load_dag.py
"""
DAG para cargar el archivo Excel IPM SISBEN desde GCS a BigQuery en la capa bronze.
El archivo debe estar en la carpeta 'ipm/sisben' dentro de 'data_staging/dpt_planeacion_municipal/'.
Crea la tabla 'ipm_sisben_raw_data' en el dataset de bronce.
Procesa TODAS las hojas del Excel (puede tener 3 hojas con ~1 millón de filas cada una).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
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
    get_latest_excel_from_gcs_folder,
    download_excel_from_gcs,
    load_excel_to_bq,
    cleanup_temp_paths,
)
from modules.config import CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE

# === CONFIGURACIÓN ===
GCS_BUCKET_NAME = DEFAULT_BUCKET_NAME
GCS_FOLDER_PATH = CONF.ipm_sisben.gcs_folder
TABLE_NAME = CONF.ipm_sisben.table_name

def _ensure_dataset_bronze_task():
    """Asegura que el dataset bronze exista."""
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _download_excel_task():
    """Descarga el último archivo Excel de la carpeta en GCS."""
    print(f"[INFO] Buscando el último archivo .xlsx en gs://{GCS_BUCKET_NAME}/{GCS_FOLDER_PATH}")
    gcs_uri = get_latest_excel_from_gcs_folder(bucket_name=GCS_BUCKET_NAME, folder_path=GCS_FOLDER_PATH)
    
    print(f"[INFO] Descargando archivo desde GCS: {gcs_uri}")
    return download_excel_from_gcs(gcs_uri=gcs_uri)

def _load_excel_to_bq_task(ti):
    """
    Carga el archivo Excel a BigQuery usando el método rápido (Parquet desde GCS).
    """
    local_excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    if not local_excel_path:
        raise ValueError("No se recibió la ruta del Excel en XCom (task download_excel).")
    
    print(f"[INFO] Cargando Excel a BigQuery (método rápido: Parquet desde GCS)")
    print(f"[INFO] Dataset: {DATASET_ID_BRONZE}")
    print(f"[INFO] Tabla: {TABLE_NAME}")
    
    # Cargar el Excel a BigQuery usando el método rápido
    rows_loaded = load_excel_to_bq(
        local_path=local_excel_path,
        dataset_id=DATASET_ID_BRONZE,
        table_name=TABLE_NAME,
        use_fast_method=True,  # Usar método rápido (Parquet desde GCS)
        bucket_name=GCS_BUCKET_NAME,
        gcs_temp_folder=f"data_staging/dpt_planeacion_municipal/temp_parquet"
    )
    
    print(f"[OK] Archivo cargado exitosamente: {rows_loaded} filas en {DATASET_ID_BRONZE}.{TABLE_NAME}")
    
    return rows_loaded

def _cleanup_temp_files_task(ti):
    """Limpia los archivos temporales."""
    excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    cleanup_temp_paths([excel_path])

with DAG(
    dag_id="src_planeacion_load_ipm_sisben",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=6),  # Timeout de 4 horas para el DAG completo
    tags=["secretaria:planeacion", "actividad:carga", "fuente:ipm_sisben", "ejecución:manual"],
    description="Carga el archivo Excel IPM SISBEN desde GCS a BigQuery en la capa bronze. Procesa TODAS las hojas del Excel. Crea la tabla 'ipm_sisben_raw_data' en el dataset de bronce.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la carga (capa bronze)
    with TaskGroup(group_id="bronze") as bronze_group:
        t1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_bronze_task,
        )

        t2_download_excel = PythonOperator(
            task_id="download_excel",
            python_callable=_download_excel_task,
            execution_timeout=timedelta(hours=4),  # 1 hora para descargar el archivo grande
        )

        t3_load_excel = PythonOperator(
            task_id="load_excel_to_bq",
            python_callable=_load_excel_to_bq_task,
            execution_timeout=timedelta(hours=4),  # 3 horas para procesar todas las hojas
        )

        t4_cleanup_temp_files = PythonOperator(
            task_id="cleanup_temp_files",
            python_callable=_cleanup_temp_files_task,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        t1_ensure_dataset >> t2_download_excel >> t3_load_excel >> t4_cleanup_temp_files

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> bronze_group >> end

