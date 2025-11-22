# dags/src_idc_load_dag.py
"""
DAG para extraer datos del archivo Excel IDC desde Google Cloud Storage,
transformarlos mínimamente y cargarlos en la capa bronze de BigQuery.
El archivo tiene 3 hojas que se cargan como tablas separadas:
- Dato_original -> idc_raw_data_dato_original
- Valor_normalizado -> idc_raw_data_valor_normalizado
- Valor_ranking -> idc_raw_data_valor_ranking
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os, sys, tempfile
import pandas as pd

# Asegura que podamos importar el módulo local
# Agregar el directorio raíz del proyecto al path para importar módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from modules.idc.idc_load import (
    ensure_dataset,
    download_excel_from_gcs,
    get_latest_excel_from_gcs_folder,
    load_all_sheets_to_bq,
    cleanup_temp_paths,
)

# === CONFIGURACIÓN ===
# Configuración para buscar el último archivo Excel en la carpeta idc
# El DAG buscará automáticamente el archivo .xlsx más reciente en esta carpeta
GCS_BUCKET_NAME = "datalake_gdv"
GCS_FOLDER_PATH = "data_staging/dpt_planeacion_municipal/idc"
DATASET_ID_BRONZE = "test_idc_bronze"

# Mapeo de hojas del Excel a nombres de tablas en BigQuery
SHEET_TO_TABLE_MAPPING = {
    "Dato_original": "idc_raw_data_dato_original",
    "Valor_normalizado": "idc_raw_data_valor_normalizado",
    "Valor_ranking": "idc_raw_data_valor_ranking",
}

def _ensure_dataset_bronze_task():
    """Asegura que el dataset bronze exista."""
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _download_excel_task():
    """Descarga el último archivo Excel de la carpeta en GCS."""
    # Obtener el último archivo Excel de la carpeta en GCS
    try:
        # Intentar obtener configuración desde Variables de Airflow
        bucket_name = Variable.get("idc_gcs_bucket", default_var=GCS_BUCKET_NAME)
        folder_path = Variable.get("idc_gcs_folder", default_var=GCS_FOLDER_PATH)
        print(f"[INFO] Usando configuración: bucket={bucket_name}, carpeta={folder_path}")
    except:
        # Si no existen las variables, usar valores por defecto hardcodeados
        bucket_name = GCS_BUCKET_NAME
        folder_path = GCS_FOLDER_PATH
        print(f"[INFO] Usando configuración por defecto: bucket={bucket_name}, carpeta={folder_path}")
    
    # Buscar el último archivo Excel en la carpeta
    print(f"[INFO] Buscando el último archivo .xlsx en gs://{bucket_name}/{folder_path}")
    gcs_uri = get_latest_excel_from_gcs_folder(bucket_name=bucket_name, folder_path=folder_path)
    
    print(f"[INFO] Descargando archivo desde GCS: {gcs_uri}")
    return download_excel_from_gcs(gcs_uri=gcs_uri)

def _load_all_sheets_task(ti):
    """
    Carga todas las hojas del Excel a BigQuery como tablas separadas.
    """
    local_excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    if not local_excel_path:
        raise ValueError("No se recibió la ruta del Excel en XCom (task download_excel).")
    
    print(f"[INFO] Cargando todas las hojas del Excel a BigQuery")
    print(f"[INFO] Mapeo de hojas a tablas: {SHEET_TO_TABLE_MAPPING}")
    
    # Cargar todas las hojas
    results = load_all_sheets_to_bq(
        local_path=local_excel_path,
        dataset_id=DATASET_ID_BRONZE,
        sheet_to_table_mapping=SHEET_TO_TABLE_MAPPING
    )
    
    print(f"[OK] Todas las hojas fueron cargadas exitosamente:")
    for sheet_name, table_name in results.items():
        print(f"  - {sheet_name} -> {table_name}")
    
    return results

def _cleanup_temp_files_task(ti):
    """Limpia los archivos temporales."""
    excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    cleanup_temp_paths([excel_path])

with DAG(
    dag_id="src_planeacion_load_idc",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:idc", "ejecución:manual"],
    description="Lee Excel IDC desde GCS (con 3 hojas), transforma mínimamente y carga a BigQuery en test_idc_bronze como 3 tablas separadas. Luego ejecuta el DAG de transformación src_planeacion_transf_idc.",
) as dag:

    # Tarea inicial vacía
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la extracción (capa bronze)
    with TaskGroup(group_id="bronze") as bronze_group:
        t1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_bronze_task,
        )

        t2_download_excel = PythonOperator(
            task_id="download_excel",
            python_callable=_download_excel_task,
        )

        t3_load_all_sheets = PythonOperator(
            task_id="load_all_sheets",
            python_callable=_load_all_sheets_task,
        )

        t4_cleanup_temp_files = PythonOperator(
            task_id="cleanup_temp_files",
            python_callable=_cleanup_temp_files_task,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        t1_ensure_dataset >> t2_download_excel >> t3_load_all_sheets >> t4_cleanup_temp_files

    # Tarea para ejecutar el DAG de transformación
    trigger_transf_dag = TriggerDagRunOperator(
        task_id="trigger_transf_idc",
        trigger_dag_id="src_planeacion_transf_idc",
        wait_for_completion=True,  # Espera a que el DAG de transformación termine
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> bronze_group >> trigger_transf_dag >> end

