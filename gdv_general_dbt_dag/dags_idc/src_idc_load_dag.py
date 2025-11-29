# dags/src_idc_load_dag.py
"""
DAG para extraer datos del archivo Excel IDC desde Google Cloud Storage,
transformarlos mínimamente y cargarlos en la capa bronze de BigQuery.

El archivo SIEMPRE tiene 4 hojas:
1. Primera hoja (índice 0): Estructura/metadatos → SE ELIMINA SIEMPRE
2. Segunda hoja (índice 1): Dato_original → idc_raw_data_dato_original
3. Tercera hoja (índice 2): Valor_normalizado -> idc_raw_data_valor_normalizado
4. Cuarta hoja (índice 3): Valor_ranking -> idc_raw_data_valor_ranking

PROCESO AUTOMÁTICO (siempre se ejecuta):
1. Se elimina SIEMPRE la primera hoja del Excel (hoja de estructura/metadatos)
2. De las 3 hojas restantes, en cada una se elimina SIEMPRE la primera fila (fila de metadatos/estructura)
3. La segunda fila de cada hoja se usa como encabezados (nombres de columnas)
4. Las hojas se mapean por índice (no por nombre), por lo que el orden es crítico:
   - Primera hoja restante (índice 0) = Dato_original
   - Segunda hoja restante (índice 1) = Valor_normalizado
   - Tercera hoja restante (índice 2) = Valor_ranking
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
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
from modules.config import CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE
GCS_BUCKET_NAME = DEFAULT_BUCKET_NAME
GCS_FOLDER_PATH = CONF.idc.gcs_folder

# Mapeo de hojas del Excel a nombres de tablas en BigQuery
# Convertimos el SimpleNamespace a dict para que sea iterable como antes
SHEET_TO_TABLE_MAPPING = vars(CONF.idc.tables.raw_data_mapping)

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
    schedule=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:idc", "ejecución:manual"],
    description="Lee Excel IDC desde GCS (con 3 hojas), transforma mínimamente y carga a BigQuery en bronze_dpt_planeacion_municipal_dev como 3 tablas separadas. Luego ejecuta el DAG de transformación src_planeacion_transf_idc.",
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

