# dags/src_idc_dictionary_load_dag.py
"""
DAG para extraer datos del archivo CSV del diccionario IDC desde Google Cloud Storage,
transformarlos mínimamente y cargarlos en la capa gold de BigQuery.

El archivo CSV contiene el diccionario de indicadores con la estructura:
ID_FACTOR,ID_PILAR,ID_INDICADOR,ID_SUBINDICADOR,NOM_FACTOR,NOM_PILAR,NOM_INDICADOR,NOM_SUBINDICADOR

La tabla se crea en el dataset gold_dpt_planeacion_municipal_dev con el nombre dim_idc.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os, sys

# Asegura que podamos importar el módulo local
# Agregar el directorio raíz del proyecto al path para importar módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from modules.idc.idc_dictionary_load import (
    ensure_dataset,
    download_csv_from_gcs,
    get_latest_csv_from_gcs_folder,
    load_csv_to_bq,
    cleanup_temp_paths,
)

# === CONFIGURACIÓN ===
# Configuración para buscar el último archivo CSV en la carpeta idc
# El DAG buscará automáticamente el archivo .csv más reciente en esta carpeta
GCS_BUCKET_NAME = "datalake_gdv"
GCS_FOLDER_PATH = "data_staging/dpt_planeacion_municipal/idc"
DATASET_ID_GOLD = "gold_dpt_planeacion_municipal_dev"
TABLE_NAME = "dim_idc"

def _ensure_dataset_gold_task():
    """Asegura que el dataset gold exista."""
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

def _download_csv_task():
    """Descarga el último archivo CSV de la carpeta en GCS."""
    # Obtener el último archivo CSV de la carpeta en GCS
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
    
    # Buscar el último archivo CSV en la carpeta
    print(f"[INFO] Buscando el último archivo .csv en gs://{bucket_name}/{folder_path}")
    gcs_uri = get_latest_csv_from_gcs_folder(bucket_name=bucket_name, folder_path=folder_path)
    
    print(f"[INFO] Descargando archivo desde GCS: {gcs_uri}")
    return download_csv_from_gcs(gcs_uri=gcs_uri)

def _load_csv_task(ti):
    """
    Carga el archivo CSV a BigQuery.
    """
    local_csv_path = ti.xcom_pull(task_ids="gold.download_csv")
    if not local_csv_path:
        raise ValueError("No se recibió la ruta del CSV en XCom (task download_csv).")
    
    print(f"[INFO] Cargando archivo CSV a BigQuery")
    print(f"[INFO] Tabla destino: {DATASET_ID_GOLD}.{TABLE_NAME}")
    
    # Cargar el CSV
    table_name = load_csv_to_bq(
        local_path=local_csv_path,
        dataset_id=DATASET_ID_GOLD,
        table_name=TABLE_NAME
    )
    
    print(f"[OK] Archivo CSV cargado exitosamente en la tabla: {table_name}")
    return table_name

def _cleanup_temp_files_task(ti):
    """Limpia los archivos temporales."""
    csv_path = ti.xcom_pull(task_ids="gold.download_csv")
    cleanup_temp_paths([csv_path])

with DAG(
    dag_id="src_planeacion_load_idc_dictionary",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:idc_dictionary", "ejecución:manual"],
    description="Lee CSV del diccionario IDC desde GCS, transforma mínimamente y carga a BigQuery en gold_dpt_planeacion_municipal_dev como tabla dim_idc.",
) as dag:

    # Tarea inicial vacía
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la carga (capa gold)
    with TaskGroup(group_id="gold") as gold_group:
        t1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        t2_download_csv = PythonOperator(
            task_id="download_csv",
            python_callable=_download_csv_task,
        )

        t3_load_csv = PythonOperator(
            task_id="load_csv",
            python_callable=_load_csv_task,
        )

        t4_cleanup_temp_files = PythonOperator(
            task_id="cleanup_temp_files",
            python_callable=_cleanup_temp_files_task,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        t1_ensure_dataset >> t2_download_csv >> t3_load_csv >> t4_cleanup_temp_files

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> gold_group >> end

