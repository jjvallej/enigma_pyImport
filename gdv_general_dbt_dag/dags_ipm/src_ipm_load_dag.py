# dags/src_ipm_load_dag.py
"""
DAG para extraer datos del archivo Excel IPM desde Google Cloud Storage,
transformarlos mínimamente y cargarlos en la capa bronze de BigQuery.
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
import tempfile
import pandas as pd

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

from modules.ipm.ipm_load import (
    ensure_dataset,
    download_excel_from_gcs,
    get_latest_excel_from_gcs_folder,
    transform_excel,
    load_dataframe_to_bq,
    cleanup_temp_paths,
)

# === CONFIGURACIÓN ===
# Configuración para buscar el último archivo Excel en la carpeta ipm
# El DAG buscará automáticamente el archivo .xlsx más reciente en esta carpeta
from modules.config import CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE
GCS_BUCKET_NAME = DEFAULT_BUCKET_NAME
GCS_FOLDER_PATH = CONF.ipm.gcs_folder
TABLE_NAME_BRONZE = CONF.ipm.tables.bronze
SHEET_INDEX = 0

def _ensure_dataset_bronze_task():
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _download_excel_task():
    # Obtener el último archivo Excel de la carpeta en GCS
    try:
        # Intentar obtener configuración desde Variables de Airflow
        bucket_name = Variable.get("ipm_gcs_bucket", default_var=GCS_BUCKET_NAME)
        folder_path = Variable.get("ipm_gcs_folder", default_var=GCS_FOLDER_PATH)
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
    load_dataframe_to_bq(df, dataset_id=DATASET_ID_BRONZE, table_name=TABLE_NAME_BRONZE)

def _cleanup_temp_files_task(ti):
    excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    pickle_path = ti.xcom_pull(task_ids="bronze.transform_dataframe")
    cleanup_temp_paths([excel_path, pickle_path])

with DAG(
    dag_id="src_planeacion_load_ipm",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:carga", "fuente:ipm", "ejecución:manual"],
    description="Lee Excel IPM desde GCS, transforma mínimamente y carga a BigQuery en bronze_dpt_planeacion_municipal_dev.ipm_raw_data, luego dispara el DAG de transformación src_planeacion_transf_ipm (sin esperar a que termine).",
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

    # Tarea para ejecutar el DAG de transformación
    # wait_for_completion=False permite que este DAG termine exitosamente
    # sin esperar a que el DAG de transformación termine completamente
    trigger_transf_dag = TriggerDagRunOperator(
        task_id="trigger_transf_ipm",
        trigger_dag_id="src_planeacion_transf_ipm",
        wait_for_completion=False,  # Dispara el DAG de transformación y continúa sin esperar
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> bronze -> trigger_transf_dag -> end
    start >> bronze_group >> trigger_transf_dag >> end
