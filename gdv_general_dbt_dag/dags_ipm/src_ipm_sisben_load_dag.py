# dags/src_ipm_sisben_load_dag.py
"""
DAG para extraer datos del archivo Excel IPM SISBEN desde Google Cloud Storage
y cargarlos tal cual están (sin transformaciones) en la capa bronze de BigQuery.

El archivo se lee desde: datalake_gdv_dev/data_staging/dpt_planeacion_municipal/ipm/sisben
Y se carga en: bronze_dpt_planeacion_municipal_dev.ipm_sisben_raw_data
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os, sys

# Asegura que podamos importar el módulo local
# Agregar el directorio raíz del proyecto al path para importar módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from modules.ipm.ipm_load import (
    ensure_dataset,
    get_latest_excel_from_gcs_folder,
    convert_excel_to_csv_in_gcs,
    load_csv_from_gcs_to_bq_raw,
)

# === CONFIGURACIÓN ===
# Configuración para buscar el último archivo Excel en la carpeta ipm/sisben
# El DAG buscará automáticamente el archivo .xlsx más reciente en esta carpeta
GCS_BUCKET_NAME = "datalake_gdv_dev"
GCS_FOLDER_PATH_SISBEN = "data_staging/dpt_planeacion_municipal/ipm/sisben"  # Carpeta sisben dentro de ipm (también para CSV)
DATASET_ID_BRONZE = "bronze_dpt_planeacion_municipal_dev"
TABLE_NAME_BRONZE_SISBEN = "ipm_sisben_raw_data"
SHEET_INDEX = 0  # Primera hoja del Excel

def _ensure_dataset_bronze_task():
    """Asegura que el dataset bronze existe, si no lo crea."""
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _get_gcs_uri_task():
    """Obtiene la URI del último archivo Excel en la carpeta sisben."""
    try:
        # Intentar obtener configuración desde Variables de Airflow
        bucket_name = Variable.get("ipm_sisben_gcs_bucket", default_var=GCS_BUCKET_NAME)
        folder_path = Variable.get("ipm_sisben_gcs_folder", default_var=GCS_FOLDER_PATH_SISBEN)
        print(f"[INFO] Usando configuración: bucket={bucket_name}, carpeta={folder_path}")
    except:
        # Si no existen las variables, usar valores por defecto hardcodeados
        bucket_name = GCS_BUCKET_NAME
        folder_path = GCS_FOLDER_PATH_SISBEN
        print(f"[INFO] Usando configuración por defecto: bucket={bucket_name}, carpeta={folder_path}")
    
    # Buscar el último archivo Excel en la carpeta
    print(f"[INFO] Buscando el último archivo .xlsx en gs://{bucket_name}/{folder_path}")
    gcs_uri = get_latest_excel_from_gcs_folder(bucket_name=bucket_name, folder_path=folder_path)
    
    print(f"[INFO] Archivo encontrado: {gcs_uri}")
    return gcs_uri

def _convert_excel_to_csv_task(ti):
    """
    Convierte el archivo Excel a CSV y lo sube a GCS.
    Esto permite cargar directamente desde GCS a BigQuery (mucho más rápido).
    """
    gcs_excel_uri = ti.xcom_pull(task_ids="bronze_sisben.get_gcs_uri")
    if not gcs_excel_uri:
        raise ValueError("No se recibió la URI del archivo en XCom (task get_gcs_uri).")
    
    # Generar nombre del CSV (mismo nombre pero con extensión .csv)
    csv_filename = gcs_excel_uri.split('/')[-1].replace('.xlsx', '.csv').replace('.xls', '.csv')
    gcs_csv_uri = f"gs://{GCS_BUCKET_NAME}/{GCS_FOLDER_PATH_SISBEN}/{csv_filename}"
    
    print(f"[INFO] Convirtiendo Excel a CSV para carga optimizada")
    print(f"[INFO] Excel origen: {gcs_excel_uri}")
    print(f"[INFO] CSV destino: {gcs_csv_uri}")
    
    # Convertir Excel a CSV en GCS
    csv_uri = convert_excel_to_csv_in_gcs(
        gcs_excel_uri=gcs_excel_uri,
        gcs_csv_uri=gcs_csv_uri,
        sheet_index=SHEET_INDEX,
        delete_excel_after=False  # Mantener el Excel original
    )
    
    print(f"[OK] CSV creado exitosamente: {csv_uri}")
    return csv_uri

def _load_csv_to_bq_task(ti):
    """
    Carga el CSV directamente desde GCS a BigQuery.
    MUCHO más rápido que cargar desde DataFrame porque BigQuery lee directamente desde GCS.
    """
    gcs_csv_uri = ti.xcom_pull(task_ids="bronze_sisben.convert_excel_to_csv")
    if not gcs_csv_uri:
        raise ValueError("No se recibió la URI del CSV en XCom (task convert_excel_to_csv).")
    
    print(f"[INFO] Cargando CSV directamente desde GCS a BigQuery")
    print(f"[INFO] CSV URI: {gcs_csv_uri}")
    print(f"[INFO] Dataset: {DATASET_ID_BRONZE}")
    print(f"[INFO] Tabla: {TABLE_NAME_BRONZE_SISBEN}")
    
    # Cargar CSV directamente desde GCS a BigQuery (muy rápido)
    load_csv_from_gcs_to_bq_raw(
        gcs_csv_uri=gcs_csv_uri,
        dataset_id=DATASET_ID_BRONZE,
        table_name=TABLE_NAME_BRONZE_SISBEN
    )

with DAG(
    dag_id="src_planeacion_load_ipm_sisben",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:carga", "fuente:ipm_sisben", "ejecución:manual"],
    description="Lee Excel IPM SISBEN desde GCS (carpeta ipm/sisben) y lo carga tal cual está (sin transformaciones) a BigQuery en bronze_dpt_planeacion_municipal_dev.ipm_sisben_raw_data.",
) as dag:

    # Tarea inicial vacía
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la carga del archivo IPM SISBEN (capa bronze)
    with TaskGroup(group_id="bronze_sisben") as bronze_sisben_group:
        t1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_bronze_task,
        )

        t2_get_gcs_uri = PythonOperator(
            task_id="get_gcs_uri",
            python_callable=_get_gcs_uri_task,
        )

        t3_convert_to_csv = PythonOperator(
            task_id="convert_excel_to_csv",
            python_callable=_convert_excel_to_csv_task,
        )

        t4_load_csv_to_bq = PythonOperator(
            task_id="load_csv_to_bq",
            python_callable=_load_csv_to_bq_task,
        )

        t1_ensure_dataset >> t2_get_gcs_uri >> t3_convert_to_csv >> t4_load_csv_to_bq

    # Tarea para ejecutar el DAG de transformación (cuando esté listo)
    # trigger_transf_dag = TriggerDagRunOperator(
    #     task_id="trigger_transf_ipm_sisben",
    #     trigger_dag_id="src_planeacion_transf_ipm_sisben",
    #     wait_for_completion=True,  # Espera a que el DAG de transformación termine
    # )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> bronze_sisben -> end
    # Cuando el DAG de transformación esté listo, descomentar y actualizar:
    # start >> bronze_sisben_group >> trigger_transf_dag >> end
    start >> bronze_sisben_group >> end

