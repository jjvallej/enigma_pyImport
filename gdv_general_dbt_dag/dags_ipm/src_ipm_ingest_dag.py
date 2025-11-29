# dags/src_ipm_ingest_dag.py
"""
DAG para ingerir archivos Excel desde Google Drive (enlace público) a Google Cloud Storage.
Ingiere el archivo desde Drive usando un enlace público y lo sube al bucket GCS 
en la carpeta 'ipm' dentro de 'data_staging/dpt_planeacion_municipal/'.

Requiere: Enlace público de Google Drive (no requiere autenticación)
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

# Asegura que podamos importar el módulo local
import sys
import os

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
from modules.ipm.ipm_ingest import move_file_from_drive_to_gcs
from modules.config import CONF

# === CONFIGURACIÓN ===
DEFAULT_BUCKET_NAME = "datalake_gdv_dev"  # Cambiar según el bucket deseado
DEFAULT_FOLDER_NAME = CONF.ipm.gcs_folder
DRIVE_URL = CONF.ipm.drive_url

def _upload_file_task():
    """
    Task que mueve el archivo desde Google Drive a GCS.
    Usa la URL hardcodeada del archivo IPM.
    El archivo se sube a la subcarpeta 'dane' dentro de la carpeta ipm.
    """
    # Todos los parámetros se configuran automáticamente:
    # - drive_url_or_id: URL hardcodeada del archivo IPM
    # - bucket_name: usa valor por defecto
    # - folder_name: usa valor por defecto + subcarpeta dane (data_staging/dpt_planeacion_municipal/ipm/dane)
    # - use_public_link: siempre True (más fácil)
    # - destination_file_name: se extraerá automáticamente del nombre del archivo en Drive
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = f"{DEFAULT_FOLDER_NAME}/dane"  # Subcarpeta 'dane' dentro de ipm
    destination_file_name = None  # Se extraerá automáticamente del archivo
    
    print(f"[INFO] Iniciando transferencia de archivo desde Google Drive")
    print(f"[INFO] Drive URL/ID: {DRIVE_URL}")
    print(f"[INFO] Bucket destino: {bucket_name} (configurado automáticamente)")
    print(f"[INFO] Carpeta destino: {folder_name} (configurado automáticamente, incluye subcarpeta dane/)")
    print(f"[INFO] Método: Enlace Público")
    print(f"[INFO] Nombre del archivo: se extraerá automáticamente del Drive")
    
    # Mover el archivo
    gcs_uri = move_file_from_drive_to_gcs(
        drive_url_or_id=DRIVE_URL,
        bucket_name=bucket_name,
        folder_name=folder_name,
        destination_file_name=destination_file_name
    )
    
    print(f"[OK] Archivo transferido exitosamente a: {gcs_uri}")
    return gcs_uri

with DAG(
    dag_id="src_planeacion_ingest_ipm",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:ipm", "ejecución:manual"],
    description="Ingiere el archivo Excel IPM desde Google Drive (enlace público fijo), lo sube a GCS en la carpeta ipm/dane dentro de data_staging/dpt_planeacion_municipal/, y luego dispara el DAG de carga src_planeacion_load_ipm (sin esperar a que termine).",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea principal: mover archivo desde Drive a GCS
    upload_file = PythonOperator(
        task_id="upload_file_from_drive_to_gcs",
        python_callable=_upload_file_task,
    )

    # Tarea para ejecutar el DAG de carga
    # wait_for_completion=False permite que este DAG termine exitosamente
    # sin esperar a que el DAG de carga termine completamente
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_ipm",
        trigger_dag_id="src_planeacion_load_ipm",
        wait_for_completion=False,  # Dispara el DAG de carga y continúa sin esperar
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> upload_file >> trigger_load_dag >> end
