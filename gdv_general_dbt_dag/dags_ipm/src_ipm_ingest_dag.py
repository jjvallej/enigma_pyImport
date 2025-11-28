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
# Agregar el directorio raíz del proyecto al path para importar módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from modules.ipm.ipm_ingest import move_file_from_drive_to_gcs

# === CONFIGURACIÓN ===
DEFAULT_BUCKET_NAME = "datalake_gdv_dev"  # Cambiar según el bucket deseado
DEFAULT_FOLDER_NAME = "data_staging/dpt_planeacion_municipal/ipm"  # Carpeta ipm (minúsculas) dentro de dpt_planeacion_municipal
DRIVE_URL_IPM = "https://docs.google.com/spreadsheets/d/1uXHTK64SVXmsV-nKGXT8Vz7u_b4gXvYs/edit?usp=drive_link&ouid=109263228047844968910&rtpof=true&sd=true"  # URL fija del archivo IPM en Google Drive
DRIVE_URL_IPM_SISBEN = "https://docs.google.com/spreadsheets/d/1lAiRpLoTs4iA7ViqWCCnTwip4hRZMDWyzqicwdmgXFw/edit?usp=sharing"  # URL del archivo IPM SISBEN en Google Drive

def _upload_file_ipm_task():
    """
    Task que mueve el archivo IPM desde Google Drive a GCS.
    Usa la URL hardcodeada del archivo IPM.
    El archivo se sube a la subcarpeta 'dane' dentro de la carpeta ipm.
    """
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = f"{DEFAULT_FOLDER_NAME}/dane"  # Subcarpeta 'dane' dentro de ipm
    destination_file_name = None  # Se extraerá automáticamente del archivo
    
    print(f"[INFO] Iniciando transferencia de archivo IPM desde Google Drive")
    print(f"[INFO] Drive URL/ID: {DRIVE_URL_IPM}")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    print(f"[INFO] Método: Enlace Público")
    print(f"[INFO] Nombre del archivo: se extraerá automáticamente del Drive")
    
    # Mover el archivo
    gcs_uri = move_file_from_drive_to_gcs(
        drive_url_or_id=DRIVE_URL_IPM,
        bucket_name=bucket_name,
        folder_name=folder_name,
        destination_file_name=destination_file_name
    )
    
    print(f"[OK] Archivo IPM transferido exitosamente a: {gcs_uri}")
    return gcs_uri

def _upload_file_ipm_sisben_task():
    """
    Task que mueve el archivo IPM SISBEN desde Google Drive a GCS.
    Usa la URL hardcodeada del archivo IPM SISBEN.
    El archivo se sube a la subcarpeta 'sisben' dentro de la carpeta ipm.
    """
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = f"{DEFAULT_FOLDER_NAME}/sisben"  # Subcarpeta 'sisben' dentro de ipm
    destination_file_name = None  # Se extraerá automáticamente del archivo
    
    print(f"[INFO] Iniciando transferencia de archivo IPM SISBEN desde Google Drive")
    print(f"[INFO] Drive URL/ID: {DRIVE_URL_IPM_SISBEN}")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    print(f"[INFO] Método: Enlace Público")
    print(f"[INFO] Nombre del archivo: se extraerá automáticamente del Drive")
    
    # Mover el archivo
    gcs_uri = move_file_from_drive_to_gcs(
        drive_url_or_id=DRIVE_URL_IPM_SISBEN,
        bucket_name=bucket_name,
        folder_name=folder_name,
        destination_file_name=destination_file_name
    )
    
    print(f"[OK] Archivo IPM SISBEN transferido exitosamente a: {gcs_uri}")
    return gcs_uri

with DAG(
    dag_id="src_planeacion_ingest_ipm",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:ipm", "ejecución:manual"],
    description="Ingiere los archivos Excel IPM e IPM SISBEN desde Google Drive (enlaces públicos fijos), los sube a GCS en la carpeta ipm dentro de data_staging/dpt_planeacion_municipal/, y luego ejecuta el DAG de carga src_planeacion_load_ipm que carga los datos a BigQuery.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea para mover archivo IPM desde Drive a GCS
    upload_file_ipm = PythonOperator(
        task_id="upload_file_ipm_from_drive_to_gcs",
        python_callable=_upload_file_ipm_task,
    )

    # Tarea para mover archivo IPM SISBEN desde Drive a GCS
    upload_file_ipm_sisben = PythonOperator(
        task_id="upload_file_ipm_sisben_from_drive_to_gcs",
        python_callable=_upload_file_ipm_sisben_task,
    )

    # Tarea para ejecutar el DAG de carga (solo después de que ambos archivos se hayan subido)
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_ipm",
        trigger_dag_id="src_planeacion_load_ipm",
        wait_for_completion=True,  # Espera a que el DAG de carga termine
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: ambas tareas de upload se ejecutan en paralelo, luego se dispara el DAG de carga
    start >> [upload_file_ipm, upload_file_ipm_sisben] >> trigger_load_dag >> end
