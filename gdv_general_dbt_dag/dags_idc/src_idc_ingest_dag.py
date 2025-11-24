# dags/src_idc_ingest_dag.py
"""
DAG para ingerir archivos Excel desde Google Drive (enlace público) a Google Cloud Storage.
Ingiere el archivo desde Drive usando un enlace público y lo sube al bucket GCS 
en la carpeta 'idc' dentro de 'data_staging/dpt_planeacion_municipal/'.

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
from modules.idc.idc_ingest import move_file_from_drive_to_gcs

# === CONFIGURACIÓN ===
DEFAULT_BUCKET_NAME = "datalake_gdv"  # Cambiar según el bucket deseado
DEFAULT_FOLDER_NAME = "data_staging/dpt_planeacion_municipal/idc"  # Carpeta idc (minúsculas) dentro de dpt_planeacion_municipal
DRIVE_URL = "https://docs.google.com/spreadsheets/d/1J42xKC3VnkjhwHQENKxo2DDQzuBINYxY/edit?usp=drive_link&ouid=109263228047844968910&rtpof=true&sd=true"  # URL fija del archivo IDC en Google Drive

def _upload_file_task():
    """
    Task que mueve el archivo desde Google Drive a GCS.
    Usa la URL hardcodeada del archivo IDC.
    """
    # Todos los parámetros se configuran automáticamente:
    # - drive_url_or_id: URL hardcodeada del archivo IDC
    # - bucket_name: usa valor por defecto
    # - folder_name: usa valor por defecto (data_staging/dpt_planeacion_municipal/idc)
    # - destination_file_name: se extrae automáticamente del nombre del archivo en Drive
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = DEFAULT_FOLDER_NAME
    destination_file_name = None  # Se extraerá automáticamente del archivo
    
    print(f"[INFO] Iniciando transferencia de archivo desde Google Drive")
    print(f"[INFO] Drive URL/ID: {DRIVE_URL}")
    print(f"[INFO] Bucket destino: {bucket_name} (configurado automáticamente)")
    print(f"[INFO] Carpeta destino: {folder_name} (configurado automáticamente)")
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
    dag_id="src_planeacion_ingest_idc",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:idc", "ejecución:manual"],
    description="Ingiere el archivo Excel IDC desde Google Drive (enlace público fijo), lo sube a GCS en la carpeta idc dentro de data_staging/dpt_planeacion_municipal/, y luego ejecuta el DAG de carga src_planeacion_load_idc que carga los datos a BigQuery.",
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
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_idc",
        trigger_dag_id="src_planeacion_load_idc",
        wait_for_completion=True,  # Espera a que el DAG de carga termine
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> upload_file >> trigger_load_dag >> end

