# dags/upload_excel_to_gcs_dag.py
"""
DAG para mover archivos Excel desde Google Drive (enlace público) a Google Cloud Storage.
Descarga el archivo desde Drive y lo sube al bucket GCS en la carpeta 'ipm' dentro de 'data_staging/dpt_planeacion_municipal/'.

USO MÁS SIMPLE: Con enlace público de Google Drive (no requiere autenticación)
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os

# Asegura que podamos importar el módulo local
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.upload_excel_to_gcs import move_file_from_drive_to_gcs

# === CONFIGURACIÓN ===
DEFAULT_BUCKET_NAME = "datalake_gdv"  # Cambiar según el bucket deseado
DEFAULT_FOLDER_NAME = "data_staging/dpt_planeacion_municipal/ipm"  # Carpeta ipm (minúsculas) dentro de dpt_planeacion_municipal

def _upload_file_task(**context):
    """
    Task que mueve el archivo desde Google Drive a GCS.
    Solo requiere la URL del archivo, todo lo demás se maneja automáticamente.
    """
    # Obtener drive_url desde parámetros (obligatorio)
    drive_url_or_id = context.get("params", {}).get("drive_url")
    
    if not drive_url_or_id:
        raise ValueError(
            "Se requiere la URL del archivo de Google Drive.\n\n"
            "Pásala como parámetro 'drive_url' al ejecutar el DAG.\n"
            "Ejemplo: https://drive.google.com/file/d/FILE_ID/view?usp=sharing"
        )
    
    # Todos los demás parámetros se configuran automáticamente:
    # - bucket_name: usa valor por defecto
    # - folder_name: usa valor por defecto (data_staging/dpt_planeacion_municipal/ipm)
    # - use_public_link: siempre True (más fácil)
    # - destination_file_name: se extrae automáticamente del nombre del archivo en Drive
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = DEFAULT_FOLDER_NAME
    use_public_link = True
    destination_file_name = None  # Se extraerá automáticamente del archivo
    
    print(f"[INFO] Iniciando transferencia de archivo desde Google Drive")
    print(f"[INFO] Drive URL/ID: {drive_url_or_id}")
    print(f"[INFO] Bucket destino: {bucket_name} (configurado automáticamente)")
    print(f"[INFO] Carpeta destino: {folder_name} (configurado automáticamente)")
    print(f"[INFO] Método: Enlace Público (automático)")
    print(f"[INFO] Nombre del archivo: se extraerá automáticamente del Drive")
    
    # Mover el archivo
    gcs_uri = move_file_from_drive_to_gcs(
        drive_url_or_id=drive_url_or_id,
        bucket_name=bucket_name,
        folder_name=folder_name,
        destination_file_name=destination_file_name,
        use_public_link=use_public_link
    )
    
    print(f"[OK] Archivo transferido exitosamente a: {gcs_uri}")
    return gcs_uri

with DAG(
    dag_id="scr_planeacion_inges_ipm",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:ipm", "ejecución:manual"],
    description="Descarga un archivo Excel desde Google Drive (enlace público) y lo sube a GCS en la carpeta ipm dentro de data_staging/dpt_planeacion_municipal/. Solo requiere la URL del archivo.",
    params={
        "drive_url": None,  # REQUERIDO: URL pública de Google Drive o File ID
    },
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea principal: mover archivo desde Drive a GCS
    upload_file = PythonOperator(
        task_id="upload_file_from_drive_to_gcs",
        python_callable=_upload_file_task,
        provide_context=True,
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> upload_file >> end

