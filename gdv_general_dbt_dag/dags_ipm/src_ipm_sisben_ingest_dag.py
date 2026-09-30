# dags/src_ipm_sisben_ingest_dag.py
"""
DAG para mover archivos Excel desde la carpeta temporal a la carpeta destino en GCS para IPM SISBEN.
El archivo debe estar previamente en la carpeta 'temp' dentro de 'data_staging/dpt_planeacion_municipal/'.
El DAG lo mueve a la carpeta 'ipm/sisben' dentro de 'data_staging/dpt_planeacion_municipal/'.

El archivo debe estar disponible en: data_staging/dpt_planeacion_municipal/temp
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import sys

# Asegura que podamos importar el módulo local
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
from modules.ipm.ipm_sisben_ingest import move_file_within_gcs
from modules.config import CONF, DEFAULT_BUCKET_NAME

# === CONFIGURACIÓN ===
SOURCE_FOLDER = CONF.ipm_sisben.gcs_temp_folder
DESTINATION_FOLDER = CONF.ipm_sisben.gcs_folder

def _move_file_task():
    """
    Task que mueve el archivo desde la carpeta temporal a la carpeta destino en GCS.
    El archivo debe estar previamente en la carpeta 'temp' configurada en config.yaml.
    """
    bucket_name = DEFAULT_BUCKET_NAME
    source_folder = SOURCE_FOLDER
    destination_folder = DESTINATION_FOLDER
    
    print(f"[INFO] Iniciando movimiento de archivo dentro de GCS")
    print(f"[INFO] Bucket: {bucket_name}")
    print(f"[INFO] Carpeta origen: {source_folder}")
    print(f"[INFO] Carpeta destino: {destination_folder}")
    
    # Mover el archivo
    gcs_uri = move_file_within_gcs(
        bucket_name=bucket_name,
        source_folder=source_folder,
        destination_folder=destination_folder,
        file_pattern="*.xlsx"  # Solo archivos Excel
    )
    
    print(f"[OK] Archivo movido exitosamente a: {gcs_uri}")
    return gcs_uri

with DAG(
    dag_id="src_planeacion_ingest_ipm_sisben",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:ipm_sisben", "ejecución:manual"],
    description="Mueve el archivo Excel IPM SISBEN desde la carpeta temporal (temp) a la carpeta destino (ipm/sisben) dentro de data_staging/dpt_planeacion_municipal/ en GCS.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea principal: mover archivo dentro de GCS
    move_file = PythonOperator(
        task_id="move_file_within_gcs",
        python_callable=_move_file_task,
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> move_file >> end

