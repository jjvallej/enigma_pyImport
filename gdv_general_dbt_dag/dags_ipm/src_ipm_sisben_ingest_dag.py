# dags/src_ipm_sisben_ingest_dag.py
"""
DAG para ingerir archivos IPM SISBEN desde la carpeta temporal en GCS
y copiarlos a la carpeta de destino ipm/sisben.

El archivo se encuentra en: gs://datalake_gdv_dev/data_staging/dpt_planeacion_municipal/tmp/IPM_SISBEN.xlsx
Y se copia a: datalake_gdv_dev/data_staging/dpt_planeacion_municipal/ipm/sisben
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
from modules.ipm.ipm_sisben_ingest import copy_file_from_gcs_uri_to_destination

# === CONFIGURACIÓN ===
# URL completa del archivo origen en GCS (puede ser de cualquier bucket/proyecto)
# Formato: 
#   - Archivo específico: gs://bucket-name/path/to/file.xlsx
#   - Carpeta (buscará el último .xlsx): gs://bucket-name/path/to/folder/
# URL del archivo IPM SISBEN en la carpeta temporal
SOURCE_GCS_URI = "gs://datalake_gdv_dev/data_staging/dpt_planeacion_municipal/tmp/IPM_SISBEN.xlsx"

# Configuración del destino
DESTINATION_BUCKET_NAME = "datalake_gdv_dev"
DESTINATION_FOLDER = "data_staging/dpt_planeacion_municipal/ipm/sisben"  # Carpeta destino IPM SISBEN
FILE_EXTENSION = ".xlsx"  # Extensión a buscar si SOURCE_GCS_URI es una carpeta

def _copy_file_ipm_sisben_task():
    """
    Task que copia el archivo IPM SISBEN desde la URI origen a la carpeta de destino.
    Si SOURCE_GCS_URI apunta a una carpeta, buscará el último archivo con FILE_EXTENSION.
    Si apunta a un archivo específico, copiará ese archivo directamente.
    """
    print(f"[INFO] Iniciando copia de archivo IPM SISBEN")
    print(f"[INFO] URI origen: {SOURCE_GCS_URI}")
    print(f"[INFO] Bucket destino: {DESTINATION_BUCKET_NAME}")
    print(f"[INFO] Carpeta destino: {DESTINATION_FOLDER}")
    print(f"[INFO] Extensión buscada (si es carpeta): {FILE_EXTENSION}")
    
    # Copiar el archivo (la función detecta automáticamente si es archivo o carpeta)
    gcs_uri = copy_file_from_gcs_uri_to_destination(
        source_gcs_uri=SOURCE_GCS_URI,
        destination_bucket_name=DESTINATION_BUCKET_NAME,
        destination_folder=DESTINATION_FOLDER,
        file_extension=FILE_EXTENSION,
        overwrite=True
    )
    
    print(f"[OK] Archivo IPM SISBEN copiado exitosamente a: {gcs_uri}")
    return gcs_uri

with DAG(
    dag_id="src_planeacion_ingest_ipm_sisben",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:ipm_sisben", "ejecución:manual"],
    description="Ingiere el archivo Excel IPM SISBEN desde la carpeta temporal (tmp) en GCS y lo copia a la carpeta ipm/sisben dentro de data_staging/dpt_planeacion_municipal/. Luego ejecuta el DAG de carga correspondiente.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea para copiar archivo IPM SISBEN desde tmp a ipm/sisben
    copy_file_ipm_sisben = PythonOperator(
        task_id="copy_file_ipm_sisben_from_tmp_to_sisben",
        python_callable=_copy_file_ipm_sisben_task,
    )

    # Tarea para ejecutar el DAG de carga (después de que el archivo se haya copiado)
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_ipm_sisben",
        trigger_dag_id="src_planeacion_load_ipm_sisben",
        wait_for_completion=True,  # Espera a que el DAG de carga termine
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: se copia el archivo IPM SISBEN, luego se dispara el DAG de carga
    start >> copy_file_ipm_sisben >> trigger_load_dag >> end

