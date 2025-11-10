from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
import sys
import os
import shutil
import pathlib

def _clean_python_cache():
    """Limpia los archivos de caché de Python (.pyc y __pycache__) en el directorio del DAG."""
    dag_dir = pathlib.Path(__file__).parent
    for pyc_file in dag_dir.rglob("*.pyc"):
        try:
            pyc_file.unlink()
        except Exception:
            pass
    
    for cache_dir in dag_dir.rglob("__pycache__"):
        try:
            shutil.rmtree(cache_dir)
        except Exception:
            pass

# Limpiar caché al importar el módulo
_clean_python_cache()

# Agregar el directorio del DAG al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.load_rawdata_dictionary import load_dictionary_from_gcs_excel

# Configuración del archivo en GCS (archivo del diccionario)
GCS_URI = "gs://gdv_ipm_dane/24/10/2025/1_Estructura de datos IMP .xlsx"  # Archivo del diccionario (nota el espacio antes de .xlsx)
BUCKET_NAME = "gdv_ipm_dane"  # Nombre del bucket

# Directorio de dbt
# DBT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"

def load_data_task():
    """Task que carga el diccionario desde GCS a BigQuery"""
    return load_dictionary_from_gcs_excel(GCS_URI)


with DAG(
    dag_id="gdv_load_idictionary_raw_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # lo disparas manualmente
    catchup=False,
    tags=["load", "ipm", "raw"],
    description="DAG que carga el diccionario de datos IPM desde GCS a BigQuery (raw layer)",
) as dag:

    # Task 1: Cargar datos desde GCS a BigQuery
    load_data = PythonOperator(
        task_id="load_data_from_gcs",
        python_callable=load_data_task,
    )
    
    
