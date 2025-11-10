from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Agregar el directorio del DAG al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.clean_rawdata_ipm import run_clean_to_bronze

def clean_data_task():
    """Task que limpia datos y los guarda en el dataset bronze"""
    return run_clean_to_bronze()

with DAG(
    dag_id="gdv_clean_ipm_bronze_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # lo disparas manualmente
    catchup=False,
    description="DAG que limpia datos IPM de RAW y los guarda en BRONZE",
    tags=["clean", "bronze", "ipm"],
) as dag:

    # Task: Limpiar datos y guardar en dataset bronze
    clean_data = PythonOperator(
        task_id="clean_data_to_bronze",
        python_callable=clean_data_task,
    )
