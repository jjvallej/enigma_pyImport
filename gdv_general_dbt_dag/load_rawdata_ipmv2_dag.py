# dags/load_rawdata_ipmv2_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os, sys

# Asegura que podamos importar el módulo local
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.load_rawdata_ipmv2 import ensure_dataset, process_and_load_from_gcs

# === CONFIGURACIÓN ===
GCS_URI = "gs://gdv_ipm_dane/24/10/2025/2_IPM_DANE.xlsx"   # <--- cambia si es necesario
DATASET_ID = "gdv_ipmv2_bronze"
TABLE_NAME = "rawdata_ipmv2"

def _ensure_dataset_task():
    ensure_dataset(dataset_id=DATASET_ID)

def _process_and_load_task():
    process_and_load_from_gcs(
        gcs_uri=GCS_URI,
        dataset_id=DATASET_ID,
        table_name=TABLE_NAME
    )

with DAG(
    dag_id="gdv_load_rawdata_ipmv2_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["load", "ipm", "bronze", "ipmv2"],
    description="Lee Excel IPM desde GCS, quita primera fila, renombra columnas (_Abs/_Porc), agrega fecha_lectura y carga a BQ (gdv_ipmv2_bronze.rawdata_ipmv2).",
) as dag:

    t1_ensure_dataset = PythonOperator(
        task_id="ensure_dataset",
        python_callable=_ensure_dataset_task,
    )

    t2_process_and_load = PythonOperator(
        task_id="process_and_load_from_gcs",
        python_callable=_process_and_load_task,
    )

    t1_ensure_dataset >> t2_process_and_load
