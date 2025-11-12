# dags/load_rawdata_ipmv2_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import os, sys, tempfile
import pandas as pd

# Asegura que podamos importar el módulo local
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.load_rawdata_ipmv2 import (
    ensure_dataset,
    download_excel_from_gcs,
    transform_excel,
    load_dataframe_to_bq,
    cleanup_temp_paths,
)

# === CONFIGURACIÓN ===
GCS_URI = "gs://gdv_ipm_dane/24/10/2025/2_IPM_DANE.xlsx"   # <--- cambia si es necesario
DATASET_ID = "gdv_ipmv2_bronze"
TABLE_NAME = "rawdata_ipmv2"
SHEET_INDEX = 0

def _ensure_dataset_task():
    ensure_dataset(dataset_id=DATASET_ID)

def _download_excel_task():
    return download_excel_from_gcs(gcs_uri=GCS_URI)

def _transform_task(ti):
    local_excel_path = ti.xcom_pull(task_ids="download_excel")
    if not local_excel_path:
        raise ValueError("No se recibió la ruta del Excel en XCom (task download_excel).")
    df = transform_excel(local_path=local_excel_path, sheet_index=SHEET_INDEX)
    tmp = tempfile.NamedTemporaryFile(suffix=".pkl", delete=False)
    tmp_path = tmp.name
    tmp.close()
    df.to_pickle(tmp_path)
    return tmp_path

def _load_task(ti):
    pickle_path = ti.xcom_pull(task_ids="transform_dataframe")
    if not pickle_path:
        raise ValueError("No se recibió la ruta del DataFrame transformado en XCom (task transform_dataframe).")
    df = pd.read_pickle(pickle_path)
    load_dataframe_to_bq(df, dataset_id=DATASET_ID, table_name=TABLE_NAME)

def _cleanup_temp_files_task(ti):
    excel_path = ti.xcom_pull(task_ids="download_excel")
    pickle_path = ti.xcom_pull(task_ids="transform_dataframe")
    cleanup_temp_paths([excel_path, pickle_path])

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

    t2_download_excel = PythonOperator(
        task_id="download_excel",
        python_callable=_download_excel_task,
    )

    t3_transform_dataframe = PythonOperator(
        task_id="transform_dataframe",
        python_callable=_transform_task,
    )

    t4_load_to_bq = PythonOperator(
        task_id="load_to_bq",
        python_callable=_load_task,
    )

    t5_cleanup_temp_files = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=_cleanup_temp_files_task,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t1_ensure_dataset >> t2_download_excel >> t3_transform_dataframe >> t4_load_to_bq >> t5_cleanup_temp_files
