from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
import sys
import os

# Agregar el directorio del DAG al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.load_rawdata_ipm import load_ipm_wide_from_gcs_excel

# Configuración del archivo en GCS
GCS_URI = "gs://gdv_ipm_dane/24/10/2025/2_IPM_DANE.xlsx"  # Cambia por tu URI real

# Directorio de dbt
# DBT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"

def load_data_task():
    """Task que carga datos desde GCS a BigQuery"""
    return load_ipm_wide_from_gcs_excel(GCS_URI)

with DAG(
    dag_id="gdv_load_ipm_raw_data_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # lo disparas manualmente
    catchup=False,
    tags=["load", "ipm", "raw"],
    description="DAG que carga datos IPM desde GCS a BigQuery (raw layer)",
) as dag:

    # Task 1: Cargar datos desde GCS a BigQuery
    load_data = PythonOperator(
        task_id="load_data_from_gcs",
        python_callable=load_data_task,
    )

    # Task 2: Ejecutar dbt run (consulta básica)
    # dbt_run = BashOperator(
    #     task_id="dbt_run",
    #     bash_command=f"cd {DBT_DIR} && export GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/include/sa.json && export DBT_PROFILES_DIR=/opt/airflow/include/dbt && dbt run --no-write-json",
    # )

    # Definir dependencias: primero carga datos, luego ejecuta dbt
    # load_data >> dbt_run
