from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
import os

PROJECT_ID   = "datagov-473122"
GOLD_DATASET = "gdv_ipm_sisben_gold"
LOCATION     = "us-central1"
SA_PATH      = "/opt/airflow/include/sa.json"

DBT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"
DBT_ENV = {
    "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
    "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
}

def ensure_gold_dataset():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    client = bigquery.Client(project=PROJECT_ID)
    ds_id = f"{PROJECT_ID}.{GOLD_DATASET}"
    ds = bigquery.Dataset(ds_id)
    try:
        existing = client.get_dataset(ds_id)
        if existing.location != LOCATION:
            print(f"[WARN] {ds_id} en {existing.location}, se recreará en {LOCATION}")
            client.delete_dataset(ds_id, delete_contents=True, not_found_ok=True)
            ds.location = LOCATION
            ds.description = "Capa GOLD IPM SISBEN"
            client.create_dataset(ds)
            print(f"[OK] Dataset recreado: {ds_id} ({LOCATION})")
        else:
            print(f"[OK] Dataset ya existe: {ds_id} ({existing.location})")
    except Exception:
        ds.location = LOCATION
        ds.description = "Capa GOLD IPM SISBEN"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_id} ({LOCATION})")

with DAG(
    dag_id="gdv_build_fact_ipm_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt","gold","ipm"],
    description="Crea el dataset GOLD y construye gdv_ipm_sisben_fact desde dimensiones",
) as dag:

    ensure_ds = PythonOperator(
        task_id="ensure_gold_dataset",
        python_callable=ensure_gold_dataset,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_fact_ipm",
        env=DBT_ENV,
        bash_command=f"cd {DBT_DIR} && dbt run --select gdv_ipm_sisben_fact",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_fact_ipm",
        env=DBT_ENV,
        bash_command=f"cd {DBT_DIR} && dbt test --select gdv_ipm_sisben_fact",
    )

    ensure_ds >> dbt_run >> dbt_test
