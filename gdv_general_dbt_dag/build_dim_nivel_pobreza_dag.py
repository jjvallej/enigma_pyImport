from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
import os

PROJECT_ID   = "datagov-473122"
DIMS_DATASET = "gdv_ipm_sisben_dims"
LOCATION     = "us-central1"
SA_PATH      = "/opt/airflow/include/sa.json"

DBT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"
DBT_ENV = {
    "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
    "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
}

def ensure_dims_dataset():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    client = bigquery.Client(project=PROJECT_ID)
    ds_id = f"{PROJECT_ID}.{DIMS_DATASET}"
    ds = bigquery.Dataset(ds_id)
    try:
        existing = client.get_dataset(ds_id)
        if existing.location != LOCATION:
            client.delete_dataset(ds_id, delete_contents=True, not_found_ok=True)
            ds.location = LOCATION
            ds.description = "Dimensiones IPM (creadas por dbt)"
            client.create_dataset(ds)
        else:
            print(f"[OK] Dataset ya existe: {ds_id} ({existing.location})")
    except Exception:
        ds.location = LOCATION
        ds.description = "Dimensiones IPM (creadas por dbt)"
        client.create_dataset(ds)

with DAG(
    dag_id="gdv_build_dim_nivel_pobreza_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt","dims","ipm"],
    description="Construye la dimensión dim_nivel_pobreza en gdv_ipm_sisben_dims con dbt",
) as dag:

    ensure_ds = PythonOperator(
        task_id="ensure_dims_dataset",
        python_callable=ensure_dims_dataset,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_dim_nivel_pobreza",
        env=DBT_ENV,
        bash_command=f"cd {DBT_DIR} && dbt run --select dim_nivel_pobreza",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_dim_nivel_pobreza",
        env=DBT_ENV,
        bash_command=f"cd {DBT_DIR} && dbt test --select dim_nivel_pobreza",
    )

    ensure_ds >> dbt_run >> dbt_test
