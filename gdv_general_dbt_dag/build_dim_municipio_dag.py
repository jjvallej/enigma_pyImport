# gdv_general_dbt_dag/build_dim_municipio_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
import os

# --- CONSTANTES ---
PROJECT_ID = "datagov-473122"
DIMS_DATASET = "gdv_ipm_sisben_dims"   # dataset destino para las dimensiones
LOCATION = "us-central1"               # ubicación del dataset
SA_PATH = "/opt/airflow/include/sa.json"

# Ruta del proyecto dbt dentro del contenedor de Airflow
DBT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"
# PATH incluye ~/.local/bin donde pip instala los ejecutables
DBT_ENV = {
    "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
    "PATH": "/home/airflow/.local/bin:" + os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin")
}

# --- PYTHON CALLABLE: crear/verificar dataset ---
def ensure_dims_dataset():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    client = bigquery.Client(project=PROJECT_ID)
    ds_id = f"{PROJECT_ID}.{DIMS_DATASET}"
    ds = bigquery.Dataset(ds_id)

    try:
        existing = client.get_dataset(ds_id)
        if existing.location != LOCATION:
            # Si existe en otra región, lo recreamos en la región correcta
            print(f"[WARN] {ds_id} está en {existing.location}, se recreará en {LOCATION}")
            client.delete_dataset(ds_id, delete_contents=True, not_found_ok=True)
            ds.location = LOCATION
            ds.description = "Dimensiones IPM (creadas por dbt)"
            client.create_dataset(ds)
            print(f"[OK] Dataset recreado: {ds_id} ({LOCATION})")
        else:
            print(f"[OK] Dataset ya existe: {ds_id} ({existing.location})")
    except Exception:
        # No existe: crearlo
        ds.location = LOCATION
        ds.description = "Dimensiones IPM (creadas por dbt)"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_id} ({LOCATION})")

# --- DAG ---
with DAG(
    dag_id="gdv_build_dim_municipio_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "dims", "ipm"],
    description="Crea el dataset de dimensiones y construye dim_municipio con dbt",
) as dag:

    # 1) Asegurar dataset de dimensiones
    ensure_ds = PythonOperator(
        task_id="ensure_dims_dataset",
        python_callable=ensure_dims_dataset,
    )

    # 2) Ejecutar dbt (construye la tabla dim_municipio)
    dbt_run = BashOperator(
        task_id="dbt_run_dim_municipio",
        env=DBT_ENV,
        bash_command=f"cd {DBT_DIR} && dbt run --select dim_municipio",
    )

    # 3) (Opcional) Tests dbt sobre ese modelo
    dbt_test = BashOperator(
        task_id="dbt_test_dim_municipio",
        env=DBT_ENV,
        bash_command=f"cd {DBT_DIR} && dbt test --select dim_municipio",
    )

    ensure_ds >> dbt_run >> dbt_test
