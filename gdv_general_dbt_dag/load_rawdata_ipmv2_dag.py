# dags/load_rawdata_ipmv2_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os, sys, tempfile
import pandas as pd

# Asegura que podamos importar el módulo local
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from modules.load_rawdata_ipmv2 import (
    ensure_dataset,
    download_excel_from_gcs,
    get_latest_excel_from_gcs_folder,
    transform_excel,
    load_dataframe_to_bq,
    cleanup_temp_paths,
)

# === CONFIGURACIÓN ===
# Configuración para buscar el último archivo Excel en la carpeta ipm
# El DAG buscará automáticamente el archivo .xlsx más reciente en esta carpeta
GCS_BUCKET_NAME = "datalake_gdv"
GCS_FOLDER_PATH = "data_staging/dpt_planeacion_municipal/ipm"
DATASET_ID_BRONZE = "bronze_dpt_planeacion_municipal_dev"
DATASET_ID_SILVER = "silver_dpt_planeacion_municipal_dev"
DATASET_ID_GOLD = "gold_dpt_planeacion_municipal_dev"
TABLE_NAME_BRONZE = "bronze_dpt_planeacion_municipal_dev_ipm"
TABLE_NAME_SILVER = "silver_dpt_planeacion_municipal_dev_ipm"
TABLE_NAME_GOLD = "gold_dpt_planeacion_municipal_dev_ipm"
SHEET_INDEX = 0
DBT_PROJECT_DIR = "/opt/airflow/dags/gdv_general_dbt_dag/dbt"

def _ensure_dataset_bronze_task():
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _ensure_dataset_silver_task():
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

def _download_excel_task():
    # Obtener el último archivo Excel de la carpeta en GCS
    try:
        # Intentar obtener configuración desde Variables de Airflow
        bucket_name = Variable.get("ipm_gcs_bucket", default_var=GCS_BUCKET_NAME)
        folder_path = Variable.get("ipm_gcs_folder", default_var=GCS_FOLDER_PATH)
        print(f"[INFO] Usando configuración: bucket={bucket_name}, carpeta={folder_path}")
    except:
        # Si no existen las variables, usar valores por defecto hardcodeados
        bucket_name = GCS_BUCKET_NAME
        folder_path = GCS_FOLDER_PATH
        print(f"[INFO] Usando configuración por defecto: bucket={bucket_name}, carpeta={folder_path}")
    
    # Buscar el último archivo Excel en la carpeta
    print(f"[INFO] Buscando el último archivo .xlsx en gs://{bucket_name}/{folder_path}")
    gcs_uri = get_latest_excel_from_gcs_folder(bucket_name=bucket_name, folder_path=folder_path)
    
    print(f"[INFO] Descargando archivo desde GCS: {gcs_uri}")
    return download_excel_from_gcs(gcs_uri=gcs_uri)

def _transform_task(ti):
    local_excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    if not local_excel_path:
        raise ValueError("No se recibió la ruta del Excel en XCom (task download_excel).")
    df = transform_excel(local_path=local_excel_path, sheet_index=SHEET_INDEX)
    tmp = tempfile.NamedTemporaryFile(suffix=".pkl", delete=False)
    tmp_path = tmp.name
    tmp.close()
    df.to_pickle(tmp_path)
    return tmp_path

def _load_task(ti):
    pickle_path = ti.xcom_pull(task_ids="bronze.transform_dataframe")
    if not pickle_path:
        raise ValueError("No se recibió la ruta del DataFrame transformado en XCom (task transform_dataframe).")
    df = pd.read_pickle(pickle_path)
    load_dataframe_to_bq(df, dataset_id=DATASET_ID_BRONZE, table_name=TABLE_NAME_BRONZE)

def _cleanup_temp_files_task(ti):
    excel_path = ti.xcom_pull(task_ids="bronze.download_excel")
    pickle_path = ti.xcom_pull(task_ids="bronze.transform_dataframe")
    cleanup_temp_paths([excel_path, pickle_path])

with DAG(
    dag_id="scr_planeacion_transf_ipm",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:transformacion", "fuente:ipm", "ejecución:manual"],
    description="Lee Excel IPM desde GCS (subido por scr_planeacion_inges_ipm), transforma y carga a BigQuery en bronze_dpt_planeacion_municipal_dev.bronze_dpt_planeacion_municipal_dev_ipm",
) as dag:

    # Tarea inicial vacía
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la capa bronze
    with TaskGroup(group_id="bronze") as bronze_group:
        t1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_bronze_task,
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

    # Grupo de tareas para la capa silver
    with TaskGroup(group_id="silver") as silver_group:
        s1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_silver_task,
        )

        # Tareas dbt para cada modelo
        # Usamos la ruta completa del ejecutable dbt o lo buscamos en el PATH del usuario
        s2_dbt_run_stg = BashOperator(
            task_id="dbt_run_stg",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_stg || dbt run --select rawdata_ipmv2_stg",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s3_dbt_run_normalize_text = BashOperator(
            task_id="dbt_run_normalize_text",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_normalize_text || dbt run --select rawdata_ipmv2_normalize_text",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s4_dbt_run_transform_types = BashOperator(
            task_id="dbt_run_transform_types",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_transform_types || dbt run --select rawdata_ipmv2_transform_types",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s5_dbt_run_clean_numbers = BashOperator(
            task_id="dbt_run_clean_numbers",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_clean_numbers || dbt run --select rawdata_ipmv2_clean_numbers",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s6_dbt_run_detect_negatives = BashOperator(
            task_id="dbt_run_detect_negatives",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_detect_negatives || dbt run --select rawdata_ipmv2_detect_negatives",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s7_dbt_run_apply_validations = BashOperator(
            task_id="dbt_run_apply_validations",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_apply_validations || dbt run --select rawdata_ipmv2_apply_validations",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s8_dbt_run_clean = BashOperator(
            task_id="dbt_run_clean",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_clean || dbt run --select rawdata_ipmv2_clean",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        s9_dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt test --select rawdata_ipmv2_clean || dbt test --select rawdata_ipmv2_clean",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Dependencias: 
        # ensure_dataset -> stg -> normalize_text -> [transform_types, clean_numbers] -> detect_negatives -> apply_validations -> clean -> test
        # transform_types y clean_numbers pueden ejecutarse en paralelo (ambos dependen de normalize_text)
        s1_ensure_dataset >> s2_dbt_run_stg >> s3_dbt_run_normalize_text
        s3_dbt_run_normalize_text >> [s4_dbt_run_transform_types, s5_dbt_run_clean_numbers]
        s5_dbt_run_clean_numbers >> s6_dbt_run_detect_negatives >> s7_dbt_run_apply_validations >> s8_dbt_run_clean >> s9_dbt_test

    # Grupo de tareas para la capa gold
    with TaskGroup(group_id="gold") as gold_group:
        g1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        # Tarea dbt para ejecutar el modelo gold
        g2_dbt_run_gold = BashOperator(
            task_id="dbt_run_gold",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt run --select rawdata_ipmv2_gold || dbt run --select rawdata_ipmv2_gold",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        g3_dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"cd {DBT_PROJECT_DIR} && ~/.local/bin/dbt test --select rawdata_ipmv2_gold || dbt test --select rawdata_ipmv2_gold",
            env={
                "DBT_PROFILES_DIR": "/opt/airflow/include/dbt",
                "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/include/sa.json",
                "PATH": "/home/airflow/.local/bin:$PATH",
            },
        )

        # Dependencias: ensure_dataset -> dbt_run_gold -> dbt_test
        g1_ensure_dataset >> g2_dbt_run_gold >> g3_dbt_test

    # Dependencias: start -> bronze -> silver -> gold
    start >> bronze_group >> silver_group >> gold_group
