# dags/src_idi_ingest_dag.py
"""
DAG para ingestar archivos Excel de IDI desde Función Pública.
Lee configuración desde Excel en Google Drive y procesa TODOS los años automáticamente.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys

# Función para encontrar la raíz del proyecto (donde está la carpeta modules)
def add_project_root_to_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while current_dir != "/":
        if os.path.exists(os.path.join(current_dir, "modules")):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            return
        current_dir = os.path.dirname(current_dir)
    
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

add_project_root_to_path()

from modules.idi.idi_ingest import ingest_all_years_idi
from modules.config import CONF, DEFAULT_BUCKET_NAME

# === CONFIGURACIÓN ===
CONFIG_DRIVE_URL = CONF.idi.config_drive_url
# link_name_keywords es una lista en config.yaml, se mantiene como lista
LINK_NAME_KEYWORDS = list(CONF.idi.link_name_keywords) if CONF.idi.link_name_keywords else []
BUCKET_NAME = DEFAULT_BUCKET_NAME
BASE_FOLDER_NAME = CONF.idi.gcs_base_folder
SKIP_ROWS = CONF.idi.transform_config.skip_rows
# sheet_name y columns_to_drop pueden ser None en config.yaml
SHEET_NAME = getattr(CONF.idi.transform_config, 'sheet_name', None)
COLUMNS_TO_DROP = getattr(CONF.idi.transform_config, 'columns_to_drop', None)

def _ingest_idi():
    """Task que ejecuta la ingesta de TODOS los años."""
    print(f"[INFO] Iniciando ingesta IDI")
    print(f"[INFO] Buscando: Palabras clave={LINK_NAME_KEYWORDS}")
    print(f"[INFO] Procesará TODOS los años encontrados")
    print(f"[INFO] Destino base: gs://{BUCKET_NAME}/{BASE_FOLDER_NAME}/")
    
    results = ingest_all_years_idi(
        config_drive_url=CONFIG_DRIVE_URL,
        link_name_keywords=LINK_NAME_KEYWORDS,
        bucket_name=BUCKET_NAME,
        base_folder_name=BASE_FOLDER_NAME,
        skip_rows=SKIP_ROWS,
        sheet_name=SHEET_NAME,
        columns_to_drop=COLUMNS_TO_DROP
    )
    
    # Mostrar resumen final
    success_count = sum(1 for r in results if r['status'] == 'success')
    error_count = sum(1 for r in results if r['status'] == 'error')
    
    print(f"\n[INFO] Procesamiento completo:")
    print(f"       Total años: {len(results)}")
    print(f"       Exitosos: {success_count}")
    print(f"       Errores: {error_count}")
    
    return results

with DAG(
    dag_id="src_planeacion_ingest_idi",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:carga", "fuente:idi", "ejecución:manual"],
    description="Ingesta archivos Excel de IDI desde config en Google Drive (TODOS los años)"
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    ingest = PythonOperator(
        task_id="ingest_idi_all_years",
        python_callable=_ingest_idi
    )
    
    end = EmptyOperator(task_id="end")
    
    start >> ingest >> end