# dags/src_evaplan_load_dag.py
"""
DAG para extraer datos JSON desde Google Cloud Storage y cargarlos
en la capa bronze de BigQuery para las fuentes de Evaplan.

Maneja múltiples JSON por carpeta con lógica inteligente:
1. Si solo hay 1 JSON: crea la tabla basada en ese JSON
2. Si hay múltiples JSON y la tabla existe: carga solo el más reciente, reemplaza registros del mismo día
3. Si hay múltiples JSON y la tabla NO existe: carga todos los JSON con sus fechas correspondientes
4. Si hay registros del mismo día: los reemplaza para evitar duplicados

Flujo:
1. Asegura que el dataset bronze exista
2. Para cada fuente (periodos, avance_mr, avance_mp, avance_x_subprograma, avance_general):
   - Procesa todos los JSON de la carpeta según la lógica indicada
   - Carga los datos a BigQuery en una tabla con nombre evaplan_api_{fuente}_raw_data
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import os

# Asegura que podamos importar el módulo local
import sys
# Agregar el directorio raíz del proyecto al path para importar módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from modules.evaplan.evaplan_load import (
    ensure_dataset,
    load_json_files_to_bq,
    get_table_name_for_fuente,
)

# === CONFIGURACIÓN ===
DEFAULT_BUCKET_NAME = "datalake_gdv_dev"
DATASET_ID_BRONZE = "bronze_dpt_planeacion_municipal_dev"

# Lista de fuentes a procesar
FUENTES = [
    "periodos",
    "avance_mr",
    "avance_mp",
    "avance_x_subprograma",
    "avance_general"
]

def _ensure_dataset_bronze_task():
    """Asegura que el dataset bronze exista."""
    ensure_dataset(dataset_id=DATASET_ID_BRONZE)

def _load_fuente_task(fuente: str):
    """
    Carga los JSON de una fuente a BigQuery.
    
    Args:
        fuente: Nombre de la fuente
    """
    def task_function():
        table_name = get_table_name_for_fuente(fuente)
        
        print(f"[INFO] Procesando fuente: {fuente}")
        print(f"[INFO] Tabla destino: {DATASET_ID_BRONZE}.{table_name}")
        
        # Cargar JSON usando la función principal que maneja toda la lógica
        load_json_files_to_bq(
            fuente=fuente,
            bucket_name=DEFAULT_BUCKET_NAME,
            dataset_id=DATASET_ID_BRONZE,
            table_name=table_name
        )
        
        print(f"[OK] Fuente {fuente} procesada exitosamente")
        return table_name
    
    return task_function

with DAG(
    dag_id="src_planeacion_load_evaplan",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:evaplan", "ejecución:manual"],
    description="Lee archivos JSON de Evaplan desde GCS y carga a BigQuery en bronze_dpt_planeacion_municipal_dev con nomenclatura evaplan_api_{fuente}_raw_data. Maneja múltiples JSON por carpeta con lógica inteligente de carga y reemplazo de registros duplicados por fecha.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea para asegurar que el dataset exista
    ensure_dataset_task = PythonOperator(
        task_id="ensure_dataset",
        python_callable=_ensure_dataset_bronze_task,
    )

    # Crear TaskGroups para cada fuente (pueden ejecutarse en paralelo)
    load_tasks = []
    
    for fuente in FUENTES:
        with TaskGroup(group_id=f"load_{fuente}") as fuente_group:
            # Task única que maneja toda la lógica de carga para esta fuente
            load_task = PythonOperator(
                task_id="load_to_bq",
                python_callable=_load_fuente_task(fuente),
            )
            
            load_tasks.append(fuente_group)

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> ensure_dataset -> todas las cargas en paralelo -> end
    start >> ensure_dataset_task >> load_tasks >> end

