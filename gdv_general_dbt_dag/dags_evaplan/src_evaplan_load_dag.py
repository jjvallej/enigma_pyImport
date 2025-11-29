# dags/src_evaplan_load_dag.py
"""
DAG para extraer datos JSON desde Google Cloud Storage y cargarlos
en la capa bronze de BigQuery para las fuentes de Evaplan.

Nueva lógica:
1. Busca solo archivos JSON de la fecha actual en cada carpeta de fuente
2. Lee todos los JSON de la fecha actual para cada fuente
3. Extrae peri_idp de cada JSON (del nivel raíz o del nombre del archivo)
4. Une todos los registros de todos los JSON de la fecha actual
5. Agrega fecha_lectura (fecha de carga) y peri_idp a cada registro
6. Si la tabla existe y hay registros del mismo día y mismo peri_idp, los elimina antes de cargar

Flujo:
1. Asegura que el dataset bronze exista
2. Para cada fuente (periodos, avance_mr, avance_mp, avance_x_subprograma, avance_general):
   - Busca todos los JSON de la fecha actual en la carpeta
   - Procesa todos los JSON y une los registros
   - Agrega fecha_lectura y peri_idp a cada registro
   - Elimina registros duplicados (mismo día y mismo peri_idp) si la tabla existe
   - Carga los datos a BigQuery en una tabla con nombre evaplan_api_{fuente}_raw_data
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import os

# Asegura que podamos importar el módulo local
import sys
import os

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
from modules.evaplan.evaplan_load import (
    ensure_dataset,
    load_json_files_to_bq,
    get_table_name_for_fuente,
)

# === CONFIGURACIÓN ===
from modules.config import CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE

# Lista de fuentes a procesar
FUENTES = CONF.evaplan.fuentes

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
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:evaplan", "ejecución:manual"],
    description="Lee archivos JSON de Evaplan desde GCS (solo de la fecha actual) y carga a BigQuery en bronze_dpt_planeacion_municipal_dev con nomenclatura evaplan_api_{fuente}_raw_data. Une todos los JSON de la fecha actual, agrega fecha_lectura y peri_idp a cada registro, y elimina registros duplicados (mismo día y mismo peri_idp) antes de cargar.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la carga (capa bronze)
    with TaskGroup(group_id="bronze") as bronze_group:
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
        
        # Dependencias dentro del grupo bronze: ensure_dataset -> todas las cargas
        ensure_dataset_task >> load_tasks

    # Tarea para disparar el DAG de transformación
    trigger_transform_dag = TriggerDagRunOperator(
        task_id="trigger_transform_evaplan",
        trigger_dag_id="src_planeacion_transform_evaplan",
        wait_for_completion=False,  # No esperar a que termine el DAG de transformación
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> bronze (ensure_dataset -> todas las cargas en paralelo) -> trigger_transform -> end
    start >> bronze_group >> trigger_transform_dag >> end

