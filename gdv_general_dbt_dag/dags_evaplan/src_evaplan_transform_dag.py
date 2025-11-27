# dags/src_evaplan_transform_dag.py
"""
DAG para transformar datos de Evaplan desde la capa bronze a la capa silver.

Lógica:
1. Asegura que el dataset silver exista
2. Obtiene los peri_idp únicos de las tablas bronze de la fecha actual
3. Para cada fuente, elimina registros con esos peri_idp de las tablas silver
4. Copia los datos transformados de bronze a silver

Las tablas silver siempre tienen los últimos datos de los endpoints para cada periodo
que se está trabajando en el DAG. Si la tabla existe, se borran todos los registros
que tengan los ids de periodo que estamos trabajando en el proyecto.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import os
import sys

# Asegura que podamos importar el módulo local
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from modules.evaplan.evaplan_transform import (
    ensure_dataset,
    get_all_peri_idps_from_bronze,
    transform_fuente_to_silver,
)

# === CONFIGURACIÓN ===
DATASET_ID_SILVER = "silver_dpt_planeacion_municipal_dev"

# Lista de fuentes a procesar
FUENTES = [
    "periodos",
    "avance_mr",
    "avance_mp",
    "avance_x_subprograma",
    "avance_general"
]

def _ensure_dataset_silver_task():
    """Asegura que el dataset silver exista."""
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _get_peri_idps_task():
    """Obtiene todos los peri_idp únicos de las tablas bronze de la fecha actual."""
    peri_idps = get_all_peri_idps_from_bronze()
    return sorted(list(peri_idps)) if peri_idps else []

def _transform_fuente_task(fuente: str):
    """
    Transforma una fuente de bronze a silver.
    
    Args:
        fuente: Nombre de la fuente
    """
    def task_function(ti):
        # Obtener peri_idps del task anterior
        peri_idps_list = ti.xcom_pull(task_ids="get_peri_idps")
        peri_idps = set(peri_idps_list) if peri_idps_list else None
        
        print(f"[INFO] Transformando fuente: {fuente}")
        if peri_idps:
            print(f"[INFO] Peri_idp a procesar: {sorted(peri_idps)}")
        
        # Transformar fuente
        transform_fuente_to_silver(fuente, peri_idps)
        
        print(f"[OK] Fuente {fuente} transformada exitosamente")
        return fuente
    
    return task_function

with DAG(
    dag_id="src_planeacion_transform_evaplan",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:transformacion", "fuente:evaplan", "ejecución:manual"],
    description="Transforma datos de Evaplan desde bronze a silver. Obtiene los peri_idp únicos de las tablas bronze de la fecha actual, elimina registros con esos peri_idp de las tablas silver, y copia los nuevos datos de bronze a silver. Las tablas silver siempre tienen los últimos datos de los endpoints para cada periodo que se está trabajando.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea para obtener peri_idp únicos de bronze
    get_peri_idps_task = PythonOperator(
        task_id="get_peri_idps",
        python_callable=_get_peri_idps_task,
    )

    # Grupo de tareas para la transformación (capa silver)
    with TaskGroup(group_id="silver") as silver_group:
        # Tarea para asegurar que el dataset exista
        ensure_dataset_task = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_silver_task,
        )

        # Crear TaskGroups para cada fuente (pueden ejecutarse en paralelo)
        transform_tasks = []
        
        for fuente in FUENTES:
            with TaskGroup(group_id=f"transform_{fuente}") as fuente_group:
                # Task única que maneja toda la lógica de transformación para esta fuente
                transform_task = PythonOperator(
                    task_id="transform_to_silver",
                    python_callable=_transform_fuente_task(fuente),
                )
                
                transform_tasks.append(fuente_group)
        
        # Dependencias dentro del grupo silver: ensure_dataset -> todas las transformaciones
        ensure_dataset_task >> transform_tasks

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: 
    # - start -> get_peri_idps -> silver (ensure_dataset -> todas las transformaciones en paralelo) -> end
    start >> get_peri_idps_task >> silver_group >> end

