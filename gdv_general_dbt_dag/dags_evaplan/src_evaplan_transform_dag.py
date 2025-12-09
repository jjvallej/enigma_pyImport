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
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
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

from modules.evaplan.evaplan_transform import (
    ensure_dataset,
    get_all_peri_idps_from_bronze,
    transform_fuente_to_silver,
)

# === CONFIGURACIÓN ===
from modules.config import CONF, DATASET_ID_SILVER, DATASET_ID_GOLD, get_dbt_command
# Usar project_root (calculado arriba) para encontrar la carpeta dbt dinámicamente
# Nota: project_root no está definido globalmente, debemos recalcularlo o usar una ruta relativa segura
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt")

# Lista de fuentes a procesar
FUENTES = CONF.evaplan.fuentes

def _ensure_dataset_silver_task():
    """Asegura que el dataset silver exista."""
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    """Asegura que el dataset gold exista."""
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

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
    schedule=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:transformacion", "fuente:evaplan", "ejecución:manual"],
    description="Transforma datos de Evaplan desde bronze a silver y crea vistas en gold. Obtiene los peri_idp únicos de las tablas bronze de la fecha actual, elimina registros con esos peri_idp de las tablas silver, y copia los nuevos datos de bronze a silver (transformando nombres de columnas a minúsculas). Luego crea vistas en gold que unen la tabla de periodos con cada tabla de avance mediante JOIN por peri_idp.",
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

    # Grupo de tareas para la capa gold
    with TaskGroup(group_id="gold") as gold_group:
        # Tarea para asegurar que el dataset exista
        ensure_dataset_gold_task = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        # Tareas dbt para crear las vistas en gold
        # Nota: En Composer, dbt debe estar instalado en el entorno.
        # Usamos 'dbt' directamente.
        # Los comandos incluyen visualización de logs de dbt después de la ejecución.
        
        def _get_dbt_command_with_logs(dbt_command: str, dbt_project_dir: str) -> str:
            """
            Genera un comando bash que ejecuta dbt y muestra los logs de dbt después de la ejecución.
            Los logs se muestran siempre, incluso si dbt falla.
            """
            # Obtener valores desde config.yaml
            from modules.config import PROJECT_ID, DATASET_ID_BRONZE, DATASET_ID_SILVER, DATASET_ID_GOLD, LOCATION
            import json
            
            env_vars = {
                "DBT_PROJECT_ID": PROJECT_ID,
                "DBT_DATASET_BRONZE": DATASET_ID_BRONZE,
                "DBT_DATASET_SILVER": DATASET_ID_SILVER,
                "DBT_DATASET_GOLD": DATASET_ID_GOLD,
                "DBT_LOCATION": LOCATION,
            }
            
            # Construir comando con export de variables
            export_vars = " && ".join([f'export {key}="{value}"' for key, value in env_vars.items()])
            
            # Construir argumentos --vars para pasar variables a dbt
            dbt_vars = {
                "project_id": env_vars["DBT_PROJECT_ID"],
                "bronze_dataset": env_vars["DBT_DATASET_BRONZE"],
                "silver_dataset": env_vars["DBT_DATASET_SILVER"],
                "gold_dataset": env_vars["DBT_DATASET_GOLD"],
            }
            vars_json = json.dumps(dbt_vars)
            vars_arg = f"--vars '{vars_json}'"
            
            # Ruta del archivo de log de dbt
            log_file = os.path.join(dbt_project_dir, "logs", "dbt.log")
            
            # Comando completo que:
            # 1. Exporta variables de entorno
            # 2. Cambia al directorio del proyecto dbt
            # 3. Ejecuta dbt y captura el código de salida
            # 4. Muestra los logs siempre (incluso si falló)
            # 5. Sale con el código de salida original
            full_command = (
                f'set +e && '  # Desactivar exit on error temporalmente
                f'{export_vars} && '
                f'cd {dbt_project_dir} && '
                f'{dbt_command} {vars_arg} --project-dir {dbt_project_dir} --profiles-dir {dbt_project_dir} 2>&1; '
                f'DBT_EXIT_CODE=$? && '
                f'echo "" && '
                f'echo "==========================================" && '
                f'echo "DBT LOGS (últimas 100 líneas):" && '
                f'echo "==========================================" && '
                f'if [ -f "{log_file}" ]; then '
                f'tail -n 100 "{log_file}" || echo "No se pudieron leer los logs de dbt"; '
                f'else '
                f'echo "Archivo de log de dbt no encontrado en: {log_file}"; '
                f'fi && '
                f'echo "==========================================" && '
                f'if [ $DBT_EXIT_CODE -ne 0 ]; then '
                f'echo "DBT command failed with exit code: $DBT_EXIT_CODE"; '
                f'fi && '
                f'exit $DBT_EXIT_CODE'
            )
            
            return full_command
        
        dbt_avance_mr = BashOperator(
            task_id="dbt_avance_mr_processed_data",
            bash_command=_get_dbt_command_with_logs("dbt run --select evaplan_api_avance_mr_processed_data", DBT_PROJECT_DIR),
            append_env=True,
        )

        dbt_avance_mp = BashOperator(
            task_id="dbt_avance_mp_processed_data",
            bash_command=_get_dbt_command_with_logs("dbt run --select evaplan_api_avance_mp_processed_data", DBT_PROJECT_DIR),
            append_env=True,
        )

        dbt_avance_x_subprograma = BashOperator(
            task_id="dbt_avance_x_subprograma_processed_data",
            bash_command=_get_dbt_command_with_logs("dbt run --select evaplan_api_avance_x_subprograma_processed_data", DBT_PROJECT_DIR),
            append_env=True,
        )

        dbt_avance_general = BashOperator(
            task_id="dbt_avance_general_processed_data",
            bash_command=_get_dbt_command_with_logs("dbt run --select evaplan_api_avance_general_processed_data", DBT_PROJECT_DIR),
            append_env=True,
        )

        # Dependencias dentro del grupo gold: ensure_dataset -> ejecución secuencial de todas las vistas
        ensure_dataset_gold_task >> dbt_avance_mr >> dbt_avance_mp >> dbt_avance_x_subprograma >> dbt_avance_general

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: 
    # - start -> get_peri_idps -> silver (ensure_dataset -> todas las transformaciones en paralelo) -> gold -> end
    start >> get_peri_idps_task >> silver_group >> gold_group >> end

