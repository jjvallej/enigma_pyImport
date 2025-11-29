# dags/src_idc_transform_dag.py
"""
DAG para transformar los datos del IDC desde la capa bronze a la capa silver
utilizando modelos dbt. Procesa cinco pasos:
1. Normalización de nombres de columnas a snake_case (vistas)
2. Conversión de nombres de columnas a minúsculas (vistas)
3. Normalización de departamento a mayúsculas sin acentos (vistas)
4. Reemplazo de NULL/NaN por 0 en columnas numéricas (vistas intermedias)
5. Redondeo/Conversión de columnas numéricas (tablas finales)

Cada tabla tiene sus propios modelos:
- idc_normalize_columns_*: Convierte nombres a snake_case (vistas)
- idc_lowercase_columns_*: Convierte nombres a minúsculas (vistas)
- idc_uppercase_*: Normaliza departamento (vistas)
- idc_fill_nulls_*: Reemplaza NULL/NaN por 0 (vistas intermedias)
- idc_round_decimals_*: Redondea a 2 decimales para dato_original y valor_normalizado (tablas finales)
- idc_round_integers_*: Convierte a enteros para valor_ranking (tablas finales)
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os
import sys

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

from modules.idc.idc_load import (
    ensure_dataset,
)

# === CONFIGURACIÓN ===
from modules.config import DATASET_ID_SILVER, DATASET_ID_GOLD
# Usar project_root (calculado arriba) para encontrar la carpeta dbt dinámicamente
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt")

def _ensure_dataset_silver_task():
    """Asegura que el dataset silver exista."""
    ensure_dataset(dataset_id=DATASET_ID_SILVER)

def _ensure_dataset_gold_task():
    """Asegura que el dataset gold exista."""
    ensure_dataset(dataset_id=DATASET_ID_GOLD)

with DAG(
    dag_id="src_planeacion_transf_idc",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:transformacion", "fuente:idc", "ejecución:manual"],
    description="Normaliza los nombres de columnas del IDC a snake_case, convierte a minúsculas, normaliza departamento a mayúsculas sin acentos, reemplaza NULL/NaN por 0 y redondea/convierte columnas numéricas desde bronze a silver utilizando modelos dbt. Crea las tablas finales: idc_transformed_data_*. Luego une las 3 tablas con el diccionario (dim_idc) en la capa gold creando fact_idc con estructura: DEPARTAMENTO, ANIO, ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, VALOR_ORIGINAL, VALOR_NORMALIZADO, VALOR_RANKING. Espera que los datos ya estén en bronze_dpt_planeacion_municipal_dev y que dim_idc exista en gold_dpt_planeacion_municipal_dev.",
) as dag:

    # Tarea inicial vacía
    start = EmptyOperator(
        task_id="start",
    )

    # Grupo de tareas para la capa silver
    with TaskGroup(group_id="silver") as silver_group:
        s1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_silver_task,
        )

        # Paso 1: Normalizar nombres de columnas a snake_case (para las 3 tablas en paralelo)
        s2_dbt_normalize_columns_dato_original = BashOperator(
            task_id="dbt_normalize_columns_dato_original",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_normalize_columns_dato_original",
            append_env=True,
        )

        s3_dbt_normalize_columns_valor_normalizado = BashOperator(
            task_id="dbt_normalize_columns_valor_normalizado",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_normalize_columns_valor_normalizado",
            append_env=True,
        )

        s4_dbt_normalize_columns_valor_ranking = BashOperator(
            task_id="dbt_normalize_columns_valor_ranking",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_normalize_columns_valor_ranking",
            append_env=True,
        )

        # Paso 2: Convertir nombres de columnas a minúsculas (depende de normalize_columns, para las 3 tablas en paralelo)
        s5_dbt_lowercase_columns_dato_original = BashOperator(
            task_id="dbt_lowercase_columns_dato_original",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_lowercase_columns_dato_original",
            append_env=True,
        )

        s6_dbt_lowercase_columns_valor_normalizado = BashOperator(
            task_id="dbt_lowercase_columns_valor_normalizado",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_lowercase_columns_valor_normalizado",
            append_env=True,
        )

        s7_dbt_lowercase_columns_valor_ranking = BashOperator(
            task_id="dbt_lowercase_columns_valor_ranking",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_lowercase_columns_valor_ranking",
            append_env=True,
        )

        # Paso 3: Normalizar departamento a mayúsculas sin acentos (depende de lowercase_columns, crea tablas finales)
        s8_dbt_uppercase_dato_original = BashOperator(
            task_id="dbt_uppercase_dato_original",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_uppercase_dato_original",
            append_env=True,
        )

        s9_dbt_uppercase_valor_normalizado = BashOperator(
            task_id="dbt_uppercase_valor_normalizado",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_uppercase_valor_normalizado",
            append_env=True,
        )

        s10_dbt_uppercase_valor_ranking = BashOperator(
            task_id="dbt_uppercase_valor_ranking",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_uppercase_valor_ranking",
            append_env=True,
        )

        # Paso 4: Reemplazar NULL/NaN por 0 en columnas numéricas (depende de uppercase, vistas intermedias)
        s11_dbt_fill_nulls_dato_original = BashOperator(
            task_id="dbt_fill_nulls_dato_original",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_fill_nulls_dato_original",
            append_env=True,
        )

        s12_dbt_fill_nulls_valor_normalizado = BashOperator(
            task_id="dbt_fill_nulls_valor_normalizado",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_fill_nulls_valor_normalizado",
            append_env=True,
        )

        s13_dbt_fill_nulls_valor_ranking = BashOperator(
            task_id="dbt_fill_nulls_valor_ranking",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_fill_nulls_valor_ranking",
            append_env=True,
        )

        # Paso 5: Redondear/Convertir columnas numéricas (depende de fill_nulls, crea tablas finales)
        s14_dbt_round_decimals_dato_original = BashOperator(
            task_id="dbt_round_decimals_dato_original",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_round_decimals_dato_original",
            append_env=True,
        )

        s15_dbt_round_decimals_valor_normalizado = BashOperator(
            task_id="dbt_round_decimals_valor_normalizado",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_round_decimals_valor_normalizado",
            append_env=True,
        )

        s16_dbt_round_integers_valor_ranking = BashOperator(
            task_id="dbt_round_integers_valor_ranking",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_round_integers_valor_ranking",
            append_env=True,
        )

        # Dependencias: ensure_dataset -> [normalize_columns en paralelo] -> [lowercase_columns en paralelo] -> [uppercase en paralelo] -> [fill_nulls en paralelo] -> [round_decimals/round_integers en paralelo]
        s1_ensure_dataset >> [s2_dbt_normalize_columns_dato_original, s3_dbt_normalize_columns_valor_normalizado, s4_dbt_normalize_columns_valor_ranking]
        s2_dbt_normalize_columns_dato_original >> s5_dbt_lowercase_columns_dato_original
        s3_dbt_normalize_columns_valor_normalizado >> s6_dbt_lowercase_columns_valor_normalizado
        s4_dbt_normalize_columns_valor_ranking >> s7_dbt_lowercase_columns_valor_ranking
        s5_dbt_lowercase_columns_dato_original >> s8_dbt_uppercase_dato_original
        s6_dbt_lowercase_columns_valor_normalizado >> s9_dbt_uppercase_valor_normalizado
        s7_dbt_lowercase_columns_valor_ranking >> s10_dbt_uppercase_valor_ranking
        s8_dbt_uppercase_dato_original >> s11_dbt_fill_nulls_dato_original
        s9_dbt_uppercase_valor_normalizado >> s12_dbt_fill_nulls_valor_normalizado
        s10_dbt_uppercase_valor_ranking >> s13_dbt_fill_nulls_valor_ranking
        s11_dbt_fill_nulls_dato_original >> s14_dbt_round_decimals_dato_original
        s12_dbt_fill_nulls_valor_normalizado >> s15_dbt_round_decimals_valor_normalizado
        s13_dbt_fill_nulls_valor_ranking >> s16_dbt_round_integers_valor_ranking

    # Grupo de tareas para la capa gold
    with TaskGroup(group_id="gold") as gold_group:
        g1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        # Unir las 3 tablas en una estructura final con el diccionario
        g2_dbt_fact_idc = BashOperator(
            task_id="dbt_fact_idc",
            bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_processed_data",
            append_env=True,
        )

        # Dependencias: ensure_dataset -> dbt_fact_idc
        # Nota: dbt_fact_idc depende de las 3 tablas finales de silver (s14, s15, s16) y de dim_idc en gold
        g1_ensure_dataset >> g2_dbt_fact_idc

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: 
    # - start -> silver -> gold -> end
    # - Las 3 tablas finales de silver (s14, s15, s16) deben completarse antes de gold
    start >> silver_group
    [s14_dbt_round_decimals_dato_original, s15_dbt_round_decimals_valor_normalizado, s16_dbt_round_integers_valor_ranking] >> gold_group >> end
