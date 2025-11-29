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
from datetime import datetime, timedelta
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
from modules.idc.idc_transform import (
    normalize_columns,
    lowercase_columns,
    uppercase_departamento,
    fill_nulls,
    round_decimals,
    round_integers,
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
    description="Normaliza los nombres de columnas del IDC a snake_case, convierte a minúsculas, normaliza departamento a mayúsculas sin acentos, reemplaza NULL/NaN por 0 y redondea/convierte columnas numéricas desde bronze a silver utilizando Python y BigQuery directamente. Crea las tablas finales: idc_transformed_data_*. Luego une las 3 tablas con el diccionario (dim_idc) en la capa gold creando fact_idc con estructura: DEPARTAMENTO, ANIO, ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, VALOR_ORIGINAL, VALOR_NORMALIZADO, VALOR_RANKING. Espera que los datos ya estén en bronze_dpt_planeacion_municipal_dev y que dim_idc exista en gold_dpt_planeacion_municipal_dev.",
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
        s2_normalize_columns_dato_original = PythonOperator(
            task_id="normalize_columns_dato_original",
            python_callable=lambda: normalize_columns('dato_original'),
            execution_timeout=timedelta(minutes=30),
        )

        s3_normalize_columns_valor_normalizado = PythonOperator(
            task_id="normalize_columns_valor_normalizado",
            python_callable=lambda: normalize_columns('valor_normalizado'),
            execution_timeout=timedelta(minutes=30),
        )

        s4_normalize_columns_valor_ranking = PythonOperator(
            task_id="normalize_columns_valor_ranking",
            python_callable=lambda: normalize_columns('valor_ranking'),
            execution_timeout=timedelta(minutes=30),
        )

        # Paso 2: Convertir nombres de columnas a minúsculas (depende de normalize_columns, para las 3 tablas en paralelo)
        s5_lowercase_columns_dato_original = PythonOperator(
            task_id="lowercase_columns_dato_original",
            python_callable=lambda: lowercase_columns('dato_original'),
            execution_timeout=timedelta(minutes=30),
        )

        s6_lowercase_columns_valor_normalizado = PythonOperator(
            task_id="lowercase_columns_valor_normalizado",
            python_callable=lambda: lowercase_columns('valor_normalizado'),
            execution_timeout=timedelta(minutes=30),
        )

        s7_lowercase_columns_valor_ranking = PythonOperator(
            task_id="lowercase_columns_valor_ranking",
            python_callable=lambda: lowercase_columns('valor_ranking'),
            execution_timeout=timedelta(minutes=30),
        )

        # Paso 3: Normalizar departamento a mayúsculas sin acentos (depende de lowercase_columns, crea tablas finales)
        s8_uppercase_dato_original = PythonOperator(
            task_id="uppercase_dato_original",
            python_callable=lambda: uppercase_departamento('dato_original'),
            execution_timeout=timedelta(minutes=30),
        )

        s9_uppercase_valor_normalizado = PythonOperator(
            task_id="uppercase_valor_normalizado",
            python_callable=lambda: uppercase_departamento('valor_normalizado'),
            execution_timeout=timedelta(minutes=30),
        )

        s10_uppercase_valor_ranking = PythonOperator(
            task_id="uppercase_valor_ranking",
            python_callable=lambda: uppercase_departamento('valor_ranking'),
            execution_timeout=timedelta(minutes=30),
        )

        # Paso 4: Reemplazar NULL/NaN por 0 en columnas numéricas (depende de uppercase, tablas intermedias)
        s11_fill_nulls_dato_original = PythonOperator(
            task_id="fill_nulls_dato_original",
            python_callable=lambda: fill_nulls('dato_original'),
            execution_timeout=timedelta(minutes=30),
        )

        s12_fill_nulls_valor_normalizado = PythonOperator(
            task_id="fill_nulls_valor_normalizado",
            python_callable=lambda: fill_nulls('valor_normalizado'),
            execution_timeout=timedelta(minutes=30),
        )

        s13_fill_nulls_valor_ranking = PythonOperator(
            task_id="fill_nulls_valor_ranking",
            python_callable=lambda: fill_nulls('valor_ranking'),
            execution_timeout=timedelta(minutes=30),
        )

        # Paso 5: Redondear/Convertir columnas numéricas (depende de fill_nulls, crea tablas finales)
        s14_round_decimals_dato_original = PythonOperator(
            task_id="round_decimals_dato_original",
            python_callable=lambda: round_decimals('dato_original'),
            execution_timeout=timedelta(minutes=30),
        )

        s15_round_decimals_valor_normalizado = PythonOperator(
            task_id="round_decimals_valor_normalizado",
            python_callable=lambda: round_decimals('valor_normalizado'),
            execution_timeout=timedelta(minutes=30),
        )

        s16_round_integers_valor_ranking = PythonOperator(
            task_id="round_integers_valor_ranking",
            python_callable=lambda: round_integers(),
            execution_timeout=timedelta(minutes=30),
        )

        # Dependencias: ensure_dataset -> [normalize_columns en paralelo] -> [lowercase_columns en paralelo] -> [uppercase en paralelo] -> [fill_nulls en paralelo] -> [round_decimals/round_integers en paralelo]
        s1_ensure_dataset >> [s2_normalize_columns_dato_original, s3_normalize_columns_valor_normalizado, s4_normalize_columns_valor_ranking]
        s2_normalize_columns_dato_original >> s5_lowercase_columns_dato_original
        s3_normalize_columns_valor_normalizado >> s6_lowercase_columns_valor_normalizado
        s4_normalize_columns_valor_ranking >> s7_lowercase_columns_valor_ranking
        s5_lowercase_columns_dato_original >> s8_uppercase_dato_original
        s6_lowercase_columns_valor_normalizado >> s9_uppercase_valor_normalizado
        s7_lowercase_columns_valor_ranking >> s10_uppercase_valor_ranking
        s8_uppercase_dato_original >> s11_fill_nulls_dato_original
        s9_uppercase_valor_normalizado >> s12_fill_nulls_valor_normalizado
        s10_uppercase_valor_ranking >> s13_fill_nulls_valor_ranking
        s11_fill_nulls_dato_original >> s14_round_decimals_dato_original
        s12_fill_nulls_valor_normalizado >> s15_round_decimals_valor_normalizado
        s13_fill_nulls_valor_ranking >> s16_round_integers_valor_ranking

    # Grupo de tareas para la capa gold
    with TaskGroup(group_id="gold") as gold_group:
        g1_ensure_dataset = PythonOperator(
            task_id="ensure_dataset",
            python_callable=_ensure_dataset_gold_task,
        )

        # Unir las 3 tablas en una estructura final con el diccionario
        g2_dbt_fact_idc = BashOperator(
            task_id="dbt_fact_idc",
            bash_command=f"set -e && cd {DBT_PROJECT_DIR} && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --select idc_processed_data 2>&1 || (echo 'DBT command failed with exit code:' $? && exit 1)",
            append_env=True,
            execution_timeout=timedelta(minutes=30),
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
    [s14_round_decimals_dato_original, s15_round_decimals_valor_normalizado, s16_round_integers_valor_ranking] >> gold_group >> end
