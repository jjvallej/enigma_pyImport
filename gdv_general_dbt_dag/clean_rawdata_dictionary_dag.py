from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Agregar el directorio del DAG al path
dag_dir = os.path.dirname(os.path.abspath(__file__))
if dag_dir not in sys.path:
    sys.path.append(dag_dir)

# Importar la función de limpieza
try:
    from modules.clean_rawdata_dictionary import run_clean_dictionary_to_bronze
except ImportError as e:
    raise ImportError(
        f"No se pudo importar el módulo clean_rawdata_dictionary. "
        f"Verifica que el archivo existe en {dag_dir}/modules/clean_rawdata_dictionary.py. "
        f"Error: {e}"
    )

with DAG(
    dag_id="gdv_clean_dictionary_bronze_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["clean", "bronze", "dictionary"],
    description="Limpia tildes del diccionario y escribe en BRONZE (dictionary_clean)",
) as dag:

    clean_dictionary = PythonOperator(
        task_id="clean_dictionary_to_bronze",
        python_callable=run_clean_dictionary_to_bronze,
    )
