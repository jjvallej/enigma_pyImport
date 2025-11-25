# dags/src_evaplan_ingest_dag.py
"""
DAG para ingerir datos desde la API de Evaplan.
Consume endpoints de autenticación y periodos, y almacena las respuestas JSON en Google Cloud Storage.

Flujo:
1. Autentica con la API y obtiene un token Bearer
2. Obtiene la lista de periodos usando el token
3. Almacena la respuesta JSON en GCS en la carpeta evaplan
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os

# Asegura que podamos importar el módulo local
import sys
# Agregar el directorio raíz del proyecto al path para importar módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from modules.evaplan.evaplan_ingest import (
    authenticate,
    get_periodos,
    get_periodo_mas_reciente,
    save_periodos_to_gcs
)

# === CONFIGURACIÓN ===
DEFAULT_BUCKET_NAME = "datalake_gdv_dev"
DEFAULT_FOLDER_NAME = "data_staging/dpt_planeacion_municipal/evaplan/periodos"

def _authenticate_task():
    """
    Task que autentica con la API de Evaplan y obtiene un token.
    Retorna el token para que esté disponible en XCom para las tareas posteriores.
    """
    print(f"[INFO] Iniciando autenticación con la API de Evaplan...")
    
    # Autenticar y obtener token
    token = authenticate()
    
    print(f"[OK] Autenticación exitosa. Token obtenido.")
    
    # Retornar token para que esté disponible en XCom
    return token

def _get_periodos_task(ti):
    """
    Task que obtiene la lista de periodos desde la API de Evaplan.
    Usa el token obtenido en la tarea anterior mediante XCom.
    """
    # Obtener token desde XCom (de la tarea anterior)
    token = ti.xcom_pull(task_ids="authenticate")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo periodos desde la API de Evaplan...")
    print(f"[DEBUG] Usando token: {token[:20]}...")
    
    # Obtener periodos
    periodos_data = get_periodos(token=token)
    
    # Obtener el periodo más reciente
    periodo_mas_reciente = get_periodo_mas_reciente(periodos_data)
    
    if periodo_mas_reciente:
        print(f"[OK] Periodos obtenidos exitosamente.")
        print(f"[INFO] Periodo más reciente: {periodo_mas_reciente.get('peri_nombre', 'N/A')} (ID: {periodo_mas_reciente.get('peri_idp', 'N/A')})")
    else:
        print(f"[WARN] No se encontró periodo más reciente.")
    
    # Retornar datos para que estén disponibles en XCom
    return periodos_data

def _save_periodos_to_gcs_task(ti):
    """
    Task que guarda la respuesta de periodos en un archivo JSON en GCS.
    Usa los datos obtenidos en la tarea anterior mediante XCom.
    """
    # Obtener datos desde XCom (de la tarea anterior)
    periodos_data = ti.xcom_pull(task_ids="get_periodos")
    
    if not periodos_data:
        raise ValueError("No se encontraron datos de periodos. La tarea de obtener periodos debe ejecutarse primero.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = DEFAULT_FOLDER_NAME
    
    print(f"[INFO] Guardando periodos en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar en GCS
    gcs_uri = save_periodos_to_gcs(
        periodos_data=periodos_data,
        bucket_name=bucket_name,
        folder_name=folder_name
    )
    
    print(f"[OK] Periodos guardados exitosamente en: {gcs_uri}")
    
    return gcs_uri

with DAG(
    dag_id="src_planeacion_ingest_evaplan",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:evaplan", "ejecución:manual"],
    description="Ingiere datos desde la API de Evaplan. Autentica con la API, obtiene la lista de periodos y almacena la respuesta JSON en GCS en la carpeta evaplan/periodos dentro de data_staging/dpt_planeacion_municipal/.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Tarea 1: Autenticación
    authenticate_task = PythonOperator(
        task_id="authenticate",
        python_callable=_authenticate_task,
    )

    # Tarea 2: Obtener periodos
    get_periodos_task = PythonOperator(
        task_id="get_periodos",
        python_callable=_get_periodos_task,
    )

    # Tarea 3: Guardar periodos en GCS
    save_periodos_task = PythonOperator(
        task_id="save_periodos_to_gcs",
        python_callable=_save_periodos_to_gcs_task,
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    start >> authenticate_task >> get_periodos_task >> save_periodos_task >> end

