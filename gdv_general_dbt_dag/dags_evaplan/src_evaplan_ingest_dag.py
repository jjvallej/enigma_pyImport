# dags/src_evaplan_ingest_dag.py
"""
DAG para ingerir datos desde la API de Evaplan.
Consume endpoints de autenticación y periodos, y almacena las respuestas JSON en Google Cloud Storage.

Flujo:
1. Autentica con la API y obtiene un token Bearer
2. Obtiene la lista de periodos usando el token
3. Almacena la respuesta de periodos en GCS
4. Obtiene AvanceMR usando el token y el periodo más reciente
5. Almacena AvanceMR en GCS
6. Obtiene AvanceMP usando el token y el periodo más reciente
7. Almacena AvanceMP en GCS
8. Obtiene AvanceXSubprograma usando el token y el periodo más reciente
9. Almacena AvanceXSubprograma en GCS
10. Obtiene AvanceGeneral usando el token y el periodo más reciente
11. Almacena AvanceGeneral en GCS
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
    save_periodos_to_gcs,
    get_avance_mr,
    get_avance_mp,
    get_avance_x_subprograma,
    get_avance_general,
    save_avance_to_gcs
)

# === CONFIGURACIÓN ===
DEFAULT_BUCKET_NAME = "datalake_gdv_dev"
DEFAULT_FOLDER_NAME = "data_staging/dpt_planeacion_municipal/api_evaplan/periodos"

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
    Retorna tanto los datos de periodos como el peri_idp del periodo más reciente.
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
        peri_idp = periodo_mas_reciente.get('peri_idp')
        print(f"[OK] Periodos obtenidos exitosamente.")
        print(f"[INFO] Periodo más reciente: {periodo_mas_reciente.get('peri_nombre', 'N/A')} (ID: {peri_idp})")
        
        # Retornar tanto los datos como el peri_idp
        return {
            "periodos_data": periodos_data,
            "peri_idp": peri_idp
        }
    else:
        raise ValueError("No se encontró periodo más reciente. No se puede continuar.")

def _save_periodos_to_gcs_task(ti):
    """
    Task que guarda la respuesta de periodos en un archivo JSON en GCS.
    Usa los datos obtenidos en la tarea anterior mediante XCom.
    """
    # Obtener datos desde XCom (de la tarea anterior)
    result = ti.xcom_pull(task_ids="get_periodos")
    
    if not result:
        raise ValueError("No se encontraron datos de periodos. La tarea de obtener periodos debe ejecutarse primero.")
    
    # Extraer periodos_data del resultado
    periodos_data = result.get("periodos_data") if isinstance(result, dict) else result
    
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

def _get_avance_mr_task(ti):
    """
    Task que obtiene los datos de AvanceMR desde la API de Evaplan.
    Usa el token y peri_idp obtenidos en tareas anteriores mediante XCom.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceMR desde la API de Evaplan...")
    print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    # Obtener AvanceMR
    avance_mr_data = get_avance_mr(token=token, peri_idp=peri_idp)
    
    print(f"[OK] AvanceMR obtenido exitosamente.")
    
    # Retornar datos para que estén disponibles en XCom
    return avance_mr_data

def _save_avance_mr_to_gcs_task(ti):
    """
    Task que guarda la respuesta de AvanceMR en un archivo JSON en GCS.
    Usa los datos obtenidos en la tarea anterior mediante XCom.
    """
    # Obtener datos desde XCom
    avance_mr_data = ti.xcom_pull(task_ids="get_avance_mr")
    
    if not avance_mr_data:
        raise ValueError("No se encontraron datos de AvanceMR. La tarea de obtener AvanceMR debe ejecutarse primero.")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_mr"
    
    print(f"[INFO] Guardando AvanceMR en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar en GCS
    gcs_uri = save_avance_to_gcs(
        avance_data=avance_mr_data,
        bucket_name=bucket_name,
        folder_name=folder_name,
        tipo_avance="AvanceMR",
        peri_idp=peri_idp
    )
    
    print(f"[OK] AvanceMR guardado exitosamente en: {gcs_uri}")
    
    return gcs_uri

def _get_avance_mp_task(ti):
    """
    Task que obtiene los datos de AvanceMP desde la API de Evaplan.
    Usa el token y peri_idp obtenidos en tareas anteriores mediante XCom.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceMP desde la API de Evaplan...")
    print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    # Obtener AvanceMP
    avance_mp_data = get_avance_mp(token=token, peri_idp=peri_idp)
    
    print(f"[OK] AvanceMP obtenido exitosamente.")
    
    # Retornar datos para que estén disponibles en XCom
    return avance_mp_data

def _save_avance_mp_to_gcs_task(ti):
    """
    Task que guarda la respuesta de AvanceMP en un archivo JSON en GCS.
    Usa los datos obtenidos en la tarea anterior mediante XCom.
    """
    # Obtener datos desde XCom
    avance_mp_data = ti.xcom_pull(task_ids="get_avance_mp")
    
    if not avance_mp_data:
        raise ValueError("No se encontraron datos de AvanceMP. La tarea de obtener AvanceMP debe ejecutarse primero.")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_mp"
    
    print(f"[INFO] Guardando AvanceMP en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar en GCS
    gcs_uri = save_avance_to_gcs(
        avance_data=avance_mp_data,
        bucket_name=bucket_name,
        folder_name=folder_name,
        tipo_avance="AvanceMP",
        peri_idp=peri_idp
    )
    
    print(f"[OK] AvanceMP guardado exitosamente en: {gcs_uri}")
    
    return gcs_uri

def _get_avance_x_subprograma_task(ti):
    """
    Task que obtiene los datos de AvanceXSubprograma desde la API de Evaplan.
    Usa el token y peri_idp obtenidos en tareas anteriores mediante XCom.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceXSubprograma desde la API de Evaplan...")
    print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    # Obtener AvanceXSubprograma
    avance_x_subprograma_data = get_avance_x_subprograma(token=token, peri_idp=peri_idp)
    
    print(f"[OK] AvanceXSubprograma obtenido exitosamente.")
    
    # Retornar datos para que estén disponibles en XCom
    return avance_x_subprograma_data

def _save_avance_x_subprograma_to_gcs_task(ti):
    """
    Task que guarda la respuesta de AvanceXSubprograma en un archivo JSON en GCS.
    Usa los datos obtenidos en la tarea anterior mediante XCom.
    """
    # Obtener datos desde XCom
    avance_x_subprograma_data = ti.xcom_pull(task_ids="get_avance_x_subprograma")
    
    if not avance_x_subprograma_data:
        raise ValueError("No se encontraron datos de AvanceXSubprograma. La tarea de obtener AvanceXSubprograma debe ejecutarse primero.")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_x_subprograma"
    
    print(f"[INFO] Guardando AvanceXSubprograma en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar en GCS
    gcs_uri = save_avance_to_gcs(
        avance_data=avance_x_subprograma_data,
        bucket_name=bucket_name,
        folder_name=folder_name,
        tipo_avance="AvanceXSubprograma",
        peri_idp=peri_idp
    )
    
    print(f"[OK] AvanceXSubprograma guardado exitosamente en: {gcs_uri}")
    
    return gcs_uri

def _get_avance_general_task(ti):
    """
    Task que obtiene los datos de AvanceGeneral desde la API de Evaplan.
    Usa el token y peri_idp obtenidos en tareas anteriores mediante XCom.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceGeneral desde la API de Evaplan...")
    print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    # Obtener AvanceGeneral
    avance_general_data = get_avance_general(token=token, peri_idp=peri_idp)
    
    print(f"[OK] AvanceGeneral obtenido exitosamente.")
    
    # Retornar datos para que estén disponibles en XCom
    return avance_general_data

def _save_avance_general_to_gcs_task(ti):
    """
    Task que guarda la respuesta de AvanceGeneral en un archivo JSON en GCS.
    Usa los datos obtenidos en la tarea anterior mediante XCom.
    """
    # Obtener datos desde XCom
    avance_general_data = ti.xcom_pull(task_ids="get_avance_general")
    
    if not avance_general_data:
        raise ValueError("No se encontraron datos de AvanceGeneral. La tarea de obtener AvanceGeneral debe ejecutarse primero.")
    
    # Obtener peri_idp desde XCom
    result = ti.xcom_pull(task_ids="get_periodos")
    if not result or not isinstance(result, dict):
        raise ValueError("No se encontró peri_idp. La tarea de obtener periodos debe ejecutarse primero.")
    
    peri_idp = result.get("peri_idp")
    if not peri_idp:
        raise ValueError("No se encontró peri_idp en los datos de periodos.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_general"
    
    print(f"[INFO] Guardando AvanceGeneral en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar en GCS (usar "AvanceGeneral" como tipo_avance para el nombre del archivo)
    gcs_uri = save_avance_to_gcs(
        avance_data=avance_general_data,
        bucket_name=bucket_name,
        folder_name=folder_name,
        tipo_avance="AvanceGeneral",
        peri_idp=peri_idp
    )
    
    print(f"[OK] AvanceGeneral guardado exitosamente en: {gcs_uri}")
    
    return gcs_uri

with DAG(
    dag_id="src_planeacion_ingest_evaplan",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["secretaria:planeacion", "actividad:ingesta", "fuente:evaplan", "ejecución:manual"],
    description="Ingiere datos desde la API de Evaplan. Autentica con la API, obtiene la lista de periodos, AvanceMR, AvanceMP, AvanceXSubprograma y AvanceGeneral, y almacena todas las respuestas JSON en GCS en las carpetas correspondientes dentro de data_staging/dpt_planeacion_municipal/api_evaplan/.",
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

    # Tarea 4: Obtener AvanceMR
    get_avance_mr_task = PythonOperator(
        task_id="get_avance_mr",
        python_callable=_get_avance_mr_task,
    )

    # Tarea 5: Guardar AvanceMR en GCS
    save_avance_mr_task = PythonOperator(
        task_id="save_avance_mr_to_gcs",
        python_callable=_save_avance_mr_to_gcs_task,
    )

    # Tarea 6: Obtener AvanceMP
    get_avance_mp_task = PythonOperator(
        task_id="get_avance_mp",
        python_callable=_get_avance_mp_task,
    )

    # Tarea 7: Guardar AvanceMP en GCS
    save_avance_mp_task = PythonOperator(
        task_id="save_avance_mp_to_gcs",
        python_callable=_save_avance_mp_to_gcs_task,
    )

    # Tarea 8: Obtener AvanceXSubprograma
    get_avance_x_subprograma_task = PythonOperator(
        task_id="get_avance_x_subprograma",
        python_callable=_get_avance_x_subprograma_task,
    )

    # Tarea 9: Guardar AvanceXSubprograma en GCS
    save_avance_x_subprograma_task = PythonOperator(
        task_id="save_avance_x_subprograma_to_gcs",
        python_callable=_save_avance_x_subprograma_to_gcs_task,
    )

    # Tarea 10: Obtener AvanceGeneral
    get_avance_general_task = PythonOperator(
        task_id="get_avance_general",
        python_callable=_get_avance_general_task,
    )

    # Tarea 11: Guardar AvanceGeneral en GCS
    save_avance_general_task = PythonOperator(
        task_id="save_avance_general_to_gcs",
        python_callable=_save_avance_general_to_gcs_task,
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    # Flujo: autenticar -> obtener periodos -> [guardar periodos, obtener todos los avances] -> guardar todos los avances -> fin
    start >> authenticate_task >> get_periodos_task >> [
        save_periodos_task,
        get_avance_mr_task,
        get_avance_mp_task,
        get_avance_x_subprograma_task,
        get_avance_general_task
    ]
    
    # Guardar todos los avances después de obtenerlos
    get_avance_mr_task >> save_avance_mr_task
    get_avance_mp_task >> save_avance_mp_task
    get_avance_x_subprograma_task >> save_avance_x_subprograma_task
    get_avance_general_task >> save_avance_general_task
    
    # Finalizar cuando todas las tareas de guardado terminen
    [
        save_periodos_task,
        save_avance_mr_task,
        save_avance_mp_task,
        save_avance_x_subprograma_task,
        save_avance_general_task
    ] >> end

