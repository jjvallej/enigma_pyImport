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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

# Asegura que podamos importar el módulo local
import sys
import os

# Función para encontrar la raíz del proyecto (donde está la carpeta modules)
def add_project_root_to_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Buscar hacia arriba hasta encontrar 'modules'
    while current_dir != "/":
        possible_modules = os.path.join(current_dir, "modules")
        if os.path.exists(possible_modules):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            return
        current_dir = os.path.dirname(current_dir)
    
    # Fallback
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

add_project_root_to_path()

from modules.evaplan.evaplan_ingest import (
    authenticate,
    get_periodos,
    save_periodos_to_gcs,
    read_latest_periodos_json_from_gcs,
    get_all_periodos_from_json,
    get_avance_mr,
    get_avance_mp,
    get_avance_x_subprograma,
    get_avance_general,
    save_avance_to_gcs,
)

# === CONFIGURACIÓN ===
from modules.config import CONF, DEFAULT_BUCKET_NAME

# === CONFIGURACIÓN ===
# DEFAULT_BUCKET_NAME viene de modules.config
DEFAULT_FOLDER_NAME = CONF.evaplan.gcs_folder

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
    Retorna los datos de periodos para guardarlos en GCS.
    """
    # Obtener token desde XCom (de la tarea anterior)
    token = ti.xcom_pull(task_ids="authenticate")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo periodos desde la API de Evaplan...")
    print(f"[DEBUG] Usando token: {token[:20]}...")
    
    # Obtener periodos
    periodos_data = get_periodos(token=token)
    
    print(f"[OK] Periodos obtenidos exitosamente.")
    
    # Retornar los datos para guardarlos en GCS
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

def _read_periodos_from_gcs_task(ti):
    """
    Task que lee el JSON más reciente de periodos desde GCS (el de la fecha actual).
    Retorna todos los periodos encontrados en el JSON.
    """
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = DEFAULT_FOLDER_NAME
    
    print(f"[INFO] Leyendo periodos desde GCS...")
    print(f"[INFO] Bucket: {bucket_name}")
    print(f"[INFO] Carpeta: {folder_name}")
    
    # Leer el JSON más reciente desde GCS
    periodos_data = read_latest_periodos_json_from_gcs(
        bucket_name=bucket_name,
        folder_name=folder_name
    )
    
    # Extraer todos los periodos del JSON
    periodos = get_all_periodos_from_json(periodos_data)
    
    print(f"[OK] Se leyeron {len(periodos)} periodo(s) desde GCS")
    
    # Retornar la lista de periodos
    return periodos

def _get_avance_mr_task(ti):
    """
    Task que obtiene los datos de AvanceMR desde la API de Evaplan para TODOS los periodos.
    Lee los periodos desde GCS y procesa cada uno.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    # Obtener todos los periodos desde GCS
    periodos = ti.xcom_pull(task_ids="read_periodos_from_gcs")
    
    if not periodos or not isinstance(periodos, list):
        raise ValueError("No se encontraron periodos. La tarea de leer periodos desde GCS debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceMR desde la API de Evaplan para {len(periodos)} periodo(s)...")
    
    # Procesar todos los periodos
    avances_mr = []
    for periodo in periodos:
        peri_idp = periodo.get('peri_idp')
        if not peri_idp:
            print(f"[WARN] Periodo sin peri_idp, se omite: {periodo}")
            continue
        
        print(f"[DEBUG] Procesando periodo: {periodo.get('peri_nombre', 'N/A')} (ID: {peri_idp})")
        
        # Obtener AvanceMR para este periodo
        avance_mr_data = get_avance_mr(token=token, peri_idp=peri_idp)
        avances_mr.append({
            'periodo': periodo,
            'avance_data': avance_mr_data
        })
    
    print(f"[OK] AvanceMR obtenido exitosamente para {len(avances_mr)} periodo(s).")
    
    # Retornar lista de avances con sus periodos
    return avances_mr

def _save_avance_mr_to_gcs_task(ti):
    """
    Task que guarda las respuestas de AvanceMR en archivos JSON en GCS para TODOS los periodos.
    """
    # Obtener datos desde XCom
    avances_mr = ti.xcom_pull(task_ids="get_avance_mr")
    
    if not avances_mr or not isinstance(avances_mr, list):
        raise ValueError("No se encontraron datos de AvanceMR. La tarea de obtener AvanceMR debe ejecutarse primero.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_mr"
    
    print(f"[INFO] Guardando {len(avances_mr)} archivo(s) de AvanceMR en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar cada avance en GCS
    gcs_uris = []
    for item in avances_mr:
        periodo = item.get('periodo')
        avance_data = item.get('avance_data')
        peri_idp = periodo.get('peri_idp')
        
        if not peri_idp or not avance_data:
            print(f"[WARN] Datos incompletos para periodo {periodo.get('peri_nombre', 'N/A')}, se omite")
            continue
        
        # Guardar en GCS
        gcs_uri = save_avance_to_gcs(
            avance_data=avance_data,
            bucket_name=bucket_name,
            folder_name=folder_name,
            tipo_avance="AvanceMR",
            peri_idp=peri_idp
        )
        
        gcs_uris.append(gcs_uri)
        print(f"[OK] AvanceMR guardado para periodo {periodo.get('peri_nombre', 'N/A')}: {gcs_uri}")
    
    print(f"[OK] Se guardaron {len(gcs_uris)} archivo(s) de AvanceMR exitosamente.")
    
    return gcs_uris

def _get_avance_mp_task(ti):
    """
    Task que obtiene los datos de AvanceMP desde la API de Evaplan para TODOS los periodos.
    Lee los periodos desde GCS y procesa cada uno.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    # Obtener todos los periodos desde GCS
    periodos = ti.xcom_pull(task_ids="read_periodos_from_gcs")
    
    if not periodos or not isinstance(periodos, list):
        raise ValueError("No se encontraron periodos. La tarea de leer periodos desde GCS debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceMP desde la API de Evaplan para {len(periodos)} periodo(s)...")
    
    # Procesar todos los periodos
    avances_mp = []
    for periodo in periodos:
        peri_idp = periodo.get('peri_idp')
        if not peri_idp:
            print(f"[WARN] Periodo sin peri_idp, se omite: {periodo}")
            continue
        
        print(f"[DEBUG] Procesando periodo: {periodo.get('peri_nombre', 'N/A')} (ID: {peri_idp})")
        
        # Obtener AvanceMP para este periodo
        avance_mp_data = get_avance_mp(token=token, peri_idp=peri_idp)
        avances_mp.append({
            'periodo': periodo,
            'avance_data': avance_mp_data
        })
    
    print(f"[OK] AvanceMP obtenido exitosamente para {len(avances_mp)} periodo(s).")
    
    # Retornar lista de avances con sus periodos
    return avances_mp

def _save_avance_mp_to_gcs_task(ti):
    """
    Task que guarda las respuestas de AvanceMP en archivos JSON en GCS para TODOS los periodos.
    """
    # Obtener datos desde XCom
    avances_mp = ti.xcom_pull(task_ids="get_avance_mp")
    
    if not avances_mp or not isinstance(avances_mp, list):
        raise ValueError("No se encontraron datos de AvanceMP. La tarea de obtener AvanceMP debe ejecutarse primero.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_mp"
    
    print(f"[INFO] Guardando {len(avances_mp)} archivo(s) de AvanceMP en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar cada avance en GCS
    gcs_uris = []
    for item in avances_mp:
        periodo = item.get('periodo')
        avance_data = item.get('avance_data')
        peri_idp = periodo.get('peri_idp')
        
        if not peri_idp or not avance_data:
            print(f"[WARN] Datos incompletos para periodo {periodo.get('peri_nombre', 'N/A')}, se omite")
            continue
        
        # Guardar en GCS
        gcs_uri = save_avance_to_gcs(
            avance_data=avance_data,
            bucket_name=bucket_name,
            folder_name=folder_name,
            tipo_avance="AvanceMP",
            peri_idp=peri_idp
        )
        
        gcs_uris.append(gcs_uri)
        print(f"[OK] AvanceMP guardado para periodo {periodo.get('peri_nombre', 'N/A')}: {gcs_uri}")
    
    print(f"[OK] Se guardaron {len(gcs_uris)} archivo(s) de AvanceMP exitosamente.")
    
    return gcs_uris

def _get_avance_x_subprograma_task(ti):
    """
    Task que obtiene los datos de AvanceXSubprograma desde la API de Evaplan para TODOS los periodos.
    Lee los periodos desde GCS y procesa cada uno.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    # Obtener todos los periodos desde GCS
    periodos = ti.xcom_pull(task_ids="read_periodos_from_gcs")
    
    if not periodos or not isinstance(periodos, list):
        raise ValueError("No se encontraron periodos. La tarea de leer periodos desde GCS debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceXSubprograma desde la API de Evaplan para {len(periodos)} periodo(s)...")
    
    # Procesar todos los periodos
    avances_x_subprograma = []
    for periodo in periodos:
        peri_idp = periodo.get('peri_idp')
        if not peri_idp:
            print(f"[WARN] Periodo sin peri_idp, se omite: {periodo}")
            continue
        
        print(f"[DEBUG] Procesando periodo: {periodo.get('peri_nombre', 'N/A')} (ID: {peri_idp})")
        
        # Obtener AvanceXSubprograma para este periodo
        avance_x_subprograma_data = get_avance_x_subprograma(token=token, peri_idp=peri_idp)
        avances_x_subprograma.append({
            'periodo': periodo,
            'avance_data': avance_x_subprograma_data
        })
    
    print(f"[OK] AvanceXSubprograma obtenido exitosamente para {len(avances_x_subprograma)} periodo(s).")
    
    # Retornar lista de avances con sus periodos
    return avances_x_subprograma

def _save_avance_x_subprograma_to_gcs_task(ti):
    """
    Task que guarda las respuestas de AvanceXSubprograma en archivos JSON en GCS para TODOS los periodos.
    """
    # Obtener datos desde XCom
    avances_x_subprograma = ti.xcom_pull(task_ids="get_avance_x_subprograma")
    
    if not avances_x_subprograma or not isinstance(avances_x_subprograma, list):
        raise ValueError("No se encontraron datos de AvanceXSubprograma. La tarea de obtener AvanceXSubprograma debe ejecutarse primero.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_x_subprograma"
    
    print(f"[INFO] Guardando {len(avances_x_subprograma)} archivo(s) de AvanceXSubprograma en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar cada avance en GCS
    gcs_uris = []
    for item in avances_x_subprograma:
        periodo = item.get('periodo')
        avance_data = item.get('avance_data')
        peri_idp = periodo.get('peri_idp')
        
        if not peri_idp or not avance_data:
            print(f"[WARN] Datos incompletos para periodo {periodo.get('peri_nombre', 'N/A')}, se omite")
            continue
        
        # Guardar en GCS
        gcs_uri = save_avance_to_gcs(
            avance_data=avance_data,
            bucket_name=bucket_name,
            folder_name=folder_name,
            tipo_avance="AvanceXSubprograma",
            peri_idp=peri_idp
        )
        
        gcs_uris.append(gcs_uri)
        print(f"[OK] AvanceXSubprograma guardado para periodo {periodo.get('peri_nombre', 'N/A')}: {gcs_uri}")
    
    print(f"[OK] Se guardaron {len(gcs_uris)} archivo(s) de AvanceXSubprograma exitosamente.")
    
    return gcs_uris

def _get_avance_general_task(ti):
    """
    Task que obtiene los datos de AvanceGeneral desde la API de Evaplan para TODOS los periodos.
    Lee los periodos desde GCS y procesa cada uno.
    """
    # Obtener token desde XCom
    token = ti.xcom_pull(task_ids="authenticate")
    
    if not token:
        raise ValueError("No se encontró token de autenticación. La tarea de autenticación debe ejecutarse primero.")
    
    # Obtener todos los periodos desde GCS
    periodos = ti.xcom_pull(task_ids="read_periodos_from_gcs")
    
    if not periodos or not isinstance(periodos, list):
        raise ValueError("No se encontraron periodos. La tarea de leer periodos desde GCS debe ejecutarse primero.")
    
    print(f"[INFO] Obteniendo AvanceGeneral desde la API de Evaplan para {len(periodos)} periodo(s)...")
    
    # Procesar todos los periodos
    avances_general = []
    for periodo in periodos:
        peri_idp = periodo.get('peri_idp')
        if not peri_idp:
            print(f"[WARN] Periodo sin peri_idp, se omite: {periodo}")
            continue
        
        print(f"[DEBUG] Procesando periodo: {periodo.get('peri_nombre', 'N/A')} (ID: {peri_idp})")
        
        # Obtener AvanceGeneral para este periodo
        avance_general_data = get_avance_general(token=token, peri_idp=peri_idp)
        avances_general.append({
            'periodo': periodo,
            'avance_data': avance_general_data
        })
    
    print(f"[OK] AvanceGeneral obtenido exitosamente para {len(avances_general)} periodo(s).")
    
    # Retornar lista de avances con sus periodos
    return avances_general

def _save_avance_general_to_gcs_task(ti):
    """
    Task que guarda las respuestas de AvanceGeneral en archivos JSON en GCS para TODOS los periodos.
    """
    # Obtener datos desde XCom
    avances_general = ti.xcom_pull(task_ids="get_avance_general")
    
    if not avances_general or not isinstance(avances_general, list):
        raise ValueError("No se encontraron datos de AvanceGeneral. La tarea de obtener AvanceGeneral debe ejecutarse primero.")
    
    bucket_name = DEFAULT_BUCKET_NAME
    folder_name = "data_staging/dpt_planeacion_municipal/api_evaplan/avance_general"
    
    print(f"[INFO] Guardando {len(avances_general)} archivo(s) de AvanceGeneral en GCS...")
    print(f"[INFO] Bucket destino: {bucket_name}")
    print(f"[INFO] Carpeta destino: {folder_name}")
    
    # Guardar cada avance en GCS
    gcs_uris = []
    for item in avances_general:
        periodo = item.get('periodo')
        avance_data = item.get('avance_data')
        peri_idp = periodo.get('peri_idp')
        
        if not peri_idp or not avance_data:
            print(f"[WARN] Datos incompletos para periodo {periodo.get('peri_nombre', 'N/A')}, se omite")
            continue
        
        # Guardar en GCS
        gcs_uri = save_avance_to_gcs(
            avance_data=avance_data,
            bucket_name=bucket_name,
            folder_name=folder_name,
            tipo_avance="AvanceGeneral",
            peri_idp=peri_idp
        )
        
        gcs_uris.append(gcs_uri)
        print(f"[OK] AvanceGeneral guardado para periodo {periodo.get('peri_nombre', 'N/A')}: {gcs_uri}")
    
    print(f"[OK] Se guardaron {len(gcs_uris)} archivo(s) de AvanceGeneral exitosamente.")
    
    return gcs_uris

with DAG(
    dag_id="src_planeacion_ingest_evaplan",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Ejecución manual
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

    # Tarea 4: Leer periodos desde GCS (el JSON más reciente de la fecha actual)
    read_periodos_from_gcs_task = PythonOperator(
        task_id="read_periodos_from_gcs",
        python_callable=_read_periodos_from_gcs_task,
    )

    # Tarea 5: Obtener AvanceMR
    get_avance_mr_task = PythonOperator(
        task_id="get_avance_mr",
        python_callable=_get_avance_mr_task,
    )

    # Tarea 6: Guardar AvanceMR en GCS
    save_avance_mr_task = PythonOperator(
        task_id="save_avance_mr_to_gcs",
        python_callable=_save_avance_mr_to_gcs_task,
    )

    # Tarea 7: Obtener AvanceMP
    get_avance_mp_task = PythonOperator(
        task_id="get_avance_mp",
        python_callable=_get_avance_mp_task,
    )

    # Tarea 8: Guardar AvanceMP en GCS
    save_avance_mp_task = PythonOperator(
        task_id="save_avance_mp_to_gcs",
        python_callable=_save_avance_mp_to_gcs_task,
    )

    # Tarea 9: Obtener AvanceXSubprograma
    get_avance_x_subprograma_task = PythonOperator(
        task_id="get_avance_x_subprograma",
        python_callable=_get_avance_x_subprograma_task,
    )

    # Tarea 10: Guardar AvanceXSubprograma en GCS
    save_avance_x_subprograma_task = PythonOperator(
        task_id="save_avance_x_subprograma_to_gcs",
        python_callable=_save_avance_x_subprograma_to_gcs_task,
    )

    # Tarea 11: Obtener AvanceGeneral
    get_avance_general_task = PythonOperator(
        task_id="get_avance_general",
        python_callable=_get_avance_general_task,
    )

    # Tarea 12: Guardar AvanceGeneral en GCS
    save_avance_general_task = PythonOperator(
        task_id="save_avance_general_to_gcs",
        python_callable=_save_avance_general_to_gcs_task,
    )

    # Tarea para disparar el DAG de load
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_evaplan",
        trigger_dag_id="src_planeacion_load_evaplan",
        wait_for_completion=False,  # No esperar a que termine el DAG de load
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias
    # Flujo: autenticar -> obtener periodos -> guardar periodos -> leer periodos desde GCS -> obtener todos los avances -> guardar todos los avances -> disparar load -> fin
    start >> authenticate_task >> get_periodos_task >> save_periodos_task >> read_periodos_from_gcs_task >> [
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
    
    # Disparar el DAG de load cuando todas las tareas de guardado terminen
    [
        save_avance_mr_task,
        save_avance_mp_task,
        save_avance_x_subprograma_task,
        save_avance_general_task
    ] >> trigger_load_dag >> end

