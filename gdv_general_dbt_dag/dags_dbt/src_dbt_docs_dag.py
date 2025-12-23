# dags/src_dbt_docs_dag.py
"""
DAG para generar y publicar la documentación de dbt en Google Cloud Storage.

Este DAG:
1. Genera la documentación de dbt usando `dbt docs generate`
2. Sube los archivos generados a un bucket de GCS
3. La documentación queda disponible como sitio web estático

La documentación se puede acceder desde:
- URL directa: https://storage.googleapis.com/BUCKET_NAME/dbt_docs/index.html
- O mediante Cloud Load Balancer / Cloud Run para acceso controlado

Configuración requerida:
- El bucket debe existir y el service account debe tener permisos de escritura
- Para servir como sitio web, el bucket debe configurarse para hosting estático
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
import os
import sys

# Función para encontrar la raíz del proyecto
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

# === CONFIGURACIÓN ===
# Calcular project_root antes de usarlo en los imports
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_PROJECT_DIR = os.path.join(project_root, "dbt")

# Verificar que el módulo existe antes de importar
dbt_docs_module_path = os.path.join(project_root, "modules", "dbt_docs")
if not os.path.exists(dbt_docs_module_path):
    raise ImportError(
        f"No se encontró el módulo dbt_docs en: {dbt_docs_module_path}\n"
        f"Ruta del proyecto: {project_root}\n"
        f"sys.path: {sys.path}"
    )

# Importar desde el módulo (el __init__.py exporta las funciones)
try:
    from modules.dbt_docs import (
        generate_dbt_docs,
        upload_dbt_docs_to_gcs,
    )
except ImportError as e:
    # Fallback: importar directamente desde el archivo
    try:
        from modules.dbt_docs.dbt_docs_upload import (
            generate_dbt_docs,
            upload_dbt_docs_to_gcs,
        )
    except ImportError as e2:
        raise ImportError(
            f"No se pudo importar el módulo dbt_docs.\n"
            f"Error desde módulo: {e}\n"
            f"Error desde archivo: {e2}\n"
            f"Ruta del proyecto: {project_root}\n"
            f"Módulo esperado en: {dbt_docs_module_path}"
        )

from modules.config import CONF, PROJECT_ID, DEFAULT_BUCKET_NAME
# getattr es una función built-in de Python, no necesita importarse

# Alias para mantener consistencia con el nombre usado en el código
BUCKET_NAME = DEFAULT_BUCKET_NAME

# Leer configuración desde config.yaml
DBT_TARGET = getattr(CONF.dbt_docs, "target", "dev")
DOCS_DESTINATION_PREFIX = getattr(CONF.dbt_docs, "destination_prefix", "dbt_docs")
SCHEDULE_INTERVAL_CONFIG = getattr(CONF.dbt_docs, "schedule_interval", "@daily")

# Convertir schedule_interval de string a objeto timedelta si es necesario
# NOTA: En Airflow 2.4+, el parámetro es 'schedule' (no 'schedule_interval')
def _parse_schedule_interval(schedule_str):
    """Convierte string de schedule_interval a objeto timedelta, None, o string (cron)."""
    if schedule_str is None or schedule_str == "null" or schedule_str == "":
        return None
    elif schedule_str == "@daily":
        return timedelta(days=1)
    elif schedule_str == "@weekly":
        return timedelta(weeks=1)
    elif schedule_str == "@monthly":
        return timedelta(days=30)
    else:
        # Si es un cron expression o otro formato, devolver como string
        return schedule_str

SCHEDULE_INTERVAL = _parse_schedule_interval(SCHEDULE_INTERVAL_CONFIG)

def _generate_dbt_docs_task():
    """Genera la documentación de dbt."""
    # Leer configuración desde config.yaml (puede cambiar entre ejecuciones)
    dbt_target = getattr(CONF.dbt_docs, "target", "dev")
    
    print(f"[INFO] Generando documentación de dbt...")
    print(f"[INFO] Directorio del proyecto dbt: {DBT_PROJECT_DIR}")
    print(f"[INFO] Target: {dbt_target}")
    
    docs_dir = generate_dbt_docs(
        dbt_project_dir=DBT_PROJECT_DIR,
        target=dbt_target
    )
    
    print(f"[OK] Documentación generada en: {docs_dir}")
    return docs_dir

def _upload_dbt_docs_task(**context):
    """Sube la documentación generada a GCS."""
    # Obtener el directorio de documentación del task anterior
    ti = context['ti']
    docs_dir = ti.xcom_pull(task_ids="generate_dbt_docs")
    
    if not docs_dir:
        raise ValueError("No se pudo obtener el directorio de documentación del task anterior")
    
    # Leer configuración desde config.yaml
    docs_prefix = getattr(CONF.dbt_docs, "destination_prefix", "dbt_docs")
    
    print(f"[INFO] Subiendo documentación a GCS...")
    print(f"[INFO] Bucket: {BUCKET_NAME}")
    print(f"[INFO] Prefijo: {docs_prefix}")
    
    gcs_uri = upload_dbt_docs_to_gcs(
        local_docs_dir=docs_dir,
        bucket_name=BUCKET_NAME,
        destination_prefix=docs_prefix,
        overwrite=True
    )
    
    # URL pública del sitio (si el bucket está configurado para hosting estático)
    public_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{docs_prefix}/index.html"
    
    print(f"[OK] Documentación subida a: {gcs_uri}")
    print(f"[INFO] URL pública: {public_url}")
    print(f"[INFO] Para acceder a la documentación, asegúrate de que el bucket esté configurado para hosting estático")
    
    return {
        "gcs_uri": gcs_uri,
        "public_url": public_url
    }

with DAG(
    dag_id="src_dbt_docs_generate",
    start_date=datetime(2024, 1, 1),
    schedule=SCHEDULE_INTERVAL,  # Configurado desde config.yaml (Airflow 2.4+ usa 'schedule' en lugar de 'schedule_interval')
    catchup=False,
    tags=["dbt", "documentacion", "docs"],
    description="Genera y publica la documentación de dbt en Google Cloud Storage. La documentación se actualiza según la configuración en config.yaml y queda disponible como sitio web estático.",
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    
    start = EmptyOperator(
        task_id="start",
        doc_md="Inicio del proceso de generación de documentación de dbt."
    )
    
    generate_docs = PythonOperator(
        task_id="generate_dbt_docs",
        python_callable=_generate_dbt_docs_task,
        doc_md="""
        Genera la documentación de dbt ejecutando `dbt docs generate`.
        
        La documentación se genera en el directorio `target/` del proyecto dbt.
        """
    )
    
    upload_docs = PythonOperator(
        task_id="upload_dbt_docs_to_gcs",
        python_callable=_upload_dbt_docs_task,
        doc_md="""
        Sube todos los archivos de la documentación generada a Google Cloud Storage.
        
        Los archivos se suben al bucket configurado en config.yaml bajo el prefijo configurado.
        
        Una vez subidos, la documentación estará disponible en:
        - GCS URI: `gs://BUCKET_NAME/PREFIX/`
        - URL pública: `https://storage.googleapis.com/BUCKET_NAME/PREFIX/index.html`
        
        **Nota**: Para que la URL pública funcione, el bucket debe estar configurado para hosting estático.
        La configuración se lee desde `config.yaml` en la sección `dbt_docs`.
        """
    )
    
    end = EmptyOperator(
        task_id="end",
        doc_md="Fin del proceso de generación y publicación de documentación de dbt."
    )
    
    # Definir dependencias
    start >> generate_docs >> upload_docs >> end

