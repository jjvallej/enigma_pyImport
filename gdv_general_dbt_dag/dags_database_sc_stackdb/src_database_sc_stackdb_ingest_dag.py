"""
DAG de ingesta: extrae datos de Postgres (sc_stackdb) y los escribe en GCS.
Usa PostgresToGCSOperator y parámetros tomados desde config.yaml.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone

import os
import sys


def add_project_root_to_path():
    """Asegura que la carpeta del proyecto (donde vive modules) esté en sys.path."""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while current_dir != "/":
        modules_dir = os.path.join(current_dir, "modules")
        if os.path.exists(modules_dir):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            return
        current_dir = os.path.dirname(current_dir)
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)


add_project_root_to_path()

from modules.config import CONF, DEFAULT_BUCKET_NAME  # noqa: E402

DB_CFG = CONF.database_sc_stackdb


def ensure_gcs_folder():
    """Crea el prefijo en GCS si no existe (GCS no tiene carpetas reales)."""
    hook = GCSHook()
    prefix = f"{DB_CFG.gcs_base_folder}".rstrip("/") + "/"
    # Si ya existe algún objeto con el prefijo, no hace falta crear marcador
    blobs = hook.list(bucket_name=DEFAULT_BUCKET_NAME, prefix=prefix, max_results=1)
    if blobs:
        print(f"[INFO] Prefijo ya existe en GCS: gs://{DEFAULT_BUCKET_NAME}/{prefix}")
        return
    # Crear objeto marcador vacío
    hook.upload(
        bucket_name=DEFAULT_BUCKET_NAME,
        object_name=prefix,
        data=b"",  # marcador vacío
        mime_type="application/x-directory",
    )
    print(f"[OK] Prefijo creado en GCS: gs://{DEFAULT_BUCKET_NAME}/{prefix}")


def check_db_connection():
    """Verifica conexión a Postgres y muestra credenciales usadas (password sin enmascarar, bajo pedido)."""
    conn_id = DB_CFG.connection_id
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_connection(conn_id)

    print("[INFO] Verificando conexión Postgres con:")
    print(f"  host={conn.host} port={conn.port} schema/database={conn.schema}")
    print(f"  login={conn.login} password={conn.password}")
    print(f"  extra={conn.extra_dejson}")

    with hook.get_conn() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT 1;")
            res = cur.fetchone()
            print(f"[OK] Conexión exitosa. SELECT 1 -> {res}")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="src_database_sc_stackdb_ingest_dag",
    default_args=DEFAULT_ARGS,
    description="Exporta tabla de Postgres (sc_stackdb) a GCS",
    schedule=getattr(DB_CFG, "schedule_interval", None),
    start_date=timezone.datetime(2025, 12, 17),
    catchup=False,
    tags=["database", "postgres", "gcs"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_conn = PythonOperator(
        task_id="check_db_connection",
        python_callable=check_db_connection,
    )

    ensure_folder = PythonOperator(
        task_id="ensure_gcs_folder",
        python_callable=ensure_gcs_folder,
    )

    extract_to_gcs = PostgresToGCSOperator(
        task_id="export_encuesta_hogares_to_gcs",
        postgres_conn_id=DB_CFG.connection_id,
        sql=f"SELECT * FROM {DB_CFG.schema}.{DB_CFG.table} LIMIT 1000;",
        bucket=DEFAULT_BUCKET_NAME,
        filename=f"{DB_CFG.gcs_base_folder}/{DB_CFG.export_filename}",
        export_format=DB_CFG.export_format,
        field_delimiter=DB_CFG.field_delimiter,
        gzip=DB_CFG.gzip,
        use_server_side_cursor=DB_CFG.use_server_side_cursor,
    )

    end = EmptyOperator(task_id="end")

    start >> check_conn >> ensure_folder >> extract_to_gcs >> end

