"""
Ingesta de tabla BigQuery desde otro proyecto (observatorio-de-seguridad) a nuestro GCS.
Usa la conexión de Airflow (Keyfile JSON del SA del otro proyecto) para leer;
escribe en GCS con las credenciales por defecto del entorno (Composer).
"""
import csv
import io
from typing import Any, Optional

from google.cloud import bigquery
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from modules.config import CONF, DEFAULT_BUCKET_NAME
from modules.gcp_utils import get_gcs_client


def _get_cfg() -> Any:
    return getattr(CONF, "delitos_historicos", None)


def run_ingest(
    connection_id: str,
    ds_nodash: Optional[str] = None,
    bucket_name: Optional[str] = None,
) -> str:
    """
    Lee la tabla de BigQuery del proyecto externo (usando la conexión) y la sube a nuestro GCS como CSV.

    Args:
        connection_id: Id de la conexión en Airflow (tipo Google Cloud, Keyfile JSON del SA del otro proyecto).
        ds_nodash: Fecha de ejecución en formato YYYYMMDD para el nombre del archivo. Si no se pasa, se usa "latest".
        bucket_name: Bucket de destino. Si no se pasa, se usa DEFAULT_BUCKET_NAME del config.

    Returns:
        Ruta GCS del archivo escrito (gs://bucket/path/file.csv).
    """
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'delitos_historicos' en config.yaml")

    bucket = bucket_name or DEFAULT_BUCKET_NAME
    source_project = cfg.source_project_id
    source_dataset = cfg.source_dataset
    source_table = cfg.source_table
    base_folder = (cfg.gcs_base_folder or "").rstrip("/")
    export_filename = (cfg.export_filename or "historico_2016_2025.csv").replace("{{ ds_nodash }}", ds_nodash or "latest")
    object_path = f"{base_folder}/{export_filename}"
    delimiter = getattr(cfg, "field_delimiter", ",") or ","

    # Cliente BigQuery con credenciales del otro proyecto (desde la conexión de Airflow)
    hook = GoogleBaseHook(gcp_conn_id=connection_id)
    credentials = hook.get_credentials()
    bq_client = bigquery.Client(project=source_project, credentials=credentials)

    # Leer la tabla con list_rows (no ejecuta una consulta / no crea job; solo requiere lectura sobre la tabla)
    table_ref = f"{source_project}.{source_dataset}.{source_table}"
    table = bq_client.get_table(table_ref)
    fieldnames = [f.name for f in table.schema]
    rows = bq_client.list_rows(table_ref)

    # Escribir CSV en memoria y subir a GCS
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, delimiter=delimiter, lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(dict(row))

    buffer.seek(0)
    content = buffer.getvalue().encode("utf-8")

    gcs_client = get_gcs_client()
    bucket_obj = gcs_client.bucket(bucket)
    blob = bucket_obj.blob(object_path)
    blob.upload_from_string(content, content_type="text/csv")

    gcs_uri = f"gs://{bucket}/{object_path}"
    return gcs_uri


def ensure_gcs_folder(bucket_name: Optional[str] = None) -> None:
    """Crea el prefijo en GCS si no existe (marcador de carpeta)."""
    cfg = _get_cfg()
    if not cfg:
        return
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    bucket = bucket_name or DEFAULT_BUCKET_NAME
    prefix = (cfg.gcs_base_folder or "").rstrip("/") + "/"
    hook = GCSHook()
    blobs = list(hook.list(bucket_name=bucket, prefix=prefix, max_results=1))
    if blobs:
        return
    hook.upload(
        bucket_name=bucket,
        object_name=prefix,
        data=b"",
        mime_type="application/x-directory",
    )
