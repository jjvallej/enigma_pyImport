"""
Carga JSONL de SAP API Evaplan desde GCS a BigQuery Bronze.
Usa esquema explícito con todas las columnas como STRING para que fecha_lectura
y run_ts sean siempre STRING (formato "YYYY-MM-DD HH:MM:SS") en ingest, load y transform.

Si la tabla Bronze ya existía con fecha_lectura DATE o run_ts TIMESTAMP, ejecutar una sola vez:
  ALTER TABLE `proyecto.dataset.sap_api_evaplan_raw_data` ALTER COLUMN fecha_lectura SET DATA TYPE STRING;
  ALTER TABLE `proyecto.dataset.sap_api_evaplan_raw_data` ALTER COLUMN run_ts SET DATA TYPE STRING;
"""
import json
from typing import Any, List

from google.cloud import bigquery

from modules.config import CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE, PROJECT_ID
from modules.gcp_utils import get_bq_client, get_gcs_client


def _get_cfg() -> Any:
    return getattr(CONF, "sap_api_evaplan", None)


def _schema_from_first_line(bucket_name: str, blob_path: str) -> List[bigquery.SchemaField]:
    """Lee la primera línea del JSONL en GCS y devuelve un esquema con todas las columnas como STRING."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_bytes()
    first_line = content.split(b"\n")[0].decode("utf-8")
    if not first_line.strip():
        raise ValueError("El archivo JSONL está vacío o la primera línea está vacía")
    record = json.loads(first_line)
    return [bigquery.SchemaField(name, "STRING") for name in record.keys()]


def run_load(
    bucket_name: str | None = None,
    gcs_path: str | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
) -> None:
    """
    Carga el archivo JSONL de GCS a BigQuery Bronze con esquema STRING para todas las columnas.
    Así fecha_lectura y run_ts son siempre STRING (formato "YYYY-MM-DD HH:MM:SS") en ingest, load y transform.
    Si no se pasa gcs_path, se arma con gcs_base_folder y export_filename (sin reemplazar {{ ds_nodash }}).
    """
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")

    bucket_name = bucket_name or DEFAULT_BUCKET_NAME
    if gcs_path is None:
        base_folder = (cfg.gcs_base_folder or "").rstrip("/")
        export_filename = getattr(cfg, "export_filename", "sap_api_evaplan.json")
        gcs_path = f"{base_folder}/{export_filename}"

    dataset_id = dataset_id or getattr(cfg, "target_dataset", None) or DATASET_ID_BRONZE
    table_id = table_id or getattr(cfg, "target_table", "sap_api_evaplan_raw_data")

    schema = _schema_from_first_line(bucket_name, gcs_path)
    uri = f"gs://{bucket_name}/{gcs_path}"

    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=False,
    )
    job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    job.result()
    print(f"[OK] Cargado {uri} -> {table_ref}")
