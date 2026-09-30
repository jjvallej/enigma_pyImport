"""
Carga JSONL de SAP API Evaplan desde GCS a BigQuery Bronze.
- Si la tabla Bronze ya existe: usa el esquema actual de la tabla para la carga, así no hay
  conflicto cuando el mismo campo viene como entero en un periodo y float en otro (ej. saldosolpe).
- Si la tabla no existe: crea con esquema STRING para todas las columnas (desde la primera línea del archivo).

Si la tabla ya existía con fecha_lectura DATE o run_ts TIMESTAMP, ejecutar una sola vez:
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


def _get_schema_for_load(
    client: bigquery.Client,
    table_ref: str,
    bucket_name: str,
    blob_path: str,
) -> List[bigquery.SchemaField]:
    """
    Define el esquema a usar en el load:
    - Si la tabla ya existe: usa el esquema de la tabla (mismo orden que las claves del archivo).
      Así no hay cambio de tipo (ej. saldosolpe FLOAT sigue siendo FLOAT aunque el JSON traiga enteros).
    - Si la tabla no existe: esquema STRING desde la primera línea del archivo.
    """
    try:
        table = client.get_table(table_ref)
        existing_types = {f.name: f for f in table.schema}
    except Exception:
        existing_types = {}

    first_line_schema = _schema_from_first_line(bucket_name, blob_path)
    if not existing_types:
        return first_line_schema

    # Usar tipos de la tabla para columnas que existan; STRING para columnas nuevas
    result = []
    for field in first_line_schema:
        if field.name in existing_types:
            result.append(existing_types[field.name])
        else:
            result.append(bigquery.SchemaField(field.name, "STRING"))
    return result


def _resolve_gcs_path_for_execution(bucket_name: str, base_folder: str, ds_nodash: str) -> str:
    """
    Busca el archivo de SAP Evaplan para la fecha de ejecución en la estructura:
    <base_folder>/YYYY/MM/sap_api_evaplan_YYYYMMDD_HHMMSS.json
    y devuelve el más reciente.
    """
    if not ds_nodash or len(ds_nodash) < 8:
        raise ValueError("ds_nodash inválido para resolver ruta jerárquica (esperado YYYYMMDD)")

    yyyy = ds_nodash[:4]
    mm = ds_nodash[4:6]
    prefix = f"{base_folder}/{yyyy}/{mm}/sap_api_evaplan_{ds_nodash}_"

    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=prefix))
    files = [b for b in blobs if not b.name.endswith("/")]
    if not files:
        raise FileNotFoundError(
            f"No se encontró archivo SAP Evaplan para ds_nodash={ds_nodash} con prefijo gs://{bucket_name}/{prefix}"
        )

    files.sort(key=lambda b: b.time_created or b.updated, reverse=True)
    return files[0].name


def run_load(
    bucket_name: str | None = None,
    gcs_path: str | None = None,
    ds_nodash: str | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
) -> None:
    """
    Carga el archivo JSONL de GCS a BigQuery Bronze.
    Si la tabla ya existe, usa su esquema para evitar conflictos FLOAT/INTEGER entre cargas.
    Si no se pasa gcs_path:
    - si llega ds_nodash, busca el archivo más reciente de ese día en la estructura jerárquica YYYY/MM.
    - si no llega ds_nodash, usa fallback con gcs_base_folder + export_filename.
    """
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")

    bucket_name = bucket_name or DEFAULT_BUCKET_NAME
    if gcs_path is None:
        base_folder = (cfg.gcs_base_folder or "").rstrip("/")
        if ds_nodash:
            gcs_path = _resolve_gcs_path_for_execution(bucket_name, base_folder, ds_nodash)
        else:
            export_filename = getattr(cfg, "export_filename", "sap_api_evaplan.json")
            gcs_path = f"{base_folder}/{export_filename}"

    dataset_id = dataset_id or getattr(cfg, "target_dataset", None) or DATASET_ID_BRONZE
    table_id = table_id or getattr(cfg, "target_table", "sap_api_evaplan_raw_data")

    uri = f"gs://{bucket_name}/{gcs_path}"
    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_id}"

    schema = _get_schema_for_load(client, table_ref, bucket_name, gcs_path)

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=False,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    job.result()
    print(f"[OK] Cargado {uri} -> {table_ref}")
    print("[SAP_EVAPLAN_LOAD] run_load() completado (módulo sap_api_evaplan_load)")
