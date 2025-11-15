# dags/modules/rawdata_ipmv2.py
from google.cloud import bigquery, storage
import pandas as pd
import os, tempfile
from datetime import datetime, timezone
from typing import Iterable, Optional

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True  # ponlo en False cuando ya no necesites logs

# ---------------------------
# Clientes
# ---------------------------
def _bq_client() -> bigquery.Client:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return bigquery.Client(project=PROJECT_ID)

def _gcs_client() -> storage.Client:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return storage.Client(project=PROJECT_ID)

# ---------------------------
# Helpers
# ---------------------------
def ensure_dataset(dataset_id: str, location: str = "us-central1"):
    """Crea el dataset si no existe."""
    client = _bq_client()
    ds_fqn = f"{PROJECT_ID}.{dataset_id}"
    try:
        ds = client.get_dataset(ds_fqn)
        if DEBUG:
            print(f"[OK] Dataset existente: {ds_fqn} ({ds.location})")
    except Exception:
        ds = bigquery.Dataset(ds_fqn)
        ds.location = location
        ds.description = "Bronze layer para IPM v2"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")

def get_latest_excel_from_gcs_folder(bucket_name: str, folder_path: str) -> str:
    """
    Obtiene la URI del último archivo Excel (.xlsx) subido a una carpeta en GCS.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta (ej: 'data_staging/dpt_planeacion_municipal/ipm')
    
    Returns:
        URI completa del archivo más reciente (gs://bucket/folder/file.xlsx)
    """
    gcs = _gcs_client()
    
    # Normalizar la ruta de la carpeta (asegurar que termine con /)
    if not folder_path.endswith('/'):
        folder_path = folder_path + '/'
    
    # Obtener el bucket
    try:
        bucket = gcs.bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder al bucket '{bucket_name}': {e}")
    
    # Listar todos los blobs en la carpeta que terminen en .xlsx
    blobs = list(bucket.list_blobs(prefix=folder_path))
    
    # Filtrar solo archivos .xlsx y obtener el más reciente
    xlsx_files = [blob for blob in blobs if blob.name.lower().endswith('.xlsx') and not blob.name.endswith('/')]
    
    if not xlsx_files:
        raise ValueError(f"No se encontraron archivos .xlsx en la carpeta 'gs://{bucket_name}/{folder_path}'")
    
    # Ordenar por tiempo de actualización (más reciente primero)
    xlsx_files.sort(key=lambda x: x.time_created, reverse=True)
    
    # Tomar el más reciente
    latest_blob = xlsx_files[0]
    gcs_uri = f"gs://{bucket_name}/{latest_blob.name}"
    
    if DEBUG:
        print(f"[DEBUG] Archivos encontrados en la carpeta: {len(xlsx_files)}")
        for blob in xlsx_files[:5]:  # Mostrar los primeros 5
            print(f"[DEBUG]   - {blob.name} (creado: {blob.time_created})")
        print(f"[INFO] Usando archivo más reciente: {latest_blob.name} (creado: {latest_blob.time_created})")
    
    return gcs_uri

def download_excel_from_gcs(gcs_uri: str) -> str:
    """
    Descarga el archivo Excel desde GCS hacia un archivo temporal
    y devuelve la ruta local generada.
    """
    gcs = _gcs_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = gcs.bucket(bucket_name).blob(blob_name)

    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    blob.download_to_filename(tmp_path)
    if DEBUG:
        print(f"[DEBUG] Archivo descargado en {tmp_path}")
    return tmp_path


def transform_excel(local_path: str, sheet_index: int = 0) -> pd.DataFrame:
    """
    Aplica las transformaciones esperadas al Excel de IPM.
    Devuelve un DataFrame listo para cargarse a BigQuery.
    """
    df = pd.read_excel(local_path, sheet_name=sheet_index, header=0)

    # (2) quitar primera fila (suele tener totales o una fila guía)
    if len(df) > 0:
        df = df.iloc[1:, :].reset_index(drop=True)

    # Normaliza nombres actuales para inspección opcional
    if DEBUG:
        print("[DEBUG] Encabezados originales:", list(df.columns))

    # (3) renombrado siguiendo el orden esperado:
    expected_cols = ["cod_mpio", "Municipio", "Total",
                     "IPM_Pobre_Abs", "IPM_No_Pobre_Abs", "IPM_Pobre_Porc", "IPM_No_Pobre_Porc"]
    for k in range(1, 16):
        expected_cols += [
            f"I{k}_Con_Privacion_Abs",
            f"I{k}_Sin_Privacion_Abs",
            f"I{k}_Con_Privacion_Porc",
            f"I{k}_Sin_Privacion_Porc",
        ]

    n_expected = len(expected_cols)
    n_actual = df.shape[1]
    if n_actual < n_expected:
        raise ValueError(
            f"El archivo trae {n_actual} columnas, pero se esperaban al menos {n_expected} "
            f"para mapear todos los indicadores con _Abs/_Porc."
        )
    if n_actual > n_expected and DEBUG:
        print(f"[WARN] El archivo tiene {n_actual} columnas; se tomarán las primeras {n_expected}.")

    df = df.iloc[:, :n_expected].copy()
    df.columns = expected_cols

    # Ajustes mínimos de tipos para respetar el esquema esperado
    # (sin lógica adicional de limpieza)
    df["cod_mpio"] = df["cod_mpio"].astype(str)
    int_columns = [
        "Total",
        "IPM_Pobre_Abs",
        "IPM_No_Pobre_Abs",
    ] + [f"I{k}_{suffix}" for k in range(1, 16) for suffix in ("Con_Privacion_Abs", "Sin_Privacion_Abs")]
    float_columns = [
        "IPM_Pobre_Porc",
        "IPM_No_Pobre_Porc",
    ] + [f"I{k}_{suffix}" for k in range(1, 16) for suffix in ("Con_Privacion_Porc", "Sin_Privacion_Porc")]

    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round().astype("Int64")

    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # (4) agregar timestamp de lectura (UTC)
    df["fecha_lectura"] = datetime.now(timezone.utc)
    return df

def _load_df_to_bq(df: pd.DataFrame, dataset_id: str, table_name: str):
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"

    # Esquema: 2 identificadores + Total + 4 de IPM + 60 (15*4) + fecha_lectura
    schema = [
        bigquery.SchemaField("cod_mpio", "STRING"),
        bigquery.SchemaField("Municipio", "STRING"),
        bigquery.SchemaField("Total", "INT64"),
        bigquery.SchemaField("IPM_Pobre_Abs", "INT64"),
        bigquery.SchemaField("IPM_No_Pobre_Abs", "INT64"),
        bigquery.SchemaField("IPM_Pobre_Porc", "FLOAT"),
        bigquery.SchemaField("IPM_No_Pobre_Porc", "FLOAT"),
    ]
    for k in range(1, 16):
        schema += [
            bigquery.SchemaField(f"I{k}_Con_Privacion_Abs", "INT64"),
            bigquery.SchemaField(f"I{k}_Sin_Privacion_Abs", "INT64"),
            bigquery.SchemaField(f"I{k}_Con_Privacion_Porc", "FLOAT"),
            bigquery.SchemaField(f"I{k}_Sin_Privacion_Porc", "FLOAT"),
        ]
    schema.append(bigquery.SchemaField("fecha_lectura", "TIMESTAMP"))

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_fqn, job_config=job_config)
    job.result()
    print(f"[OK] Cargadas {len(df)} filas en {table_fqn}")


def load_dataframe_to_bq(df: pd.DataFrame, dataset_id: str, table_name: str):
    """Función pública para cargar un DataFrame transformado."""
    _load_df_to_bq(df, dataset_id=dataset_id, table_name=table_name)


def cleanup_temp_paths(paths: Iterable[Optional[str]]):
    """Elimina los archivos temporales indicados (ignora None o paths vacíos)."""
    for path in paths:
        if not path:
            continue
        try:
            os.unlink(path)
            if DEBUG:
                print(f"[DEBUG] Archivo temporal eliminado: {path}")
        except Exception as exc:
            print(f"[WARN] No se pudo eliminar {path}: {exc}")

# ---------------------------
# Core ETL
# ---------------------------
def process_and_load_from_gcs(gcs_uri: str, dataset_id: str, table_name: str, sheet_index: int = 0):
    """
    Paso 0: asegura dataset (se supone ya ejecutado por el DAG).
    Paso 1: lee Excel desde GCS.
    Paso 2: quita la primera fila.
    Paso 3: renombra columnas a *_Abs / *_Porc con patrón fijo.
    Paso 4: agrega columna TIMESTAMP 'fecha_lectura' (UTC).
    Paso 5: guarda en BigQuery (WRITE_TRUNCATE) => dataset.table.
    """
    local_path = download_excel_from_gcs(gcs_uri)
    try:
        df = transform_excel(local_path, sheet_index=sheet_index)
        _load_df_to_bq(df, dataset_id=dataset_id, table_name=table_name)
    finally:
        cleanup_temp_paths([local_path])
