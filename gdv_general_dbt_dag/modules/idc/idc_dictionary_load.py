# modules/idc_dictionary_load.py
"""
Módulo para extraer datos del archivo CSV del diccionario IDC desde Google Cloud Storage,
transformarlos mínimamente y cargarlos en la capa bronze de BigQuery.
"""
from google.cloud import bigquery, storage
import pandas as pd
import os, tempfile
from datetime import datetime, timezone
from typing import Iterable, Optional

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"

from modules.config import CONF
DEBUG = CONF.global_config.debug

# Valores por defecto
GCS_BUCKET_NAME = "datalake_gdv_dev"
GCS_FOLDER_PATH = "data_staging/dpt_planeacion_municipal/idc"

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
        ds.description = "Gold layer para IDC Dictionary"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")

def get_latest_csv_from_gcs_folder(bucket_name: str, folder_path: str) -> str:
    """
    Obtiene la URI del último archivo CSV (.csv) subido a una carpeta en GCS.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta (ej: 'data_staging/dpt_planeacion_municipal/idc')
    
    Returns:
        URI completa del archivo más reciente (gs://bucket/folder/file.csv)
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
    
    # Listar todos los blobs en la carpeta que terminen en .csv
    blobs = list(bucket.list_blobs(prefix=folder_path))
    
    # Filtrar solo archivos .csv y obtener el más reciente
    csv_files = [blob for blob in blobs if blob.name.lower().endswith('.csv') and not blob.name.endswith('/')]
    
    if not csv_files:
        raise ValueError(f"No se encontraron archivos .csv en la carpeta 'gs://{bucket_name}/{folder_path}'")
    
    # Ordenar por tiempo de actualización (más reciente primero)
    csv_files.sort(key=lambda x: x.time_created, reverse=True)
    
    # Tomar el más reciente
    latest_blob = csv_files[0]
    gcs_uri = f"gs://{bucket_name}/{latest_blob.name}"
    
    if DEBUG:
        print(f"[DEBUG] Archivos encontrados en la carpeta: {len(csv_files)}")
        for blob in csv_files[:5]:  # Mostrar los primeros 5
            print(f"[DEBUG]   - {blob.name} (creado: {blob.time_created})")
        print(f"[INFO] Usando archivo más reciente: {latest_blob.name} (creado: {latest_blob.time_created})")
    
    return gcs_uri

def download_csv_from_gcs(gcs_uri: str) -> str:
    """
    Descarga el archivo CSV desde GCS hacia un archivo temporal
    y devuelve la ruta local generada.
    """
    gcs = _gcs_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = gcs.bucket(bucket_name).blob(blob_name)

    fd, tmp_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    blob.download_to_filename(tmp_path)
    if DEBUG:
        print(f"[DEBUG] Archivo CSV descargado en {tmp_path}")
    return tmp_path

def load_csv_to_bq(
    local_path: str,
    dataset_id: str,
    table_name: str
) -> str:
    """
    Carga un archivo CSV a BigQuery como tabla.
    
    Args:
        local_path: Ruta local del archivo CSV
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla
    
    Returns:
        Nombre de la tabla creada
    """
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # Leer el CSV con pandas
    df = pd.read_csv(local_path, encoding='utf-8')
    
    if DEBUG:
        print(f"[DEBUG] CSV leído: {df.shape[0]} filas, {df.shape[1]} columnas")
        print(f"[DEBUG] Columnas: {list(df.columns)}")
    
    # Convertir todas las columnas a STRING para preservar valores originales en bronze
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Agregar timestamp de lectura (UTC)
    df["fecha_lectura"] = datetime.now(timezone.utc)
    
    # Crear esquema: todas las columnas son STRING excepto fecha_lectura
    schema = []
    for col in df.columns:
        if col == "fecha_lectura":
            schema.append(bigquery.SchemaField(col, "TIMESTAMP"))
        else:
            schema.append(bigquery.SchemaField(col, "STRING"))
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )
    
    job = client.load_table_from_dataframe(df, table_fqn, job_config=job_config)
    job.result()
    print(f"[OK] Cargadas {len(df)} filas en {table_fqn}")
    
    return table_name

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

