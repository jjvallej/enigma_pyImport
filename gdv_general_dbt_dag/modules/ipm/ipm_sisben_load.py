# modules/ipm/ipm_sisben_load.py
"""
Módulo simplificado para procesar archivos Excel IPM SISBEN desde Google Cloud Storage.
"""
from google.cloud import bigquery
import pandas as pd
import os
import tempfile
from datetime import datetime
from typing import Optional
from modules.config import PROJECT_ID, DATASET_ID_BRONZE, CONF, LOCATION
from modules.gcp_utils import get_bq_client, get_gcs_client

DEBUG = CONF.global_config.debug

def ensure_dataset(dataset_id: str, location: str = None):
    """Crea el dataset si no existe."""
    if location is None:
        location = LOCATION
    client = get_bq_client()
    ds_fqn = f"{PROJECT_ID}.{dataset_id}"
    try:
        ds = client.get_dataset(ds_fqn)
        if DEBUG:
            print(f"[OK] Dataset existente: {ds_fqn} ({ds.location})")
    except Exception:
        ds = bigquery.Dataset(ds_fqn)
        ds.location = location
        ds.description = "Bronze layer para IPM SISBEN"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")


def leer_excel_desde_bucket(bucket_name: str, folder_path: str) -> pd.DataFrame:
    """
    Lee el archivo Excel más reciente desde el bucket de GCS.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta donde está el archivo Excel
    
    Returns:
        DataFrame de pandas con los datos del Excel
    """
    gcs = get_gcs_client()
    
    # Normalizar la ruta de la carpeta
    if not folder_path.endswith('/'):
        folder_path = folder_path + '/'
    
    try:
        bucket = gcs.bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder al bucket '{bucket_name}': {e}")
    
    # Listar todos los blobs en la carpeta que terminen en .xlsx
    blobs = list(bucket.list_blobs(prefix=folder_path))
    
    # Filtrar solo archivos .xlsx
    xlsx_files = [blob for blob in blobs if blob.name.lower().endswith('.xlsx') and not blob.name.endswith('/')]
    
    if not xlsx_files:
        raise ValueError(f"No se encontraron archivos .xlsx en la carpeta 'gs://{bucket_name}/{folder_path}'")
    
    # Ordenar por tiempo de actualización (más reciente primero)
    xlsx_files.sort(key=lambda x: x.time_created, reverse=True)
    
    latest_blob = xlsx_files[0]
    gcs_uri = f"gs://{bucket_name}/{latest_blob.name}"
    
    if DEBUG:
        print(f"[INFO] Leyendo archivo Excel desde: {gcs_uri}")
        size_str = f"{latest_blob.size / (1024*1024):.2f} MB" if latest_blob.size is not None else "desconocido"
        print(f"[INFO] Tamaño del archivo: {size_str}")
    
    # Descargar el archivo a un archivo temporal
    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    
    try:
        latest_blob.download_to_filename(tmp_path)
        if DEBUG:
            print(f"[INFO] Archivo descargado temporalmente a: {tmp_path}")
        
        # Leer el Excel con pandas (igual que en el notebook)
        print(f"[INFO] Leyendo Excel con pandas...")
        df = pd.read_excel(tmp_path)
        
        if DEBUG:
            print(f"[OK] Excel leído exitosamente: {len(df)} filas, {len(df.columns)} columnas")
        
        return df
    
    finally:
        # Limpiar archivo temporal
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
            if DEBUG:
                print(f"[DEBUG] Archivo temporal eliminado: {tmp_path}")

def convertir_excel_a_csv_y_guardar(df: pd.DataFrame, bucket_name: str, folder_path: str, csv_filename: Optional[str] = None) -> str:
    """
    Convierte un DataFrame a CSV y lo guarda en el bucket de GCS.
    
    Args:
        df: DataFrame de pandas a convertir
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta donde guardar el CSV
        csv_filename: Nombre del archivo CSV (opcional, se genera automáticamente si no se proporciona)
    
    Returns:
        URI completa del archivo CSV guardado (gs://bucket/folder/file.csv)
    """
    gcs = get_gcs_client()
    
    # Normalizar la ruta de la carpeta
    folder_path = folder_path.strip('/')
    
    # Generar nombre del archivo CSV si no se proporciona
    if csv_filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"ipm_sisben_{timestamp}.csv"
    
    # Asegurar que el nombre termine en .csv
    if not csv_filename.lower().endswith('.csv'):
        csv_filename = csv_filename + '.csv'
    
    # Construir ruta completa en GCS
    blob_name = f"{folder_path}/{csv_filename}"
    
    if DEBUG:
        print(f"[INFO] Convirtiendo DataFrame a CSV...")
        print(f"[INFO] Filas: {len(df)}, Columnas: {len(df.columns)}")
    
    # Crear archivo CSV temporal
    fd, tmp_csv = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    
    try:
        # Guardar DataFrame como CSV
        df.to_csv(tmp_csv, index=False, encoding='utf-8')
        
        if DEBUG:
            csv_size = os.path.getsize(tmp_csv)
            print(f"[INFO] CSV creado: {csv_size / (1024*1024):.2f} MB")
        
        # Subir CSV a GCS
        bucket = gcs.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        print(f"[INFO] Subiendo CSV a GCS: gs://{bucket_name}/{blob_name}")
        blob.upload_from_filename(tmp_csv)
        
        gcs_uri = f"gs://{bucket_name}/{blob_name}"
        print(f"[OK] CSV guardado exitosamente en: {gcs_uri}")
        
        return gcs_uri
    
    finally:
        # Limpiar archivo temporal
        if os.path.exists(tmp_csv):
            os.unlink(tmp_csv)
            if DEBUG:
                print(f"[DEBUG] Archivo temporal CSV eliminado: {tmp_csv}")

