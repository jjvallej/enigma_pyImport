# modules/idc_load.py
"""
Módulo para extraer datos desde Google Cloud Storage, transformarlos mínimamente
y cargarlos en la capa bronze de BigQuery para IDC.
Soporta múltiples hojas del Excel, cada una se carga como una tabla separada.
"""
from google.cloud import bigquery, storage
import pandas as pd
import os, tempfile
from datetime import datetime, timezone
from typing import Iterable, Optional, Dict, List
from airflow.models import Variable

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True

# Valores por defecto
GCS_BUCKET_NAME = "datalake_gdv"
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
        ds.description = "Bronze layer para IDC"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")

def get_latest_excel_from_gcs_folder(bucket_name: str, folder_path: str) -> str:
    """
    Obtiene la URI del último archivo Excel (.xlsx) subido a una carpeta en GCS.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta (ej: 'data_staging/dpt_planeacion_municipal/idc')
    
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

def get_excel_sheet_names(local_path: str) -> List[str]:
    """
    Obtiene la lista de nombres de hojas en el archivo Excel.
    
    Args:
        local_path: Ruta local del archivo Excel
    
    Returns:
        Lista de nombres de hojas
    """
    excel_file = pd.ExcelFile(local_path)
    sheet_names = excel_file.sheet_names
    if DEBUG:
        print(f"[DEBUG] Hojas encontradas en el Excel: {sheet_names}")
    return sheet_names

def transform_excel_sheet(local_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Lee y transforma mínimamente una hoja específica del Excel de IDC.
    Devuelve un DataFrame listo para cargarse a BigQuery en bronze.
    
    Args:
        local_path: Ruta local del archivo Excel
        sheet_name: Nombre de la hoja a leer
    
    Returns:
        DataFrame transformado
    """
    # Leer la hoja específica
    df = pd.read_excel(local_path, sheet_name=sheet_name, header=0)
    
    if DEBUG:
        print(f"[DEBUG] Hoja '{sheet_name}': {df.shape[0]} filas, {df.shape[1]} columnas")
        print(f"[DEBUG] Encabezados: {list(df.columns)[:10]}...")  # Mostrar primeros 10
    
    # En la capa bronze NO se deben hacer conversiones de tipos
    # Todos los valores se mantienen como STRING para preservar los datos originales
    # Las transformaciones y limpiezas se harán en la capa silver con dbt
    
    # Convertir todas las columnas a STRING para preservar valores originales
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Agregar timestamp de lectura (UTC)
    df["fecha_lectura"] = datetime.now(timezone.utc)
    
    # Agregar nombre de la hoja como metadato (opcional, útil para trazabilidad)
    df["nombre_hoja"] = sheet_name
    
    return df

def load_dataframe_to_bq(df: pd.DataFrame, dataset_id: str, table_name: str, schema: Optional[List[bigquery.SchemaField]] = None):
    """
    Carga un DataFrame a BigQuery. Si no se proporciona esquema, se infiere automáticamente.
    
    Args:
        df: DataFrame a cargar
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla
        schema: Esquema opcional (si no se proporciona, se infiere del DataFrame)
    """
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # Si no se proporciona esquema, crear uno basado en el DataFrame
    # En bronze, todas las columnas (excepto fecha_lectura) son STRING
    if schema is None:
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

def load_all_sheets_to_bq(
    local_path: str,
    dataset_id: str,
    sheet_to_table_mapping: Dict[str, str]
) -> Dict[str, str]:
    """
    Carga todas las hojas especificadas del Excel a BigQuery como tablas separadas.
    
    Args:
        local_path: Ruta local del archivo Excel
        dataset_id: ID del dataset en BigQuery
        sheet_to_table_mapping: Diccionario que mapea nombres de hojas a nombres de tablas
                                Ej: {"Dato_original": "idc_raw_data_dato_original"}
    
    Returns:
        Diccionario con el mapeo de hojas a tablas creadas
    """
    # Obtener todas las hojas disponibles
    available_sheets = get_excel_sheet_names(local_path)
    
    # Verificar que todas las hojas requeridas existan
    missing_sheets = set(sheet_to_table_mapping.keys()) - set(available_sheets)
    if missing_sheets:
        raise ValueError(
            f"Las siguientes hojas no se encontraron en el Excel: {missing_sheets}. "
            f"Hojas disponibles: {available_sheets}"
        )
    
    results = {}
    
    # Procesar cada hoja
    for sheet_name, table_name in sheet_to_table_mapping.items():
        if DEBUG:
            print(f"\n[INFO] Procesando hoja '{sheet_name}' -> tabla '{table_name}'")
        
        # Transformar la hoja
        df = transform_excel_sheet(local_path, sheet_name)
        
        # Cargar a BigQuery
        load_dataframe_to_bq(df, dataset_id=dataset_id, table_name=table_name)
        
        results[sheet_name] = table_name
    
    return results

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

