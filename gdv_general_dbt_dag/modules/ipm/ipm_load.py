# modules/ipm_load.py
"""
Módulo para extraer datos desde Google Cloud Storage, transformarlos mínimamente
y cargarlos en la capa bronze de BigQuery.
"""
from google.cloud import bigquery, storage
import pandas as pd
import os, tempfile
from datetime import datetime, timezone
from typing import Iterable, Optional
from airflow.models import Variable

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True

# Valores por defecto
GCS_BUCKET_NAME = "datalake_gdv_dev"
GCS_FOLDER_PATH = "data_staging/dpt_planeacion_municipal/ipm"

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
    Optimizado para archivos grandes.
    """
    gcs = _gcs_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = gcs.bucket(bucket_name).blob(blob_name)

    # Obtener el tamaño del archivo antes de descargar
    blob.reload()
    file_size = blob.size
    file_size_mb = file_size / (1024 * 1024)
    
    if DEBUG:
        print(f"[DEBUG] Descargando archivo desde GCS: {gcs_uri}")
        print(f"[DEBUG] Tamaño del archivo en GCS: {file_size_mb:.2f} MB ({file_size} bytes)")
        if file_size_mb > 100:
            print(f"[WARN] Archivo grande detectado ({file_size_mb:.2f} MB). La descarga puede tomar varios minutos...")

    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    
    # Descargar el archivo
    blob.download_to_filename(tmp_path)
    
    # Verificar que el archivo se descargó completamente
    downloaded_size = os.path.getsize(tmp_path)
    if downloaded_size != file_size:
        raise ValueError(
            f"El archivo no se descargó completamente. "
            f"Tamaño esperado: {file_size} bytes, "
            f"Tamaño descargado: {downloaded_size} bytes"
        )
    
    if DEBUG:
        print(f"[DEBUG] Archivo descargado completamente en {tmp_path}")
        print(f"[DEBUG] Tamaño verificado: {downloaded_size} bytes ({file_size_mb:.2f} MB)")
    
    return tmp_path


def read_excel_raw(local_path: str, sheet_index: int = 0) -> pd.DataFrame:
    """
    Lee un archivo Excel tal cual, sin aplicar ninguna transformación.
    Devuelve el DataFrame exactamente como está en el archivo.
    Optimizado para archivos grandes (hasta varios GB).
    
    Args:
        local_path: Ruta local del archivo Excel
        sheet_index: Índice de la hoja a leer (por defecto 0)
    
    Returns:
        DataFrame con los datos tal cual están en el Excel
    """
    import os
    
    # Verificar que el archivo existe
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"El archivo no existe: {local_path}")
    
    # Verificar el tamaño del archivo
    file_size = os.path.getsize(local_path)
    if file_size == 0:
        raise ValueError(f"El archivo está vacío: {local_path}")
    
    # Convertir bytes a MB para logging
    file_size_mb = file_size / (1024 * 1024)
    
    if DEBUG:
        print(f"[DEBUG] Leyendo archivo: {local_path}")
        print(f"[DEBUG] Tamaño del archivo: {file_size_mb:.2f} MB ({file_size} bytes)")
    
    # Verificar que el archivo comience con los bytes de un Excel (ZIP signature)
    with open(local_path, 'rb') as f:
        first_bytes = f.read(8)
        # ZIP signature (xlsx files are ZIP archives)
        if first_bytes[:2] != b'PK':
            raise ValueError(f"El archivo no parece ser un Excel válido (.xlsx). "
                           f"Los archivos .xlsx deben comenzar con la firma ZIP 'PK'. "
                           f"Primeros bytes: {first_bytes[:8].hex()}")
    
    # Para archivos grandes, usar openpyxl que es más eficiente
    # y puede manejar archivos grandes mejor que xlrd
    try:
        if DEBUG:
            print(f"[DEBUG] Intentando leer archivo grande con engine: openpyxl")
            print(f"[DEBUG] Esto puede tomar varios minutos para archivos de {file_size_mb:.2f} MB...")
        
        # Leer con openpyxl que maneja mejor archivos grandes
        df = pd.read_excel(
            local_path, 
            sheet_name=sheet_index, 
            header=0, 
            engine='openpyxl'
        )
        
        if DEBUG:
            print(f"[DEBUG] Archivo leído exitosamente con engine: openpyxl")
            print(f"[DEBUG] Filas: {len(df)}, Columnas: {len(df.columns)}")
            print(f"[DEBUG] Encabezados: {list(df.columns)}")
        
    except Exception as e:
        error_msg = f"No se pudo leer el archivo Excel: {local_path}\n"
        error_msg += f"Tamaño del archivo: {file_size_mb:.2f} MB ({file_size} bytes)\n"
        error_msg += f"Error: {str(e)}\n"
        error_msg += f"\nSugerencias:\n"
        error_msg += f"- Verifica que el archivo se descargó completamente\n"
        error_msg += f"- Verifica que el archivo no esté corrupto\n"
        error_msg += f"- Para archivos muy grandes (>2GB), considera dividirlos o usar otro formato"
        raise ValueError(error_msg) from e
    
    return df

def transform_excel(local_path: str, sheet_index: int = 0) -> pd.DataFrame:
    """
    Aplica las transformaciones esperadas al Excel de IPM.
    Devuelve un DataFrame listo para cargarse a BigQuery en bronze.
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

    # En la capa bronze NO se deben hacer conversiones de tipos
    # Todos los valores se mantienen como STRING para preservar los datos originales
    # Las transformaciones y limpiezas se harán en la capa silver con dbt
    df["cod_mpio"] = df["cod_mpio"].astype(str)
    df["Municipio"] = df["Municipio"].astype(str)
    
    # Convertir todas las columnas numéricas a STRING para preservar valores originales
    # (incluso si tienen letras, espacios, o caracteres especiales)
    int_columns = [
        "Total",
        "IPM_Pobre_Abs",
        "IPM_No_Pobre_Abs",
    ] + [f"I{k}_{suffix}" for k in range(1, 16) for suffix in ("Con_Privacion_Abs", "Sin_Privacion_Abs")]
    float_columns = [
        "IPM_Pobre_Porc",
        "IPM_No_Pobre_Porc",
    ] + [f"I{k}_{suffix}" for k in range(1, 16) for suffix in ("Con_Privacion_Porc", "Sin_Privacion_Porc")]

    # Mantener todos los valores como STRING en bronze (sin conversión)
    for col in int_columns + float_columns:
        df[col] = df[col].astype(str)

    # (4) agregar timestamp de lectura (UTC)
    df["fecha_lectura"] = datetime.now(timezone.utc)
    return df

def _load_df_to_bq(df: pd.DataFrame, dataset_id: str, table_name: str):
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"

    # Esquema: En bronze todo es STRING (excepto fecha_lectura) para preservar datos originales
    # 2 identificadores + Total + 4 de IPM + 60 (15*4) + fecha_lectura
    schema = [
        bigquery.SchemaField("cod_mpio", "STRING"),
        bigquery.SchemaField("Municipio", "STRING"),
        bigquery.SchemaField("Total", "STRING"),  # STRING en bronze
        bigquery.SchemaField("IPM_Pobre_Abs", "STRING"),  # STRING en bronze
        bigquery.SchemaField("IPM_No_Pobre_Abs", "STRING"),  # STRING en bronze
        bigquery.SchemaField("IPM_Pobre_Porc", "STRING"),  # STRING en bronze
        bigquery.SchemaField("IPM_No_Pobre_Porc", "STRING"),  # STRING en bronze
    ]
    for k in range(1, 16):
        schema += [
            bigquery.SchemaField(f"I{k}_Con_Privacion_Abs", "STRING"),  # STRING en bronze
            bigquery.SchemaField(f"I{k}_Sin_Privacion_Abs", "STRING"),  # STRING en bronze
            bigquery.SchemaField(f"I{k}_Con_Privacion_Porc", "STRING"),  # STRING en bronze
            bigquery.SchemaField(f"I{k}_Sin_Privacion_Porc", "STRING"),  # STRING en bronze
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
    """Función pública para cargar un DataFrame transformado a BigQuery."""
    _load_df_to_bq(df, dataset_id=dataset_id, table_name=table_name)

def load_dataframe_to_bq_raw(df: pd.DataFrame, dataset_id: str, table_name: str):
    """
    Carga un DataFrame a BigQuery sin esquema predefinido.
    Todas las columnas se convierten a STRING para preservar los datos tal cual están.
    Útil para archivos con estructura desconocida (como IPM SISBEN).
    """
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    if DEBUG:
        print(f"[DEBUG] Cargando DataFrame sin esquema predefinido a {table_fqn}")
        print(f"[DEBUG] Columnas del DataFrame: {list(df.columns)}")
        print(f"[DEBUG] Filas: {len(df)}")
    
    # Crear una copia del DataFrame para no modificar el original
    df_to_load = df.copy()
    
    # Convertir todas las columnas a STRING para preservar los datos tal cual
    # Esto evita problemas con tipos mixtos y valores NaN
    for col in df_to_load.columns:
        # Convertir a string, reemplazando NaN con string vacío
        df_to_load[col] = df_to_load[col].astype(str).replace('nan', '').replace('None', '')
    
    if DEBUG:
        print(f"[DEBUG] Todas las columnas convertidas a STRING para preservar datos originales")
    
    # Crear esquema dinámico basado en las columnas del DataFrame
    # Todas las columnas serán STRING para preservar los datos tal cual
    schema = [bigquery.SchemaField(col, "STRING") for col in df_to_load.columns]
    
    # Configuración con esquema dinámico (todas las columnas como STRING)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )
    
    job = client.load_table_from_dataframe(df_to_load, table_fqn, job_config=job_config)
    job.result()
    print(f"[OK] Cargadas {len(df_to_load)} filas en {table_fqn} (todas las columnas como STRING)")


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
