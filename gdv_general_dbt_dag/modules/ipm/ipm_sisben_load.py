# modules/ipm/ipm_sisben_load.py
"""
Módulo para cargar archivos Excel IPM SISBEN desde Google Cloud Storage a BigQuery.
Optimizado para archivos grandes (1.5GB+) convirtiendo a Parquet y cargando desde GCS.
Esto es MUCHO más rápido que cargar desde DataFrame en memoria.
"""
from google.cloud import bigquery, storage
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import tempfile
from datetime import datetime, timezone
from typing import Optional, List
from modules.config import PROJECT_ID, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE, CONF
from modules.gcp_utils import get_bq_client, get_gcs_client

DEBUG = CONF.global_config.debug

# Tamaño de chunk para archivos grandes (filas por chunk)
CHUNK_SIZE = 100000  # 100k filas por chunk

def ensure_dataset(dataset_id: str, location: str = "us-central1"):
    """Crea el dataset si no existe."""
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

def get_latest_excel_from_gcs_folder(bucket_name: str, folder_path: str) -> str:
    """
    Obtiene la URI del último archivo Excel (.xlsx) subido a una carpeta en GCS.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta (ej: 'data_staging/dpt_planeacion_municipal/ipm/sisben')
    
    Returns:
        URI completa del archivo más reciente (gs://bucket/folder/file.xlsx)
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
        print(f"[DEBUG] Archivos encontrados en la carpeta: {len(xlsx_files)}")
        for blob in xlsx_files[:5]:
            size_str = f"{blob.size / (1024*1024):.2f} MB" if blob.size is not None else "desconocido"
            print(f"[DEBUG]   - {blob.name} (creado: {blob.time_created}, tamaño: {size_str})")
        latest_size_str = f"{latest_blob.size / (1024*1024):.2f} MB" if latest_blob.size is not None else "desconocido"
        print(f"[INFO] Usando archivo más reciente: {latest_blob.name} (creado: {latest_blob.time_created}, tamaño: {latest_size_str})")
    
    return gcs_uri

def download_excel_from_gcs(gcs_uri: str) -> str:
    """
    Descarga el archivo Excel desde GCS hacia un archivo temporal
    y devuelve la ruta local generada.
    """
    gcs = get_gcs_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = gcs.bucket(bucket_name).blob(blob_name)

    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    
    if DEBUG:
        print(f"[INFO] Descargando archivo desde GCS: {gcs_uri}")
        # blob.size puede ser None en algunos casos, manejarlo
        if blob.size is not None:
            print(f"[INFO] Tamaño: {blob.size / (1024*1024):.2f} MB")
        else:
            print(f"[INFO] Tamaño: desconocido (se obtendrá después de la descarga)")
    
    blob.download_to_filename(tmp_path)
    
    if DEBUG:
        print(f"[DEBUG] Archivo descargado en {tmp_path}")
        file_size = os.path.getsize(tmp_path)
        print(f"[DEBUG] Tamaño local: {file_size / (1024*1024):.2f} MB")
    
    return tmp_path

def _normalize_column_name(col_name: str) -> str:
    """
    Normaliza nombres de columnas para BigQuery.
    Reemplaza espacios y caracteres especiales con guiones bajos.
    """
    import re
    # Convertir a string y limpiar
    col_name = str(col_name).strip()
    # Reemplazar espacios y caracteres especiales con guiones bajos
    col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
    # Eliminar guiones bajos múltiples
    col_name = re.sub(r'_+', '_', col_name)
    # Eliminar guiones bajos al inicio y final
    col_name = col_name.strip('_')
    # Si está vacío, usar un nombre por defecto
    if not col_name:
        col_name = "unnamed_column"
    return col_name

def convert_excel_sheet_to_parquet(
    local_path: str,
    sheet_name: str,
    bucket_name: str,
    gcs_folder: str
) -> str:
    """
    Convierte una hoja del Excel a Parquet y la sube a GCS.
    Esto es mucho más rápido que cargar desde DataFrame en memoria.
    
    Args:
        local_path: Ruta local del archivo Excel
        sheet_name: Nombre de la hoja a convertir
        bucket_name: Nombre del bucket en GCS
        gcs_folder: Carpeta en GCS donde subir el Parquet
    
    Returns:
        URI del archivo Parquet en GCS (gs://bucket/folder/file.parquet)
    """
    gcs_client = get_gcs_client()
    
    if DEBUG:
        print(f"[INFO] Convirtiendo hoja '{sheet_name}' a Parquet...")
    
    # Leer solo las primeras filas para obtener los encabezados
    df_header = pd.read_excel(local_path, sheet_name=sheet_name, nrows=2)
    
    # La primera fila puede ser metadatos, usar la segunda como encabezados si existe
    if len(df_header) >= 2:
        headers = df_header.iloc[1].astype(str).tolist()
        skip_rows = 2  # Saltar las primeras 2 filas
    else:
        headers = df_header.iloc[0].astype(str).tolist()
        skip_rows = 1  # Saltar solo la primera fila
    
    # Normalizar nombres de columnas
    headers = [_normalize_column_name(h) for h in headers]
    
    if DEBUG:
        print(f"[INFO] Encabezados detectados: {len(headers)} columnas")
        print(f"[INFO] Saltando {skip_rows} fila(s) inicial(es)")
        print(f"[INFO] Leyendo hoja completa...")
    
    # Leer el archivo completo
    try:
        df = pd.read_excel(local_path, sheet_name=sheet_name, skiprows=skip_rows, header=None)
        
        # Asignar encabezados
        if len(df.columns) == len(headers):
            df.columns = headers
        else:
            # Ajustar número de columnas
            if len(df.columns) < len(headers):
                for i in range(len(df.columns), len(headers)):
                    df[headers[i]] = None
            else:
                df = df.iloc[:, :len(headers)]
            df.columns = headers
        
        if DEBUG:
            print(f"[INFO] Hoja leída: {len(df)} filas, {len(df.columns)} columnas")
        
    except MemoryError:
        raise Exception(
            f"La hoja '{sheet_name}' es demasiado grande para leer en memoria. "
            f"Tamaño del archivo: {os.path.getsize(local_path) / (1024*1024):.2f} MB."
        )
    
    # Convertir todas las columnas a string (preservar datos originales en bronze)
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Agregar timestamp y nombre de hoja
    df["fecha_lectura"] = datetime.now(timezone.utc)
    df["nombre_hoja"] = sheet_name
    
    # Crear archivo Parquet temporal
    fd, tmp_parquet = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    
    if DEBUG:
        print(f"[INFO] Escribiendo Parquet temporal: {tmp_parquet}")
    
    # Escribir a Parquet (mucho más rápido y eficiente que Excel)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, tmp_parquet, compression='snappy')
    
    parquet_size = os.path.getsize(tmp_parquet)
    if DEBUG:
        print(f"[INFO] Parquet creado: {parquet_size / (1024*1024):.2f} MB (reducción de {((1 - parquet_size/os.path.getsize(local_path)) * 100):.1f}%)")
    
    # Subir a GCS
    bucket = gcs_client.bucket(bucket_name)
    # Nombre del archivo: usar nombre de hoja normalizado
    sheet_name_normalized = _normalize_column_name(sheet_name)
    blob_name = f"{gcs_folder}/temp/{sheet_name_normalized}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    blob = bucket.blob(blob_name)
    
    if DEBUG:
        print(f"[INFO] Subiendo Parquet a GCS: gs://{bucket_name}/{blob_name}")
    
    blob.upload_from_filename(tmp_parquet)
    
    # Limpiar archivo temporal local
    os.unlink(tmp_parquet)
    
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    
    if DEBUG:
        print(f"[OK] Parquet subido a: {gcs_uri}")
    
    return gcs_uri

def load_parquet_from_gcs_to_bq(
    gcs_uri: str,
    dataset_id: str,
    table_name: str,
    write_disposition: str = "WRITE_APPEND"
) -> int:
    """
    Carga un archivo Parquet desde GCS a BigQuery.
    Esto es MUCHO más rápido que cargar desde DataFrame en memoria.
    
    Args:
        gcs_uri: URI del archivo Parquet en GCS (gs://bucket/file.parquet)
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla en BigQuery
        write_disposition: "WRITE_TRUNCATE" o "WRITE_APPEND"
    
    Returns:
        Número de filas cargadas
    """
    client = get_bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    if DEBUG:
        print(f"[INFO] Cargando Parquet desde GCS a BigQuery")
        print(f"[INFO] Origen: {gcs_uri}")
        print(f"[INFO] Destino: {table_fqn}")
        print(f"[INFO] Modo: {write_disposition}")
    
    # Configuración del job - BigQuery puede inferir el esquema desde Parquet
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        # BigQuery puede inferir el esquema desde Parquet automáticamente
        autodetect=True,
    )
    
    # Cargar desde GCS (MUCHO más rápido que desde DataFrame)
    job = client.load_table_from_uri(gcs_uri, table_fqn, job_config=job_config)
    job.result()  # Esperar a que termine
    
    # Obtener número de filas cargadas
    table = client.get_table(table_fqn)
    rows_loaded = table.num_rows
    
    if DEBUG:
        print(f"[OK] Parquet cargado: {rows_loaded} filas en {table_fqn}")
    
    return rows_loaded

def load_excel_sheet_to_bq_fast(
    local_path: str,
    dataset_id: str,
    table_name: str,
    sheet_name: str,
    bucket_name: str,
    gcs_temp_folder: str,
    write_disposition: str = "WRITE_APPEND"
):
    """
    Carga una hoja del Excel a BigQuery usando el método rápido:
    1. Convierte Excel a Parquet
    2. Sube Parquet a GCS
    3. Carga desde GCS a BigQuery
    
    Esto es MUCHO más rápido que cargar desde DataFrame en memoria.
    
    Args:
        local_path: Ruta local del archivo Excel
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla en BigQuery
        sheet_name: Nombre de la hoja a leer
        bucket_name: Nombre del bucket en GCS para archivos temporales
        gcs_temp_folder: Carpeta temporal en GCS
        write_disposition: "WRITE_TRUNCATE" para la primera hoja, "WRITE_APPEND" para las siguientes
    
    Returns:
        Número de filas cargadas
    """
    if DEBUG:
        print(f"\n[INFO] Procesando hoja (método rápido): {sheet_name}")
        print(f"[INFO] Tabla destino: {PROJECT_ID}.{dataset_id}.{table_name}")
        print(f"[INFO] Modo: {write_disposition}")
    
    # Paso 1: Convertir Excel a Parquet y subir a GCS
    parquet_uri = convert_excel_sheet_to_parquet(
        local_path=local_path,
        sheet_name=sheet_name,
        bucket_name=bucket_name,
        gcs_folder=gcs_temp_folder
    )
    
    try:
        # Paso 2: Cargar Parquet desde GCS a BigQuery
        rows_loaded = load_parquet_from_gcs_to_bq(
            gcs_uri=parquet_uri,
            dataset_id=dataset_id,
            table_name=table_name,
            write_disposition=write_disposition
        )
        
        return rows_loaded
        
    finally:
        # Limpiar archivo Parquet temporal de GCS
        try:
            gcs_client = get_gcs_client()
            bucket = gcs_client.bucket(bucket_name)
            blob_name = parquet_uri.replace(f"gs://{bucket_name}/", "")
            blob = bucket.blob(blob_name)
            blob.delete()
            if DEBUG:
                print(f"[DEBUG] Archivo Parquet temporal eliminado de GCS: {parquet_uri}")
        except Exception as e:
            print(f"[WARN] No se pudo eliminar archivo Parquet temporal {parquet_uri}: {e}")

def load_excel_sheet_to_bq(
    local_path: str,
    dataset_id: str,
    table_name: str,
    sheet_name: str,
    write_disposition: str = "WRITE_APPEND"
):
    """
    Carga una hoja específica del Excel a BigQuery (método original, más lento).
    Mantenido para compatibilidad, pero se recomienda usar load_excel_sheet_to_bq_fast.
    """
    client = get_bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    if DEBUG:
        print(f"\n[INFO] Procesando hoja: {sheet_name}")
        print(f"[INFO] Tabla destino: {table_fqn}")
        print(f"[INFO] Modo: {write_disposition}")
    
    # Leer solo las primeras filas para obtener los encabezados
    df_header = pd.read_excel(local_path, sheet_name=sheet_name, nrows=2)
    
    # La primera fila puede ser metadatos, usar la segunda como encabezados si existe
    if len(df_header) >= 2:
        headers = df_header.iloc[1].astype(str).tolist()
        skip_rows = 2  # Saltar las primeras 2 filas
    else:
        headers = df_header.iloc[0].astype(str).tolist()
        skip_rows = 1  # Saltar solo la primera fila
    
    # Normalizar nombres de columnas
    headers = [_normalize_column_name(h) for h in headers]
    
    if DEBUG:
        print(f"[INFO] Encabezados detectados: {len(headers)} columnas")
        print(f"[INFO] Primeras 10 columnas: {headers[:10]}")
        print(f"[INFO] Saltando {skip_rows} fila(s) inicial(es)")
        print(f"[INFO] Leyendo hoja completa...")
    
    # Intentar leer todo el archivo
    try:
        df = pd.read_excel(local_path, sheet_name=sheet_name, skiprows=skip_rows, header=None)
        
        # Asignar encabezados
        if len(df.columns) == len(headers):
            df.columns = headers
        else:
            # Ajustar número de columnas
            if len(df.columns) < len(headers):
                for i in range(len(df.columns), len(headers)):
                    df[headers[i]] = None
            else:
                df = df.iloc[:, :len(headers)]
            df.columns = headers
        
        if DEBUG:
            print(f"[INFO] Hoja leída: {len(df)} filas, {len(df.columns)} columnas")
        
    except MemoryError:
        raise Exception(
            f"La hoja '{sheet_name}' es demasiado grande para leer en memoria. "
            f"Tamaño del archivo: {os.path.getsize(local_path) / (1024*1024):.2f} MB. "
            f"Considera aumentar la memoria disponible."
        )
    
    # Convertir todas las columnas a string (preservar datos originales en bronze)
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Agregar timestamp y nombre de hoja
    df["fecha_lectura"] = datetime.now(timezone.utc)
    df["nombre_hoja"] = sheet_name
    
    # Crear esquema
    schema = []
    for col in headers:
        schema.append(bigquery.SchemaField(col, "STRING"))
    schema.append(bigquery.SchemaField("fecha_lectura", "TIMESTAMP"))
    schema.append(bigquery.SchemaField("nombre_hoja", "STRING"))
    
    # Configuración del job
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
    )
    
    # Cargar a BigQuery
    if DEBUG:
        print(f"[INFO] Cargando {len(df)} filas a BigQuery...")
    
    job = client.load_table_from_dataframe(df, table_fqn, job_config=job_config)
    job.result()  # Esperar a que termine
    
    if DEBUG:
        print(f"[OK] Hoja '{sheet_name}' cargada: {len(df)} filas en {table_fqn}")
    
    return len(df)

def load_excel_to_bq(
    local_path: str,
    dataset_id: str,
    table_name: str,
    sheet_name: Optional[str] = None,
    use_fast_method: bool = True,
    bucket_name: Optional[str] = None,
    gcs_temp_folder: Optional[str] = None
):
    """
    Carga un archivo Excel a BigQuery.
    Si sheet_name es None, procesa TODAS las hojas del Excel.
    
    Por defecto usa el método rápido (convertir a Parquet y cargar desde GCS),
    que es MUCHO más rápido para archivos grandes.
    
    Args:
        local_path: Ruta local del archivo Excel
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla en BigQuery
        sheet_name: Nombre de la hoja a leer (si es None, procesa TODAS las hojas)
        use_fast_method: Si True, usa el método rápido (Parquet desde GCS). Si False, usa método directo.
        bucket_name: Nombre del bucket para archivos temporales (requerido si use_fast_method=True)
        gcs_temp_folder: Carpeta temporal en GCS (requerido si use_fast_method=True)
    """
    if DEBUG:
        print(f"[INFO] Cargando Excel a BigQuery")
        print(f"[INFO] Archivo: {local_path}")
        print(f"[INFO] Tamaño del archivo: {os.path.getsize(local_path) / (1024*1024):.2f} MB")
        print(f"[INFO] Método: {'RÁPIDO (Parquet desde GCS)' if use_fast_method else 'DIRECTO (DataFrame en memoria)'}")
    
    # Leer el Excel para obtener información básica
    excel_file = pd.ExcelFile(local_path)
    
    if DEBUG:
        print(f"[INFO] Hojas disponibles: {excel_file.sheet_names}")
        print(f"[INFO] Total de hojas: {len(excel_file.sheet_names)}")
    
    # Si no se especifica hoja, procesar todas
    if sheet_name is None:
        sheets_to_process = excel_file.sheet_names
        if DEBUG:
            print(f"[INFO] Procesando TODAS las hojas: {sheets_to_process}")
    else:
        if sheet_name not in excel_file.sheet_names:
            raise ValueError(f"La hoja '{sheet_name}' no existe en el Excel. Hojas disponibles: {excel_file.sheet_names}")
        sheets_to_process = [sheet_name]
        if DEBUG:
            print(f"[INFO] Procesando hoja específica: {sheet_name}")
    
    # Validar parámetros para método rápido
    if use_fast_method:
        if bucket_name is None:
            bucket_name = DEFAULT_BUCKET_NAME
        if gcs_temp_folder is None:
            gcs_temp_folder = f"data_staging/dpt_planeacion_municipal/temp_parquet"
    
    total_rows = 0
    
    # Procesar cada hoja
    for idx, sheet in enumerate(sheets_to_process):
        if DEBUG:
            print(f"\n{'='*60}")
            print(f"[INFO] Procesando hoja {idx + 1} de {len(sheets_to_process)}: {sheet}")
            print(f"{'='*60}")
        
        # Primera hoja: truncate, siguientes: append
        write_mode = "WRITE_TRUNCATE" if idx == 0 else "WRITE_APPEND"
        
        if use_fast_method:
            rows = load_excel_sheet_to_bq_fast(
                local_path=local_path,
                dataset_id=dataset_id,
                table_name=table_name,
                sheet_name=sheet,
                bucket_name=bucket_name,
                gcs_temp_folder=gcs_temp_folder,
                write_disposition=write_mode
            )
        else:
            rows = load_excel_sheet_to_bq(
                local_path=local_path,
                dataset_id=dataset_id,
                table_name=table_name,
                sheet_name=sheet,
                write_disposition=write_mode
            )
        
        total_rows += rows
        
        if DEBUG:
            print(f"[OK] Hoja {idx + 1}/{len(sheets_to_process)} completada: {rows} filas")
    
    if DEBUG:
        print(f"\n{'='*60}")
        print(f"[OK] Carga completada: {total_rows} filas totales de {len(sheets_to_process)} hoja(s)")
        print(f"{'='*60}")
    
    return total_rows

def cleanup_temp_paths(paths):
    """Elimina los archivos temporales indicados."""
    for path in paths:
        if not path:
            continue
        try:
            os.unlink(path)
            if DEBUG:
                print(f"[DEBUG] Archivo temporal eliminado: {path}")
        except Exception as exc:
            print(f"[WARN] No se pudo eliminar {path}: {exc}")

