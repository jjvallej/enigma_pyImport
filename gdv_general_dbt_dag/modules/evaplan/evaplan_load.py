# modules/evaplan_load.py
"""
Módulo para extraer datos JSON desde Google Cloud Storage y cargarlos
en la capa bronze de BigQuery para las fuentes de Evaplan.

Maneja múltiples JSON por carpeta con fechas diferentes y lógica de carga inteligente.
"""
from google.cloud import bigquery, storage
import pandas as pd
import os, tempfile, json, re
from datetime import datetime, timezone
from typing import Iterable, Optional, Dict, Any, List, Tuple

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True

# Valores por defecto
GCS_BUCKET_NAME = "datalake_gdv_dev"
GCS_BASE_FOLDER = "data_staging/dpt_planeacion_municipal/api_evaplan"

# Mapeo de fuentes a carpetas en GCS
FUENTE_FOLDERS = {
    "periodos": "periodos",
    "avance_mr": "avance_mr",
    "avance_mp": "avance_mp",
    "avance_x_subprograma": "avance_x_subprograma",
    "avance_general": "avance_general"
}

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
        ds.description = "Bronze layer para datos de Evaplan"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")

def extract_date_from_filename(filename: str, fuente: str) -> Optional[str]:
    """
    Extrae la fecha del nombre del archivo JSON.
    
    Los archivos tienen formato:
    - periodos: periodo_YYYYMMDD.json
    - avances: {fuente}_YYYYMMDD.json (ej: avance_mr_20251125.json)
    
    Args:
        filename: Nombre del archivo (ej: "avance_mr_20251125.json")
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
    
    Returns:
        Fecha en formato YYYYMMDD o None si no se puede extraer
    """
    # Buscar patrón de fecha YYYYMMDD en el nombre del archivo
    pattern = r'(\d{8})'
    match = re.search(pattern, filename)
    
    if match:
        fecha_str = match.group(1)
        # Validar que sea una fecha válida
        try:
            datetime.strptime(fecha_str, "%Y%m%d")
            return fecha_str
        except ValueError:
            if DEBUG:
                print(f"[WARN] Fecha extraída no válida: {fecha_str} en archivo {filename}")
            return None
    
    if DEBUG:
        print(f"[WARN] No se pudo extraer fecha del archivo: {filename}")
    return None

def parse_date_string_to_timestamp(fecha_str: str) -> datetime:
    """
    Convierte una fecha en formato YYYYMMDD a datetime con timezone UTC.
    
    Args:
        fecha_str: Fecha en formato YYYYMMDD
    
    Returns:
        datetime con timezone UTC
    """
    fecha = datetime.strptime(fecha_str, "%Y%m%d")
    return fecha.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

def get_all_json_files_from_gcs_folder(bucket_name: str, folder_path: str) -> List[Tuple[str, str]]:
    """
    Obtiene todos los archivos JSON de una carpeta en GCS, ordenados por fecha (más reciente primero).
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta (ej: 'data_staging/dpt_planeacion_municipal/api_evaplan/periodos')
    
    Returns:
        Lista de tuplas (gcs_uri, fecha_extraida) ordenadas por fecha descendente
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
    
    # Listar todos los blobs en la carpeta que terminen en .json
    blobs = list(bucket.list_blobs(prefix=folder_path))
    
    # Filtrar solo archivos .json
    json_files = [blob for blob in blobs if blob.name.lower().endswith('.json') and not blob.name.endswith('/')]
    
    if not json_files:
        if DEBUG:
            print(f"[INFO] No se encontraron archivos .json en la carpeta 'gs://{bucket_name}/{folder_path}'")
        return []
    
    # Extraer fecha de cada archivo y crear lista de tuplas
    files_with_dates = []
    for blob in json_files:
        filename = os.path.basename(blob.name)
        fecha = extract_date_from_filename(filename, "")
        
        if fecha:
            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            files_with_dates.append((gcs_uri, fecha, blob.time_created))
        else:
            if DEBUG:
                print(f"[WARN] No se pudo extraer fecha del archivo {filename}, se omitirá")
    
    # Ordenar por fecha (más reciente primero) y luego por tiempo de creación
    files_with_dates.sort(key=lambda x: (x[1], x[2]), reverse=True)
    
    # Retornar solo (gcs_uri, fecha)
    result = [(uri, fecha) for uri, fecha, _ in files_with_dates]
    
    if DEBUG:
        print(f"[INFO] Archivos JSON encontrados en la carpeta: {len(result)}")
        for uri, fecha in result[:5]:  # Mostrar los primeros 5
            filename = os.path.basename(uri)
            print(f"[DEBUG]   - {filename} (fecha: {fecha})")
    
    return result

def download_json_from_gcs(gcs_uri: str) -> str:
    """
    Descarga el archivo JSON desde GCS hacia un archivo temporal
    y devuelve la ruta local generada.
    """
    gcs = _gcs_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = gcs.bucket(bucket_name).blob(blob_name)

    fd, tmp_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    blob.download_to_filename(tmp_path)
    if DEBUG:
        print(f"[DEBUG] Archivo JSON descargado en {tmp_path}")
    return tmp_path

def extract_data_from_json(json_data: Dict[str, Any], fuente: str) -> List[Dict[str, Any]]:
    """
    Extrae los datos relevantes del JSON según la fuente.
    
    Args:
        json_data: Diccionario con el JSON completo de la respuesta de la API
        fuente: Nombre de la fuente (periodos, avance_mr, avance_mp, avance_x_subprograma, avance_general)
    
    Returns:
        Lista de diccionarios con los datos a cargar
    """
    if not json_data.get("success"):
        raise ValueError(f"La respuesta JSON indica un error: {json_data.get('message', 'Error desconocido')}")
    
    data = json_data.get("data", {})
    
    if fuente == "periodos":
        periodos = data.get("periodos", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(periodos)} periodos del JSON")
        return periodos
    elif fuente == "avance_mr":
        avance_mr = data.get("AvanceMR", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_mr)} registros de AvanceMR del JSON")
        return avance_mr
    elif fuente == "avance_mp":
        avance_mp = data.get("AvanceMP", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_mp)} registros de AvanceMP del JSON")
        return avance_mp
    elif fuente == "avance_x_subprograma":
        avance_x_subprograma = data.get("AvanceXSubprograma", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_x_subprograma)} registros de AvanceXSubprograma del JSON")
        return avance_x_subprograma
    elif fuente == "avance_general":
        avance_general = data.get("AvanceGeneral", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_general)} registros de AvanceGeneral del JSON")
        return avance_general
    else:
        raise ValueError(f"Fuente no reconocida: {fuente}")

def normalize_json_records(records: List[Dict[str, Any]], fecha_lectura: datetime) -> pd.DataFrame:
    """
    Normaliza una lista de diccionarios JSON a un DataFrame de pandas.
    Maneja estructuras anidadas expandiéndolas y agrega fecha_lectura.
    
    Args:
        records: Lista de diccionarios con los registros
        fecha_lectura: Fecha de lectura para agregar a todos los registros
    
    Returns:
        DataFrame de pandas con los registros normalizados
    """
    if not records:
        # Retornar DataFrame vacío
        return pd.DataFrame()
    
    # Normalizar los registros JSON a DataFrame
    df = pd.json_normalize(records)
    
    if DEBUG:
        print(f"[DEBUG] DataFrame normalizado: {df.shape[0]} filas, {df.shape[1]} columnas")
        if df.shape[1] > 0:
            print(f"[DEBUG] Columnas: {list(df.columns)}")
    
    # Convertir todas las columnas a STRING para preservar valores originales en bronze
    # Manejar valores nulos: convertir None/NaN a cadena vacía
    for col in df.columns:
        df[col] = df[col].astype(str).replace(['nan', 'None', '<NA>'], '')
        # Si hay valores None reales de pandas, convertirlos a cadena vacía
        df[col] = df[col].fillna('')
    
    # Agregar fecha_lectura
    df["fecha_lectura"] = fecha_lectura
    
    return df

def table_exists(dataset_id: str, table_name: str) -> bool:
    """
    Verifica si una tabla existe en BigQuery.
    
    Args:
        dataset_id: ID del dataset
        table_name: Nombre de la tabla
    
    Returns:
        True si la tabla existe, False en caso contrario
    """
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    try:
        client.get_table(table_fqn)
        return True
    except Exception:
        return False

def delete_records_by_date(dataset_id: str, table_name: str, fecha_lectura: datetime):
    """
    Elimina registros de una tabla que tengan la misma fecha de lectura (mismo día).
    
    Args:
        dataset_id: ID del dataset
        table_name: Nombre de la tabla
        fecha_lectura: Fecha de lectura (solo se compara el día, no la hora)
    """
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # Verificar que la tabla existe antes de intentar eliminar
    if not table_exists(dataset_id, table_name):
        if DEBUG:
            print(f"[INFO] Tabla {table_name} no existe, no hay registros que eliminar")
        return
    
    # Convertir fecha_lectura a formato DATE para comparación por día
    fecha_date = fecha_lectura.date().isoformat()
    
    # Query para eliminar registros del mismo día
    query = f"""
    DELETE FROM `{table_fqn}`
    WHERE DATE(fecha_lectura) = DATE('{fecha_date}')
    """
    
    if DEBUG:
        print(f"[INFO] Eliminando registros del día {fecha_date} de la tabla {table_name}")
    
    job = client.query(query)
    job.result()
    
    if DEBUG:
        print(f"[OK] Registros del día {fecha_date} eliminados exitosamente")

def get_table_schema_from_dataframe(df: pd.DataFrame) -> List[bigquery.SchemaField]:
    """
    Genera el esquema de BigQuery a partir de un DataFrame.
    
    Args:
        df: DataFrame de pandas
    
    Returns:
        Lista de SchemaField para BigQuery
    """
    schema = []
    for col in df.columns:
        if col == "fecha_lectura":
            schema.append(bigquery.SchemaField(col, "TIMESTAMP"))
        else:
            schema.append(bigquery.SchemaField(col, "STRING"))
    return schema

def load_dataframe_to_bq(
    df: pd.DataFrame,
    dataset_id: str,
    table_name: str,
    write_mode: str = "WRITE_APPEND"
):
    """
    Carga un DataFrame a BigQuery.
    
    Args:
        df: DataFrame de pandas
        dataset_id: ID del dataset
        table_name: Nombre de la tabla
        write_mode: Modo de escritura ("WRITE_APPEND" o "WRITE_TRUNCATE")
    """
    if df.empty:
        if DEBUG:
            print(f"[WARN] DataFrame vacío, no se cargarán datos")
        return
    
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # Obtener esquema
    schema = get_table_schema_from_dataframe(df)
    
    # Configurar job
    write_disposition = bigquery.WriteDisposition.WRITE_APPEND if write_mode == "WRITE_APPEND" else bigquery.WriteDisposition.WRITE_TRUNCATE
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
    )
    
    # Cargar datos
    job = client.load_table_from_dataframe(df, table_fqn, job_config=job_config)
    job.result()
    
    if DEBUG:
        print(f"[OK] Cargadas {len(df)} filas en {table_fqn} (modo: {write_mode})")

def load_json_files_to_bq(
    fuente: str,
    bucket_name: str,
    dataset_id: str,
    table_name: str
):
    """
    Función principal que maneja la lógica de carga de JSON a BigQuery según las condiciones.
    
    Lógica:
    1. Si solo hay 1 JSON: crear la tabla basada en ese JSON
    2. Si hay múltiples JSON y la tabla existe: cargar solo el más reciente, reemplazar registros del mismo día
    3. Si hay múltiples JSON y la tabla NO existe: cargar todos los JSON con sus fechas
    4. Si la tabla existe y hay registros del mismo día: reemplazarlos
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
        bucket_name: Nombre del bucket en GCS
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla
    """
    folder_path = get_fuente_folder_path(fuente)
    
    # Obtener todos los JSON de la carpeta
    json_files = get_all_json_files_from_gcs_folder(bucket_name, folder_path)
    
    if not json_files:
        print(f"[WARN] No se encontraron archivos JSON para la fuente {fuente}")
        return
    
    # Verificar si la tabla existe
    table_exist = table_exists(dataset_id, table_name)
    
    # Condición 1: Solo hay 1 JSON
    if len(json_files) == 1:
        gcs_uri, fecha_str = json_files[0]
        
        if DEBUG:
            print(f"[INFO] Solo hay 1 JSON para {fuente}. Creando/cargando tabla basada en ese JSON.")
        
        # Descargar JSON
        local_json_path = download_json_from_gcs(gcs_uri)
        
        try:
            # Leer JSON
            with open(local_json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Extraer datos
            records = extract_data_from_json(json_data, fuente)
            
            if records:
                # Normalizar y crear DataFrame
                fecha_lectura = parse_date_string_to_timestamp(fecha_str)
                df = normalize_json_records(records, fecha_lectura)
                
                # Si la tabla existe, eliminar registros del mismo día antes de cargar
                if table_exist:
                    delete_records_by_date(dataset_id, table_name, fecha_lectura)
                    write_mode = "WRITE_APPEND"
                else:
                    write_mode = "WRITE_TRUNCATE"
                
                # Cargar a BigQuery
                load_dataframe_to_bq(
                    df=df,
                    dataset_id=dataset_id,
                    table_name=table_name,
                    write_mode=write_mode
                )
            else:
                print(f"[WARN] No se encontraron registros en el JSON para {fuente}")
        
        finally:
            # Limpiar archivo temporal
            cleanup_temp_paths([local_json_path])
    
    # Condición 2 y 3: Hay múltiples JSON
    else:
        if table_exist:
            # Condición 2: Tabla existe, cargar solo el más reciente y reemplazar registros del mismo día
            if DEBUG:
                print(f"[INFO] Tabla {table_name} existe y hay {len(json_files)} JSON. Cargando solo el más reciente.")
            
            # Tomar solo el más reciente (primero de la lista que está ordenada descendentemente)
            gcs_uri, fecha_str = json_files[0]
            
            # Descargar JSON
            local_json_path = download_json_from_gcs(gcs_uri)
            
            try:
                # Leer JSON
                with open(local_json_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                
                # Extraer datos
                records = extract_data_from_json(json_data, fuente)
                
                if records:
                    # Normalizar y crear DataFrame
                    fecha_lectura = parse_date_string_to_timestamp(fecha_str)
                    df = normalize_json_records(records, fecha_lectura)
                    
                    # Eliminar registros del mismo día
                    delete_records_by_date(dataset_id, table_name, fecha_lectura)
                    
                    # Cargar nuevos registros
                    load_dataframe_to_bq(
                        df=df,
                        dataset_id=dataset_id,
                        table_name=table_name,
                        write_mode="WRITE_APPEND"
                    )
                else:
                    print(f"[WARN] No se encontraron registros en el JSON más reciente para {fuente}")
            
            finally:
                # Limpiar archivo temporal
                cleanup_temp_paths([local_json_path])
        
        else:
            # Condición 3: Tabla NO existe, cargar todos los JSON
            if DEBUG:
                print(f"[INFO] Tabla {table_name} NO existe y hay {len(json_files)} JSON. Cargando todos los JSON.")
            
            all_dfs = []
            temp_files = []
            
            try:
                # Procesar todos los JSON
                for gcs_uri, fecha_str in json_files:
                    # Descargar JSON
                    local_json_path = download_json_from_gcs(gcs_uri)
                    temp_files.append(local_json_path)
                    
                    # Leer JSON
                    with open(local_json_path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    # Extraer datos
                    records = extract_data_from_json(json_data, fuente)
                    
                    if records:
                        # Normalizar y crear DataFrame con fecha correspondiente
                        fecha_lectura = parse_date_string_to_timestamp(fecha_str)
                        df = normalize_json_records(records, fecha_lectura)
                        all_dfs.append(df)
                
                # Concatenar todos los DataFrames
                if all_dfs:
                    combined_df = pd.concat(all_dfs, ignore_index=True)
                    
                    if DEBUG:
                        print(f"[INFO] Total de registros a cargar: {len(combined_df)}")
                        print(f"[DEBUG] Rango de fechas: {combined_df['fecha_lectura'].min()} a {combined_df['fecha_lectura'].max()}")
                    
                    # Cargar a BigQuery (WRITE_TRUNCATE porque es la primera carga)
                    load_dataframe_to_bq(
                        df=combined_df,
                        dataset_id=dataset_id,
                        table_name=table_name,
                        write_mode="WRITE_TRUNCATE"
                    )
                else:
                    print(f"[WARN] No se encontraron registros en ningún JSON para {fuente}")
            
            finally:
                # Limpiar archivos temporales
                cleanup_temp_paths(temp_files)

def get_fuente_folder_path(fuente: str) -> str:
    """
    Obtiene la ruta completa de la carpeta en GCS para una fuente.
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
    
    Returns:
        Ruta completa de la carpeta en GCS
    """
    folder_name = FUENTE_FOLDERS.get(fuente)
    if not folder_name:
        raise ValueError(f"Fuente no reconocida: {fuente}. Fuentes válidas: {list(FUENTE_FOLDERS.keys())}")
    
    return f"{GCS_BASE_FOLDER}/{folder_name}"

def get_table_name_for_fuente(fuente: str) -> str:
    """
    Obtiene el nombre de la tabla en BigQuery para una fuente.
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
    
    Returns:
        Nombre de la tabla (evaplan_api_{fuente}_raw_data)
    """
    return f"evaplan_api_{fuente}_raw_data"

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

