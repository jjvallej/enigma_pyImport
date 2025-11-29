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
from typing import Optional, List, Tuple, Dict, Any, Iterable
from modules.config import PROJECT_ID, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE, CONF
from modules.gcp_utils import get_bq_client, get_gcs_client

# === CONFIGURACIÓN ===
DEBUG = CONF.global_config.debug
GCS_BASE_FOLDER = "data_staging/dpt_planeacion_municipal/api_evaplan"

# Mapeo de fuentes a carpetas en GCS
FUENTE_FOLDERS = {
    "periodos": "periodos",
    "avance_mr": "avance_mr",
    "avance_mp": "avance_mp",
    "avance_x_subprograma": "avance_x_subprograma",
    "avance_general": "avance_general"
}

# Mapeo de fuentes a carpetas en GCS (y prefijos de tablas)
SOURCES = {
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
    return get_bq_client()

def _gcs_client() -> storage.Client:
    return get_gcs_client()

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
    - avances: {fuente}_YYYYMMDD_peri_idp_{peri_idp}.json (ej: avance_mr_20251127_peri_idp_123.json)
    
    Args:
        filename: Nombre del archivo (ej: "avance_mr_20251127_peri_idp_123.json")
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

def extract_peri_idp_from_filename(filename: str) -> Optional[int]:
    """
    Extrae el peri_idp del nombre del archivo JSON.
    
    Los archivos tienen formato:
    - avances: {fuente}_YYYYMMDD_peri_idp_{peri_idp}.json (ej: avance_mr_20251127_peri_idp_123.json)
    - periodos: periodo_YYYYMMDD.json (no tiene peri_idp)
    
    Args:
        filename: Nombre del archivo (ej: "avance_mr_20251127_peri_idp_123.json")
    
    Returns:
        peri_idp como entero o None si no se puede extraer
    """
    # Buscar patrón peri_idp_{numero} en el nombre del archivo
    pattern = r'peri_idp_(\d+)'
    match = re.search(pattern, filename)
    
    if match:
        try:
            peri_idp = int(match.group(1))
            return peri_idp
        except ValueError:
            if DEBUG:
                print(f"[WARN] peri_idp extraído no válido en archivo {filename}")
            return None
    
    if DEBUG:
        print(f"[DEBUG] No se encontró peri_idp en el archivo: {filename} (puede ser normal para periodos)")
    return None

def get_json_files_from_current_date(bucket_name: str, folder_path: str) -> List[Tuple[str, str, Optional[int]]]:
    """
    Obtiene todos los archivos JSON de la fecha actual en una carpeta de GCS.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta (ej: 'data_staging/dpt_planeacion_municipal/api_evaplan/avance_mr')
    
    Returns:
        Lista de tuplas (gcs_uri, fecha_extraida, peri_idp) para archivos de la fecha actual
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
    
    # Obtener fecha actual en formato YYYYMMDD
    fecha_actual = datetime.now().strftime("%Y%m%d")
    
    # Listar todos los blobs en la carpeta que terminen en .json
    blobs = list(bucket.list_blobs(prefix=folder_path))
    
    # Filtrar solo archivos .json de la fecha actual
    json_files = []
    for blob in blobs:
        if not blob.name.lower().endswith('.json') or blob.name.endswith('/'):
            continue
        
        filename = os.path.basename(blob.name)
        fecha = extract_date_from_filename(filename, "")
        
        # Solo incluir archivos de la fecha actual
        if fecha == fecha_actual:
            peri_idp = extract_peri_idp_from_filename(filename)
            gcs_uri = f"gs://{bucket_name}/{blob.name}"
            json_files.append((gcs_uri, fecha, peri_idp))
    
    if DEBUG:
        print(f"[INFO] Archivos JSON de la fecha actual ({fecha_actual}) encontrados: {len(json_files)}")
        for uri, fecha, peri_idp in json_files[:5]:  # Mostrar los primeros 5
            filename = os.path.basename(uri)
            print(f"[DEBUG]   - {filename} (fecha: {fecha}, peri_idp: {peri_idp})")
    
    return json_files

def get_all_json_files_from_gcs_folder(bucket_name: str, folder_path: str) -> List[Tuple[str, str]]:
    """
    Obtiene todos los archivos JSON de una carpeta en GCS, ordenados por fecha (más reciente primero).
    DEPRECATED: Usar get_json_files_from_current_date para obtener solo archivos de la fecha actual.
    
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

def extract_data_from_json(json_data: Dict[str, Any], fuente: str, peri_idp: Optional[int] = None) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """
    Extrae los datos relevantes del JSON según la fuente y obtiene el peri_idp.
    
    Args:
        json_data: Diccionario con el JSON completo de la respuesta de la API
        fuente: Nombre de la fuente (periodos, avance_mr, avance_mp, avance_x_subprograma, avance_general)
        peri_idp: peri_idp del nombre del archivo (opcional, se intentará obtener del JSON si no se proporciona)
    
    Returns:
        Tupla (lista de diccionarios con los datos a cargar, peri_idp)
    """
    if not json_data.get("success"):
        raise ValueError(f"La respuesta JSON indica un error: {json_data.get('message', 'Error desconocido')}")
    
    # Obtener peri_idp del JSON si no se proporcionó
    if peri_idp is None:
        peri_idp = json_data.get("peri_idp")
        if peri_idp:
            try:
                peri_idp = int(peri_idp)
            except (ValueError, TypeError):
                peri_idp = None
    
    data = json_data.get("data", {})
    
    if fuente == "periodos":
        periodos = data.get("periodos", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(periodos)} periodos del JSON")
        return periodos, peri_idp
    elif fuente == "avance_mr":
        avance_mr = data.get("AvanceMR", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_mr)} registros de AvanceMR del JSON (peri_idp: {peri_idp})")
        return avance_mr, peri_idp
    elif fuente == "avance_mp":
        avance_mp = data.get("AvanceMP", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_mp)} registros de AvanceMP del JSON (peri_idp: {peri_idp})")
        return avance_mp, peri_idp
    elif fuente == "avance_x_subprograma":
        avance_x_subprograma = data.get("AvanceXSubprograma", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_x_subprograma)} registros de AvanceXSubprograma del JSON (peri_idp: {peri_idp})")
        return avance_x_subprograma, peri_idp
    elif fuente == "avance_general":
        avance_general = data.get("AvanceGeneral", [])
        if DEBUG:
            print(f"[DEBUG] Extrayendo {len(avance_general)} registros de AvanceGeneral del JSON (peri_idp: {peri_idp})")
        return avance_general, peri_idp
    else:
        raise ValueError(f"Fuente no reconocida: {fuente}")

def normalize_json_records(records: List[Dict[str, Any]], fecha_lectura: datetime, peri_idp: Optional[int] = None) -> pd.DataFrame:
    """
    Normaliza una lista de diccionarios JSON a un DataFrame de pandas.
    Maneja estructuras anidadas expandiéndolas y agrega fecha_lectura y peri_idp.
    
    Args:
        records: Lista de diccionarios con los registros
        fecha_lectura: Fecha de lectura para agregar a todos los registros
        peri_idp: ID del periodo para agregar a todos los registros (opcional, si ya existe en los datos se usa ese)
    
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
    
    # Manejar peri_idp ANTES de convertir todo a STRING
    # Si peri_idp ya existe en los datos, convertirlo a INT64
    # Si no existe pero se proporciona, agregarlo
    if "peri_idp" in df.columns:
        # peri_idp ya existe en los datos (como en periodos)
        # Convertirlo a INT64 directamente
        try:
            df["peri_idp"] = pd.to_numeric(df["peri_idp"], errors='coerce').astype('Int64')
            if DEBUG:
                print(f"[DEBUG] peri_idp encontrado en los datos, convertido a INT64")
        except Exception as e:
            if DEBUG:
                print(f"[WARN] Error al convertir peri_idp a INT64: {e}")
            # Si falla, intentar convertir a string primero y luego a int
            df["peri_idp"] = pd.to_numeric(df["peri_idp"].astype(str).str.replace(r'[^0-9]', '', regex=True), errors='coerce').astype('Int64')
    elif peri_idp is not None:
        # peri_idp no existe en los datos pero se proporciona externamente
        df["peri_idp"] = peri_idp
        df["peri_idp"] = df["peri_idp"].astype('Int64')
        if DEBUG:
            print(f"[DEBUG] peri_idp agregado externamente: {peri_idp}")
    
    # Convertir todas las columnas a STRING para preservar valores originales en bronze
    # EXCEPTO peri_idp y fecha_lectura que tienen tipos especiales
    for col in df.columns:
        if col == "peri_idp":
            # Ya está como INT64, no convertir a STRING
            # Asegurar que sigue siendo INT64
            if df[col].dtype != 'Int64':
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            continue
        elif col == "fecha_lectura":
            # No convertir fecha_lectura a STRING
            continue
        else:
            # Convertir a STRING
            df[col] = df[col].astype(str).replace(['nan', 'None', '<NA>'], '')
            # Si hay valores None reales de pandas, convertirlos a cadena vacía
            df[col] = df[col].fillna('')
    
    # Agregar fecha_lectura
    df["fecha_lectura"] = fecha_lectura
    
    # Verificar que peri_idp sigue siendo INT64 antes de retornar
    if "peri_idp" in df.columns:
        if df["peri_idp"].dtype != 'Int64':
            if DEBUG:
                print(f"[WARN] peri_idp no es INT64 antes de retornar, convirtiendo...")
            df["peri_idp"] = pd.to_numeric(df["peri_idp"], errors='coerce').astype('Int64')
    
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

def delete_records_by_date_and_peri_idp(dataset_id: str, table_name: str, fecha_lectura: datetime, peri_idp: Optional[int] = None):
    """
    Elimina registros de una tabla que tengan la misma fecha de lectura (mismo día) y mismo peri_idp.
    
    Args:
        dataset_id: ID del dataset
        table_name: Nombre de la tabla
        fecha_lectura: Fecha de lectura (solo se compara el día, no la hora)
        peri_idp: ID del periodo (opcional, si se proporciona se incluye en la condición)
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
    
    # Construir query según si hay peri_idp o no
    if peri_idp is not None:
        # Eliminar registros del mismo día Y mismo peri_idp
        query = f"""
        DELETE FROM `{table_fqn}`
        WHERE DATE(fecha_lectura) = DATE('{fecha_date}')
          AND CAST(peri_idp AS INT64) = {peri_idp}
        """
        if DEBUG:
            print(f"[INFO] Eliminando registros del día {fecha_date} y peri_idp {peri_idp} de la tabla {table_name}")
    else:
        # Eliminar registros del mismo día (para periodos que no tienen peri_idp)
        query = f"""
        DELETE FROM `{table_fqn}`
        WHERE DATE(fecha_lectura) = DATE('{fecha_date}')
        """
        if DEBUG:
            print(f"[INFO] Eliminando registros del día {fecha_date} de la tabla {table_name}")
    
    job = client.query(query)
    job.result()
    
    if DEBUG:
        print(f"[OK] Registros eliminados exitosamente")

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
        elif col == "peri_idp":
            schema.append(bigquery.SchemaField(col, "INT64"))
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
    
    # Asegurar que peri_idp sea INT64 antes de cargar (última verificación)
    if "peri_idp" in df.columns:
        if df["peri_idp"].dtype != 'Int64':
            if DEBUG:
                print(f"[WARN] peri_idp no es Int64 antes de cargar (tipo: {df['peri_idp'].dtype}), convirtiendo...")
            try:
                df["peri_idp"] = pd.to_numeric(df["peri_idp"], errors='coerce').astype('Int64')
            except Exception as e:
                if DEBUG:
                    print(f"[ERROR] No se pudo convertir peri_idp a INT64: {e}")
                raise ValueError(f"No se pudo convertir peri_idp a INT64: {e}")
    
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
    Función principal que carga JSON de la fecha actual a BigQuery.
    
    Nueva lógica:
    1. Busca solo archivos JSON de la fecha actual en la carpeta de la fuente
    2. Lee todos los JSON de la fecha actual
    3. Extrae peri_idp de cada JSON (del nivel raíz o del nombre del archivo)
    4. Une todos los registros de todos los JSON
    5. Agrega fecha_lectura (fecha de carga) y peri_idp a cada registro
    6. Si la tabla existe y hay registros del mismo día y mismo peri_idp, los elimina antes de cargar
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
        bucket_name: Nombre del bucket en GCS
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla
    """
    folder_path = get_fuente_folder_path(fuente)
    
    # Obtener todos los JSON de la fecha actual
    json_files = get_json_files_from_current_date(bucket_name, folder_path)
    
    if not json_files:
        print(f"[WARN] No se encontraron archivos JSON de la fecha actual para la fuente {fuente}")
        return
    
    if DEBUG:
        print(f"[INFO] Procesando {len(json_files)} archivo(s) JSON de la fecha actual para {fuente}")
    
    # Verificar si la tabla existe
    table_exist = table_exists(dataset_id, table_name)
    
    # Obtener fecha actual para fecha_lectura
    fecha_actual = datetime.now(timezone.utc)
    fecha_actual_date = fecha_actual.date().isoformat()
    
    all_dfs = []
    temp_files = []
    peri_idps_to_delete = set()  # Para rastrear qué peri_idp eliminar
    
    try:
        # Procesar todos los JSON de la fecha actual
        for gcs_uri, fecha_str, peri_idp_from_filename in json_files:
            # Descargar JSON
            local_json_path = download_json_from_gcs(gcs_uri)
            temp_files.append(local_json_path)
            
            # Leer JSON
            with open(local_json_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            
            # Extraer datos y peri_idp (del JSON o del nombre del archivo)
            records, peri_idp = extract_data_from_json(json_data, fuente, peri_idp_from_filename)
            
            if not records:
                if DEBUG:
                    print(f"[WARN] No se encontraron registros en el JSON: {os.path.basename(gcs_uri)}")
                continue
            
            if peri_idp is None and fuente != "periodos":
                if DEBUG:
                    print(f"[WARN] No se encontró peri_idp en el JSON ni en el nombre del archivo: {os.path.basename(gcs_uri)}")
                continue
            
            # Agregar peri_idp a la lista de los que hay que eliminar si la tabla existe
            if table_exist and peri_idp is not None:
                peri_idps_to_delete.add(peri_idp)
            
            # Normalizar y crear DataFrame con fecha_lectura y peri_idp
            df = normalize_json_records(records, fecha_actual, peri_idp)
            all_dfs.append(df)
            
            if DEBUG:
                print(f"[DEBUG] Procesados {len(records)} registros del archivo {os.path.basename(gcs_uri)} (peri_idp: {peri_idp})")
        
        # Si no hay DataFrames, no hay nada que cargar
        if not all_dfs:
            print(f"[WARN] No se encontraron registros en ningún JSON para {fuente}")
            return
        
        # Concatenar todos los DataFrames
        # Usar join='outer' para manejar columnas diferentes y asegurar que peri_idp se preserve
        combined_df = pd.concat(all_dfs, ignore_index=True, sort=False)
        
        # Asegurar que peri_idp sea INT64 antes de cargar
        # Esto es crítico porque pandas puede convertir tipos al concatenar
        if "peri_idp" in combined_df.columns:
            # Verificar el tipo actual
            current_dtype = str(combined_df["peri_idp"].dtype)
            if DEBUG:
                print(f"[DEBUG] Tipo actual de peri_idp después de concatenar: {current_dtype}")
            
            # Convertir peri_idp a INT64 siempre, sin importar el tipo actual
            try:
                # Primero convertir a numérico, luego a Int64 (nullable integer)
                combined_df["peri_idp"] = pd.to_numeric(combined_df["peri_idp"], errors='coerce').astype('Int64')
                if DEBUG:
                    print(f"[DEBUG] peri_idp convertido a INT64 antes de cargar")
            except Exception as e:
                if DEBUG:
                    print(f"[WARN] Error al convertir peri_idp a INT64: {e}")
                # Intentar convertir desde string eliminando caracteres no numéricos
                combined_df["peri_idp"] = pd.to_numeric(
                    combined_df["peri_idp"].astype(str).str.replace(r'[^0-9]', '', regex=True), 
                    errors='coerce'
                ).astype('Int64')
            
            # Verificar que la conversión fue exitosa
            if DEBUG:
                final_dtype = str(combined_df["peri_idp"].dtype)
                print(f"[DEBUG] Tipo final de peri_idp: {final_dtype}")
                if final_dtype != 'Int64':
                    print(f"[WARN] peri_idp NO es Int64 después de la conversión!")
        
        if DEBUG:
            print(f"[INFO] Total de registros a cargar: {len(combined_df)}")
            if "peri_idp" in combined_df.columns:
                peri_idps_unique = combined_df["peri_idp"].unique()
                print(f"[DEBUG] peri_idp únicos: {list(peri_idps_unique)}")
                print(f"[DEBUG] Tipo de dato de peri_idp: {combined_df['peri_idp'].dtype}")
        
        # Si la tabla existe, eliminar registros del mismo día y mismo peri_idp
        if table_exist:
            # Eliminar registros por cada peri_idp encontrado
            for peri_idp_to_delete in peri_idps_to_delete:
                delete_records_by_date_and_peri_idp(
                    dataset_id=dataset_id,
                    table_name=table_name,
                    fecha_lectura=fecha_actual,
                    peri_idp=peri_idp_to_delete
                )
            
            # Para periodos (que no tienen peri_idp), eliminar todos los del mismo día
            if fuente == "periodos":
                delete_records_by_date_and_peri_idp(
                    dataset_id=dataset_id,
                    table_name=table_name,
                    fecha_lectura=fecha_actual,
                    peri_idp=None
                )
            
            write_mode = "WRITE_APPEND"
        else:
            write_mode = "WRITE_TRUNCATE"
        
        # Cargar a BigQuery
        load_dataframe_to_bq(
            df=combined_df,
            dataset_id=dataset_id,
            table_name=table_name,
            write_mode=write_mode
        )
        
        if DEBUG:
            print(f"[OK] Carga completada para {fuente}: {len(combined_df)} registros cargados")
    
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

