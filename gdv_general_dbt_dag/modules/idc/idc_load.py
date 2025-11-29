# modules/idc_load.py
"""
Módulo para extraer datos desde Google Cloud Storage, transformarlos mínimamente
y cargarlos en la capa bronze de BigQuery para IDC.
Soporta múltiples hojas del Excel, cada una se carga como una tabla separada.
"""
from google.cloud import bigquery, storage
import pandas as pd
import os, tempfile, re
from datetime import datetime, timezone
from typing import Iterable, Optional, Dict, List
from modules.config import PROJECT_ID, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE
from modules.gcp_utils import get_bq_client, get_gcs_client

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
    SIEMPRE omite la primera hoja (índice 0) ya que es una hoja de estructura/metadatos.
    
    IMPORTANTE: Siempre se omite la primera hoja del archivo Excel.
    Las 3 hojas restantes son: Dato_original, Valor_normalizado, Valor_ranking (en ese orden).
    
    Args:
        local_path: Ruta local del archivo Excel
    
    Returns:
        Lista de nombres de hojas (sin la primera hoja)
    """
    excel_file = pd.ExcelFile(local_path)
    sheet_names = excel_file.sheet_names
    if DEBUG:
        print(f"[DEBUG] Hojas encontradas en el Excel (todas): {sheet_names}")
    
    # SIEMPRE omitir la primera hoja (índice 0) - es una hoja de estructura/metadatos
    if len(sheet_names) > 0:
        sheet_names = sheet_names[1:]  # Omitir la primera hoja (índice 0)
        if DEBUG:
            print(f"[DEBUG] Primera hoja eliminada. Hojas restantes (3 esperadas): {sheet_names}")
            print(f"[DEBUG] Orden esperado: [0] Dato_original, [1] Valor_normalizado, [2] Valor_ranking")
    
    return sheet_names

def _minimal_normalize_for_bq(col_name: str) -> str:
    """
    Normalización MÍNIMA de nombres de columnas solo para que BigQuery los acepte.
    Mantiene los nombres lo más similares posible al original.
    
    BigQuery requiere que los nombres de columnas:
    - No contengan espacios (se reemplazan con guiones bajos)
    - No contengan caracteres especiales problemáticos
    - Tengan máximo 300 caracteres
    
    NOTA: Esta es una normalización mínima. La normalización completa (snake_case, etc.)
    se hace en la capa silver con dbt.
    
    Args:
        col_name: Nombre de columna original
    
    Returns:
        Nombre de columna con normalización mínima para BigQuery
    """
    # Convertir a string si no lo es
    col_name = str(col_name)
    
    # Solo reemplazar espacios con guiones bajos (mínimo necesario para BigQuery)
    col_name = col_name.replace(' ', '_')
    
    # Reemplazar caracteres problemáticos comunes con guiones bajos
    # Mantener letras, números, guiones y guiones bajos
    col_name = re.sub(r'[^\w\-]', '_', col_name)
    
    # Eliminar guiones bajos múltiples consecutivos
    col_name = re.sub(r'_+', '_', col_name)
    
    # Eliminar guiones bajos al inicio y final
    col_name = col_name.strip('_')
    
    # Si está vacío, usar nombre genérico
    if not col_name:
        col_name = 'unnamed_column'
    
    # Limitar a 300 caracteres (límite de BigQuery)
    if len(col_name) > 300:
        col_name = col_name[:300]
    
    return col_name

def transform_excel_sheet(local_path: str, sheet_name: str) -> pd.DataFrame:
    """
    Lee y transforma mínimamente una hoja específica del Excel de IDC.
    Devuelve un DataFrame listo para cargarse a BigQuery en bronze.
    
    IMPORTANTE: 
    - Lee el Excel sin usar encabezados automáticamente
    - SIEMPRE elimina la primera fila (metadatos/estructura) de cada hoja
    - Usa la segunda fila como encabezados (nombres de columnas)
    - Solo hace normalización MÍNIMA de nombres (espacios -> guiones bajos) para que BigQuery los acepte
    - La normalización completa (snake_case, etc.) se hace en la capa silver con dbt
    
    Args:
        local_path: Ruta local del archivo Excel
        sheet_name: Nombre de la hoja a leer
    
    Returns:
        DataFrame transformado
    """
    # Leer la hoja sin usar encabezados automáticamente
    df = pd.read_excel(local_path, sheet_name=sheet_name, header=None)
    
    if DEBUG:
        print(f"[DEBUG] Hoja '{sheet_name}': {df.shape[0]} filas (sin encabezados), {df.shape[1]} columnas")
    
    # SIEMPRE omitir la primera fila y usar la segunda como encabezados
    if len(df) > 1:
        # La primera fila (índice 0) se elimina (metadatos/estructura)
        # La segunda fila (índice 1) se usa como encabezados
        headers = df.iloc[1].astype(str).tolist()
        
        # Normalización MÍNIMA solo para que BigQuery acepte los nombres
        # (reemplazar espacios con guiones bajos, mantener el resto)
        headers = [_minimal_normalize_for_bq(h) for h in headers]
        
        # Eliminar la primera fila (metadatos) y la segunda fila (que ahora son los encabezados)
        df = df.iloc[2:].reset_index(drop=True)
        
        # Asignar los encabezados (con normalización mínima)
        df.columns = headers
        
        if DEBUG:
            print(f"[DEBUG] Primera fila eliminada. Segunda fila usada como encabezados.")
            print(f"[DEBUG] Filas restantes: {df.shape[0]}")
            print(f"[DEBUG] Encabezados (normalización mínima, primeros 10): {list(df.columns)[:10]}...")
    elif len(df) > 0:
        # Si solo hay una fila, usar la primera como encabezados (caso edge)
        headers = df.iloc[0].astype(str).tolist()
        headers = [_minimal_normalize_for_bq(h) for h in headers]
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = headers
        if DEBUG:
            print(f"[WARN] Hoja '{sheet_name}' solo tiene 1 fila. Usando primera fila como encabezados.")
    
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
    SIEMPRE omite la primera hoja del Excel y SIEMPRE elimina la primera fila de cada hoja restante.
    
    IMPORTANTE: Esta función espera que el archivo Excel tenga 4 hojas:
    - Primera hoja (índice 0): Estructura/metadatos → SE ELIMINA SIEMPRE
    - Segunda hoja (índice 1): Dato_original → idc_raw_data_dato_original
    - Tercera hoja (índice 2): Valor_normalizado → idc_raw_data_valor_normalizado
    - Cuarta hoja (índice 3): Valor_ranking → idc_raw_data_valor_ranking
    
    Después de eliminar la primera hoja, las 3 hojas restantes se procesan:
    - Se elimina la primera fila de cada hoja (fila de metadatos/estructura)
    - La segunda fila se usa como encabezados (nombres de columnas)
    
    Args:
        local_path: Ruta local del archivo Excel
        dataset_id: ID del dataset en BigQuery
        sheet_to_table_mapping: Diccionario que mapea nombres de hojas a nombres de tablas
                                Ej: {"Dato_original": "idc_raw_data_dato_original"}
                                NOTA: Los nombres se usan solo para referencia, el mapeo real es por índice
    
    Returns:
        Diccionario con el mapeo de hojas a tablas creadas
    """
    # Obtener todas las hojas disponibles (SIEMPRE omitir la primera hoja)
    available_sheets = get_excel_sheet_names(local_path)
    
    if DEBUG:
        print(f"[DEBUG] Hojas disponibles después de omitir la primera: {available_sheets}")
        print(f"[DEBUG] Esperadas: {list(sheet_to_table_mapping.keys())}")
    
    # Verificar que tengamos al menos las hojas necesarias
    if len(available_sheets) < len(sheet_to_table_mapping):
        all_sheets = pd.ExcelFile(local_path).sheet_names
        raise ValueError(
            f"No hay suficientes hojas en el Excel. Se esperaban {len(sheet_to_table_mapping)} hojas "
            f"después de omitir la primera, pero solo hay {len(available_sheets)}. "
            f"Hojas disponibles (después de omitir primera): {available_sheets}. "
            f"Todas las hojas del Excel: {all_sheets}"
        )
    
    results = {}
    
    # Procesar cada hoja por índice (no por nombre, para ser más robusto)
    # El orden en sheet_to_table_mapping define el índice: primera clave = índice 0, segunda = índice 1, etc.
    for idx, (expected_sheet_name, table_name) in enumerate(sheet_to_table_mapping.items()):
        if idx >= len(available_sheets):
            raise ValueError(
                f"No hay suficientes hojas. Se esperaba la hoja en índice {idx} ({expected_sheet_name}), "
                f"pero solo hay {len(available_sheets)} hojas disponibles."
            )
        
        # Obtener el nombre real de la hoja en el índice correspondiente
        actual_sheet_name = available_sheets[idx]
        
        if DEBUG:
            print(f"\n[INFO] Procesando hoja en índice {idx}: '{actual_sheet_name}' (esperada: '{expected_sheet_name}') -> tabla '{table_name}'")
        
        # Transformar la hoja (SIEMPRE eliminar primera fila)
        df = transform_excel_sheet(local_path, actual_sheet_name)
        
        # Cargar a BigQuery
        load_dataframe_to_bq(df, dataset_id=dataset_id, table_name=table_name)
        
        results[actual_sheet_name] = table_name
    
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

