# modules/idi/idi_load.py
"""
Módulo para cargar archivos CSV de IDI desde Google Cloud Storage a BigQuery.
Carga todos los años disponibles en carpetas separadas.
"""
from google.cloud import storage, bigquery
import os
import tempfile
from typing import List, Dict
from modules.config import PROJECT_ID, CONF, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE
from modules.gcp_utils import get_bq_client, get_gcs_client

# === CONFIGURACIÓN ===
DEBUG = CONF.global_config.debug


def ensure_dataset(dataset_id: str, location: str = "us-central1"):
    """
    Asegura que el dataset de BigQuery exista, si no lo crea.
    
    Args:
        dataset_id: ID del dataset (ej: 'bronze_dpt_planeacion_municipal_dev')
        location: Ubicación del dataset (default: us-central1)
    """
    client = get_bq_client()
    
    dataset_ref = f"{PROJECT_ID}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        if DEBUG:
            print(f"[OK] Dataset ya existe: {dataset_ref}")
    except Exception:
        # Dataset no existe, crearlo
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset.description = "Bronze layer para datos de IDI"
        client.create_dataset(dataset)
        print(f"[OK] Dataset creado: {dataset_ref} ({location})")


def get_all_years_from_gcs(bucket_name: str, base_folder: str) -> List[int]:
    """
    Obtiene todos los años (carpetas) disponibles en GCS.
    
    Args:
        bucket_name: Nombre del bucket
        base_folder: Carpeta base (ej: 'data_staging/dpt_planeacion_municipal/idi')
    
    Returns:
        Lista de años encontrados (ordenados de más reciente a más antiguo)
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    
    # Asegurar que base_folder termine con /
    if not base_folder.endswith('/'):
        base_folder = base_folder + '/'
    
    # Listar todos los blobs en la carpeta base
    blobs = bucket.list_blobs(prefix=base_folder)
    
    # Extraer años de los paths de los archivos
    years = set()
    for blob in blobs:
        # blob.name ejemplo: 'data_staging/dpt_planeacion_municipal/idi/2024/resultados_2024.csv'
        # Remover el prefijo base_folder
        relative_path = blob.name[len(base_folder):]
        
        # Dividir por / y tomar la primera parte (el año)
        parts = relative_path.split('/')
        if len(parts) >= 1:
            year_str = parts[0]
            
            try:
                year = int(year_str)
                years.add(year)
            except ValueError:
                # No es un año válido, saltar
                continue
    
    years_list = sorted(list(years), reverse=True)
    
    if DEBUG:
        print(f"[INFO] Años encontrados en GCS: {years_list}")
    
    return years_list


def download_csv_from_gcs(gcs_uri: str) -> str:
    """
    Descarga un archivo CSV desde GCS a un archivo temporal local.
    
    Args:
        gcs_uri: URI completa del CSV (ej: gs://bucket/folder/file.csv)
    
    Returns:
        Ruta local del archivo temporal descargado
    """
    client = get_gcs_client()
    
    # Parsear URI: gs://bucket/path/file.csv
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"URI inválida (debe empezar con gs://): {gcs_uri}")
    
    parts = gcs_uri[5:].split("/", 1)  # Remover 'gs://' y dividir
    bucket_name = parts[0]
    blob_path = parts[1]
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    if not blob.exists():
        raise FileNotFoundError(f"El archivo no existe en GCS: {gcs_uri}")
    
    # Descargar a archivo temporal
    fd, local_path = tempfile.mkstemp(suffix='.csv')
    os.close(fd)
    
    blob.download_to_filename(local_path)
    
    if DEBUG:
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"[OK] CSV descargado de GCS: {local_path} ({size_mb:.2f} MB)")
    
    return local_path


def load_csv_to_bq(
    local_csv_path: str,
    dataset_id: str,
    table_name: str,
    write_disposition: str = "WRITE_TRUNCATE"
) -> str:
    """
    Carga un archivo CSV local a una tabla de BigQuery.
    
    Args:
        local_csv_path: Ruta local del CSV
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla
        write_disposition: 'WRITE_TRUNCATE' (reemplazar) o 'WRITE_APPEND' (agregar)
    
    Returns:
        Nombre completo de la tabla (project.dataset.table)
    """
    client = get_bq_client()
    
    table_ref = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    if DEBUG:
        print(f"[INFO] Cargando CSV a BigQuery: {table_ref}")
    
    # Configuración de carga
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Saltar encabezados del CSV
        autodetect=True,  # Detectar automáticamente esquema
        write_disposition=write_disposition,
        field_delimiter=';',  # ⬅️ Importante: usar punto y coma como delimitador
        allow_quoted_newlines=True,
        allow_jagged_rows=True,  # Permitir filas con diferente número de columnas
    )
    
    # Cargar archivo
    with open(local_csv_path, 'rb') as f:
        load_job = client.load_table_from_file(
            f,
            table_ref,
            job_config=job_config
        )
    
    # Esperar a que termine
    load_job.result()
    
    # Obtener info de la tabla
    table = client.get_table(table_ref)
    
    if DEBUG:
        print(f"[OK] Tabla cargada: {table_ref}")
        print(f"     Filas: {table.num_rows}")
        print(f"     Columnas: {len(table.schema)}")
    
    return table_ref


def load_all_years_idi_to_bq(
    bucket_name: str,
    base_folder: str,
    dataset_id: str,
    table_prefix: str = "idi_raw_data_territorio"
) -> Dict[int, str]:
    """
    Carga todos los años disponibles de IDI a BigQuery.
    Cada año se carga en una tabla separada.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        base_folder: Carpeta base donde están los años
        dataset_id: Dataset de BigQuery
        table_prefix: Prefijo para los nombres de tablas
    
    Returns:
        Diccionario {año: nombre_tabla}
    """
    # 1. Obtener todos los años disponibles
    years = get_all_years_from_gcs(bucket_name, base_folder)
    
    if not years:
        raise ValueError(f"No se encontraron años en gs://{bucket_name}/{base_folder}")
    
    results = {}
    temp_files = []
    
    try:
        # 2. Procesar cada año
        for year in years:
            if DEBUG:
                print(f"\n{'='*60}")
                print(f"PROCESANDO AÑO: {year}")
                print(f"{'='*60}\n")
            
            # Construir URI del CSV
            csv_uri = f"gs://{bucket_name}/{base_folder}/{year}/resultados_{year}.csv"
            
            # Descargar CSV
            local_csv = download_csv_from_gcs(csv_uri)
            temp_files.append(local_csv)
            
            # Nombre de la tabla
            table_name = f"{table_prefix}_{year}"
            
            # Cargar a BigQuery
            table_ref = load_csv_to_bq(
                local_csv_path=local_csv,
                dataset_id=dataset_id,
                table_name=table_name
            )
            
            results[year] = table_ref
            
            if DEBUG:
                print(f"[OK] Año {year} cargado en tabla: {table_name}")
        
        # 3. Resumen
        if DEBUG:
            print(f"\n{'='*60}")
            print(f"RESUMEN DE CARGA")
            print(f"{'='*60}")
            print(f"Total años procesados: {len(results)}")
            for year, table_ref in results.items():
                print(f"  ✅ {year} -> {table_ref}")
            print(f"{'='*60}\n")
        
        return results
        
    finally:
        # Limpiar archivos temporales
        cleanup_temp_paths(temp_files)


def cleanup_temp_paths(paths: List[str]):
    """
    Elimina archivos temporales.
    
    Args:
        paths: Lista de rutas de archivos a eliminar
    """
    for path in paths:
        if path and os.path.exists(path):
            try:
                os.unlink(path)
                if DEBUG:
                    print(f"[DEBUG] Archivo temporal eliminado: {path}")
            except Exception as e:
                print(f"[WARN] No se pudo eliminar {path}: {e}")