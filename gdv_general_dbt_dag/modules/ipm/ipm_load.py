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
    file_size_mb = file_size / (1024 * 1024) if file_size else 0
    
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
    if file_size and downloaded_size != file_size:
        raise ValueError(
            f"El archivo no se descargó completamente. "
            f"Tamaño esperado: {file_size} bytes, "
            f"Tamaño descargado: {downloaded_size} bytes"
        )
    
    if DEBUG:
        print(f"[DEBUG] Archivo descargado completamente en {tmp_path}")
        print(f"[DEBUG] Tamaño verificado: {downloaded_size} bytes ({file_size_mb:.2f} MB)")
    
    return tmp_path

def read_excel_from_gcs_direct(gcs_uri: str, sheet_index: int = 0) -> pd.DataFrame:
    """
    Lee un archivo Excel directamente desde GCS a memoria (sin descargar a disco).
    Más eficiente para archivos grandes ya que evita escribir en disco.
    
    Args:
        gcs_uri: URI completa del archivo en GCS (gs://bucket/path/file.xlsx)
        sheet_index: Índice de la hoja a leer (por defecto 0)
    
    Returns:
        DataFrame con los datos tal cual están en el Excel
    """
    from io import BytesIO
    
    gcs = _gcs_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = gcs.bucket(bucket_name).blob(blob_name)
    
    # Verificar que el archivo existe
    if not blob.exists():
        raise FileNotFoundError(f"El archivo no existe: {gcs_uri}")
    
    # Obtener el tamaño del archivo
    blob.reload()
    file_size = blob.size
    file_size_mb = file_size / (1024 * 1024) if file_size else 0
    
    if DEBUG:
        print(f"[INFO] Leyendo archivo Excel directamente desde GCS (sin descargar a disco)")
        print(f"[INFO] URI: {gcs_uri}")
        print(f"[INFO] Tamaño del archivo: {file_size_mb:.2f} MB ({file_size} bytes)")
        if file_size_mb > 100:
            print(f"[INFO] Archivo grande detectado. Esto puede tomar varios minutos...")
    
    # Leer el archivo desde GCS directamente a memoria (BytesIO)
    file_buffer = BytesIO()
    blob.download_to_file(file_buffer)
    file_buffer.seek(0)  # Resetear al inicio para que pandas pueda leerlo
    
    if DEBUG:
        print(f"[INFO] Archivo cargado en memoria. Leyendo con pandas...")
    
    # Leer el Excel desde el buffer en memoria
    try:
        df = pd.read_excel(
            file_buffer,
            sheet_name=sheet_index,
            header=0,
            engine='openpyxl'
        )
        
        if DEBUG:
            print(f"[INFO] Archivo leído exitosamente")
            print(f"[INFO] Filas: {len(df)}, Columnas: {len(df.columns)}")
            print(f"[INFO] Encabezados: {list(df.columns)}")
        
        # Cerrar el buffer
        file_buffer.close()
        
        return df
        
    except Exception as e:
        file_buffer.close()
        error_msg = f"No se pudo leer el archivo Excel desde GCS: {gcs_uri}\n"
        error_msg += f"Tamaño del archivo: {file_size_mb:.2f} MB ({file_size} bytes)\n"
        error_msg += f"Error: {str(e)}\n"
        error_msg += f"\nSugerencias:\n"
        error_msg += f"- Verifica que el archivo se descargó completamente\n"
        error_msg += f"- Verifica que el archivo no esté corrupto\n"
        error_msg += f"- Para archivos muy grandes (>2GB), considera dividirlos o usar otro formato"
        raise ValueError(error_msg) from e


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

def convert_excel_to_csv_in_gcs(
    gcs_excel_uri: str,
    gcs_csv_uri: str,
    sheet_index: int = 0,
    delete_excel_after: bool = False,
    chunk_size: int = 10000
) -> str:
    """
    Convierte un archivo Excel desde GCS a CSV procesándolo por chunks.
    Evita cargar todo el archivo en memoria, ideal para archivos muy grandes.
    
    Args:
        gcs_excel_uri: URI del archivo Excel en GCS (gs://bucket/path/file.xlsx)
        gcs_csv_uri: URI destino del CSV en GCS (gs://bucket/path/file.csv)
        sheet_index: Índice de la hoja a convertir (default: 0)
        delete_excel_after: Si es True, elimina el Excel después de convertir (default: False)
        chunk_size: Número de filas a procesar por chunk (default: 10000)
    
    Returns:
        URI del CSV creado en GCS
    """
    import csv as csv_module
    from openpyxl import load_workbook
    
    gcs = _gcs_client()
    
    # Parsear URIs
    excel_bucket_name = gcs_excel_uri.split("/")[2]
    excel_blob_name = "/".join(gcs_excel_uri.split("/")[3:])
    csv_bucket_name = gcs_csv_uri.split("/")[2]
    csv_blob_name = "/".join(gcs_csv_uri.split("/")[3:])
    
    if DEBUG:
        print(f"[INFO] Convirtiendo Excel a CSV (procesamiento por chunks)")
        print(f"[INFO] Excel origen: {gcs_excel_uri}")
        print(f"[INFO] CSV destino: {gcs_csv_uri}")
        print(f"[INFO] Tamaño de chunk: {chunk_size} filas")
    
    # Verificar archivo Excel
    excel_blob = gcs.bucket(excel_bucket_name).blob(excel_blob_name)
    if not excel_blob.exists():
        raise FileNotFoundError(f"El archivo Excel no existe: {gcs_excel_uri}")
    
    excel_blob.reload()
    file_size = excel_blob.size
    file_size_mb = file_size / (1024 * 1024) if file_size else 0
    
    if DEBUG:
        print(f"[INFO] Tamaño del Excel: {file_size_mb:.2f} MB")
        print(f"[INFO] Descargando Excel a disco temporal...")
    
    # Descargar Excel a disco temporal
    fd_excel, tmp_excel_path = tempfile.mkstemp(suffix='.xlsx')
    fd_csv, tmp_csv_path = tempfile.mkstemp(suffix='.csv')
    
    try:
        os.close(fd_excel)
        os.close(fd_csv)
        
        # Descargar Excel
        excel_blob.download_to_filename(tmp_excel_path)
        
        if DEBUG:
            downloaded_size = os.path.getsize(tmp_excel_path)
            print(f"[INFO] Excel descargado: {downloaded_size / (1024*1024):.2f} MB")
            print(f"[INFO] Procesando Excel por chunks usando openpyxl...")
        
        # Abrir Excel con openpyxl (más eficiente para lectura por filas)
        workbook = load_workbook(tmp_excel_path, read_only=True, data_only=True)
        sheet = workbook.worksheets[sheet_index]
        
        # Abrir archivo CSV para escritura con manejo robusto de caracteres especiales
        with open(tmp_csv_path, 'w', newline='', encoding='utf-8', errors='replace') as csv_file:
            csv_writer = csv_module.writer(csv_file, quoting=csv_module.QUOTE_MINIMAL)
            
            # Leer y escribir fila por fila (procesamiento por chunks)
            total_rows = 0
            
            try:
                for row in sheet.iter_rows(values_only=True):
                    # Convertir valores a string y manejar None/NaN
                    row_str = []
                    for cell_value in row:
                        if cell_value is None:
                            row_str.append('')
                        else:
                            try:
                                # Manejar diferentes tipos de datos
                                if isinstance(cell_value, (datetime, date, time)):
                                    # Convertir fechas a string ISO format
                                    cell_str = cell_value.isoformat() if hasattr(cell_value, 'isoformat') else str(cell_value)
                                elif isinstance(cell_value, (int, float)):
                                    # Convertir números a string
                                    cell_str = str(cell_value)
                                else:
                                    # Convertir a string y limpiar valores especiales
                                    cell_str = str(cell_value)
                                
                                # Limpiar valores especiales
                                if cell_str.lower() in ['nan', 'none', 'nat', '']:
                                    cell_str = ''
                                
                                # Reemplazar caracteres problemáticos para CSV
                                cell_str = cell_str.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
                                
                            except Exception as e:
                                # Si hay error al convertir, usar string vacío
                                if DEBUG and total_rows < 10:  # Solo mostrar primeros errores
                                    print(f"[WARN] Error procesando celda: {e}, usando valor vacío")
                                cell_str = ''
                            
                            row_str.append(cell_str)
                    
                    # Escribir fila al CSV
                    csv_writer.writerow(row_str)
                    total_rows += 1
                    
                    # Mostrar progreso cada chunk_size filas
                    if total_rows % chunk_size == 0:
                        if DEBUG:
                            print(f"[INFO] Procesadas {total_rows} filas...")
                            
            except Exception as e:
                if DEBUG:
                    print(f"[ERROR] Error procesando fila {total_rows + 1}: {e}")
                raise
        
        workbook.close()
        
        if DEBUG:
            csv_size = os.path.getsize(tmp_csv_path)
            csv_size_mb = csv_size / (1024 * 1024)
            print(f"[INFO] CSV generado: {csv_size_mb:.2f} MB ({total_rows} filas)")
            print(f"[INFO] Subiendo CSV a GCS...")
        
        # Subir CSV desde disco a GCS
        csv_blob = gcs.bucket(csv_bucket_name).blob(csv_blob_name)
        csv_blob.upload_from_filename(tmp_csv_path, content_type='text/csv')
        
        if DEBUG:
            print(f"[OK] CSV subido exitosamente a: {gcs_csv_uri}")
        
        # Opcional: eliminar Excel original
        if delete_excel_after:
            excel_blob.delete()
            if DEBUG:
                print(f"[INFO] Archivo Excel original eliminado")
        
        return gcs_csv_uri
        
    finally:
        # Limpiar archivos temporales
        try:
            if os.path.exists(tmp_excel_path):
                os.unlink(tmp_excel_path)
            if os.path.exists(tmp_csv_path):
                os.unlink(tmp_csv_path)
            if DEBUG:
                print(f"[DEBUG] Archivos temporales eliminados")
        except Exception as e:
            print(f"[WARN] No se pudieron eliminar archivos temporales: {e}")

def load_csv_from_gcs_to_bq_raw(
    gcs_csv_uri: str,
    dataset_id: str,
    table_name: str
) -> str:
    """
    Carga un archivo CSV directamente desde GCS a BigQuery.
    MUCHO más rápido que cargar desde DataFrame porque BigQuery lee directamente desde GCS.
    Todas las columnas se cargan como STRING para preservar los datos originales.
    
    Args:
        gcs_csv_uri: URI del archivo CSV en GCS (gs://bucket/path/file.csv)
        dataset_id: ID del dataset en BigQuery
        table_name: Nombre de la tabla en BigQuery
    
    Returns:
        URI completa de la tabla creada
    """
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    if DEBUG:
        print(f"[INFO] Cargando CSV directamente desde GCS a BigQuery")
        print(f"[INFO] CSV URI: {gcs_csv_uri}")
        print(f"[INFO] Tabla destino: {table_fqn}")
    
    # Primero necesitamos leer el CSV para obtener los nombres de las columnas
    # Leemos solo el header para crear el esquema
    gcs = _gcs_client()
    csv_bucket_name = gcs_csv_uri.split("/")[2]
    csv_blob_name = "/".join(gcs_csv_uri.split("/")[3:])
    
    csv_blob = gcs.bucket(csv_bucket_name).blob(csv_blob_name)
    if not csv_blob.exists():
        raise FileNotFoundError(f"El archivo CSV no existe: {gcs_csv_uri}")
    
    # Recargar blob para obtener metadata (incluyendo size)
    csv_blob.reload()
    file_size = csv_blob.size
    if file_size is None:
        raise ValueError(f"No se pudo obtener el tamaño del archivo CSV: {gcs_csv_uri}")
    
    # Leer solo la primera línea para obtener los nombres de las columnas
    from io import BytesIO
    header_buffer = BytesIO()
    csv_blob.download_to_file(header_buffer, start=0, end=min(10000, file_size))
    header_buffer.seek(0)
    header_content = header_buffer.read().decode('utf-8', errors='replace')
    header_buffer.close()
    
    # Obtener la primera línea completa (puede tener saltos de línea dentro de campos entre comillas)
    import csv as csv_module
    first_line = header_content.split('\n')[0]
    
    # Leer el header usando el parser CSV para manejar correctamente las comillas
    reader = csv_module.reader([first_line])
    try:
        columns = next(reader)
    except StopIteration:
        # Si no hay header, intentar leer más líneas
        lines = header_content.split('\n', 2)
        if len(lines) > 1:
            reader = csv_module.reader([lines[0] + '\n' + lines[1]])
            columns = next(reader)
        else:
            raise ValueError("No se pudo leer el header del CSV")
    
    # Generar nombres de columnas válidos (BigQuery no permite campos sin nombre)
    valid_columns = []
    for idx, col in enumerate(columns):
        # Limpiar el nombre de la columna
        col_name = str(col).strip() if col else ''
        
        # Si está vacío o solo tiene espacios, generar un nombre automático
        if not col_name or col_name == '':
            col_name = f"col_{idx}"
        else:
            # Limpiar caracteres no válidos para nombres de campos en BigQuery
            # BigQuery permite: letras, números, guiones bajos
            import re
            col_name = re.sub(r'[^a-zA-Z0-9_]', '_', col_name)
            # No puede empezar con número
            if col_name and col_name[0].isdigit():
                col_name = f"col_{col_name}"
            # No puede estar vacío después de limpiar
            if not col_name:
                col_name = f"col_{idx}"
        
        valid_columns.append(col_name)
    
    if DEBUG:
        print(f"[INFO] Columnas detectadas: {len(valid_columns)}")
        print(f"[INFO] Primeras 10 columnas: {valid_columns[:10]}")
        if len(valid_columns) > 10:
            print(f"[INFO] Últimas 5 columnas: {valid_columns[-5:]}")
    
    # Crear esquema: todas las columnas como STRING
    schema = [bigquery.SchemaField(col, "STRING") for col in valid_columns]
    
    # Configuración para cargar CSV desde GCS
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Saltar el header
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
        field_delimiter=',',
        quote_character='"',
        allow_quoted_newlines=True,
        encoding='UTF-8',
        # Todas las columnas se tratan como STRING
        autodetect=False,  # No auto-detectar tipos, usar el esquema
    )
    
    if DEBUG:
        print(f"[INFO] Iniciando carga desde GCS a BigQuery...")
        print(f"[INFO] Esto puede tomar varios minutos para archivos grandes...")
    
    # Cargar directamente desde GCS a BigQuery (MUCHO más rápido)
    load_job = client.load_table_from_uri(
        gcs_csv_uri,
        table_fqn,
        job_config=job_config
    )
    
    # Esperar a que termine la carga
    load_job.result()
    
    if DEBUG:
        table = client.get_table(table_fqn)
        print(f"[OK] Cargadas {table.num_rows} filas en {table_fqn}")
        print(f"[OK] Todas las columnas cargadas como STRING para preservar datos originales")
    
    return table_fqn


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
