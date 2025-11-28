# modules/ipm_sisben_ingest.py
"""
Módulo para ingerir archivos IPM SISBEN desde una carpeta temporal en GCS
y copiarlos a la carpeta de destino.
"""
from google.cloud import storage
import os
from typing import Optional

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True

# ---------------------------
# Clientes
# ---------------------------
def _gcs_client() -> storage.Client:
    """Crea un cliente de Google Cloud Storage."""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return storage.Client(project=PROJECT_ID)

# ---------------------------
# Funciones de utilidad
# ---------------------------
def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    """
    Parsea una URI de GCS (gs://bucket/path) y retorna bucket y ruta.
    
    Args:
        gcs_uri: URI completa de GCS (ej: 'gs://bucket-name/path/to/file.xlsx')
    
    Returns:
        Tupla con (bucket_name, blob_path)
    """
    if not gcs_uri.startswith('gs://'):
        raise ValueError(f"La URI debe comenzar con 'gs://': {gcs_uri}")
    
    # Remover el prefijo 'gs://'
    path = gcs_uri[5:]
    
    # Dividir en bucket y ruta
    parts = path.split('/', 1)
    bucket_name = parts[0]
    
    if len(parts) > 1:
        blob_path = parts[1]
    else:
        blob_path = ''
    
    return bucket_name, blob_path

# ---------------------------
# Funciones principales
# ---------------------------
def copy_file_within_gcs(
    bucket_name: str,
    source_blob_name: str,
    destination_blob_name: str,
    overwrite: bool = True
) -> str:
    """
    Copia un archivo dentro del mismo bucket de GCS.
    Si la carpeta destino no existe, se crea automáticamente.
    Si el archivo destino ya existe, se sobrescribe si overwrite=True.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        source_blob_name: Ruta del archivo origen en GCS (ej: 'data_staging/dpt_planeacion_municipal/tmp/archivo.xlsx')
        destination_blob_name: Ruta del archivo destino en GCS (ej: 'data_staging/dpt_planeacion_municipal/ipm/sisben/archivo.xlsx')
        overwrite: Si es True, sobrescribe el archivo si ya existe. Si es False, mantiene el anterior.
    
    Returns:
        URI completa del archivo destino en GCS (gs://bucket/path)
    """
    gcs_client = _gcs_client()
    
    # Obtener el bucket
    try:
        bucket = gcs_client.bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder al bucket '{bucket_name}': {e}")
    
    # Normalizar las rutas: eliminar barras dobles y asegurar formato consistente
    source_blob_name = '/'.join(part.strip('/') for part in source_blob_name.split('/') if part.strip('/'))
    destination_blob_name = '/'.join(part.strip('/') for part in destination_blob_name.split('/') if part.strip('/'))
    
    # Verificar que el archivo origen existe
    source_blob = bucket.blob(source_blob_name)
    if not source_blob.exists():
        raise FileNotFoundError(f"El archivo origen no existe: gs://{bucket_name}/{source_blob_name}")
    
    if DEBUG:
        print(f"[INFO] Archivo origen encontrado: gs://{bucket_name}/{source_blob_name}")
        file_size = source_blob.size
        file_size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
        print(f"[INFO] Tamaño del archivo: {file_size_mb:.2f} MB ({file_size} bytes)")
    
    # Verificar si el archivo destino ya existe
    destination_blob = bucket.blob(destination_blob_name)
    if destination_blob.exists():
        if overwrite:
            if DEBUG:
                print(f"[INFO] El archivo destino '{destination_blob_name}' ya existe. Se eliminará y se sobrescribirá con el nuevo.")
            destination_blob.delete()
            if DEBUG:
                print(f"[INFO] Archivo anterior eliminado.")
        else:
            if DEBUG:
                print(f"[WARN] El archivo destino '{destination_blob_name}' ya existe. Se mantendrá el anterior (overwrite=False).")
            gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
            return gcs_uri
    
    # Verificar carpeta destino (opcional, solo para logging)
    folder_path = '/'.join(destination_blob_name.split('/')[:-1]) if '/' in destination_blob_name else ''
    if folder_path and DEBUG:
        # Listar objetos en esa carpeta para verificar si existe
        blobs = list(bucket.list_blobs(prefix=folder_path + '/', max_results=1))
        if blobs:
            print(f"[INFO] Carpeta destino '{folder_path}' ya existe.")
        else:
            print(f"[INFO] Carpeta destino '{folder_path}' no existe. Se creará automáticamente al copiar el archivo.")
    
    # Copiar el archivo (GCS crea automáticamente la "carpeta" si no existe)
    if DEBUG:
        print(f"[INFO] Copiando archivo a: gs://{bucket_name}/{destination_blob_name}")
    
    # Usar copy_blob para copiar dentro del mismo bucket (más eficiente)
    source_blob_copy = bucket.copy_blob(
        blob=source_blob,
        destination_bucket=bucket,
        new_name=destination_blob_name
    )
    
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    
    if DEBUG:
        print(f"[OK] Archivo copiado exitosamente a: {gcs_uri}")
    
    return gcs_uri

def get_latest_file_from_gcs_folder(
    bucket_name: str,
    folder_path: str,
    file_extension: Optional[str] = None
) -> str:
    """
    Obtiene la ruta del último archivo subido a una carpeta en GCS.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_path: Ruta de la carpeta (ej: 'data_staging/dpt_planeacion_municipal/tmp')
        file_extension: Extensión opcional para filtrar archivos (ej: '.xlsx'). Si es None, busca cualquier archivo.
    
    Returns:
        Nombre del blob (ruta completa) del archivo más reciente
    """
    gcs_client = _gcs_client()
    
    # Normalizar la ruta de la carpeta (asegurar que termine con /)
    if not folder_path.endswith('/'):
        folder_path = folder_path + '/'
    
    # Obtener el bucket
    try:
        bucket = gcs_client.bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder al bucket '{bucket_name}': {e}")
    
    # Listar todos los blobs en la carpeta
    blobs = list(bucket.list_blobs(prefix=folder_path))
    
    # Filtrar solo archivos (no carpetas) y por extensión si se especifica
    files = []
    for blob in blobs:
        # Ignorar "carpetas" (blobs que terminan en /)
        if blob.name.endswith('/'):
            continue
        
        # Filtrar por extensión si se especifica
        if file_extension:
            if blob.name.lower().endswith(file_extension.lower()):
                files.append(blob)
        else:
            files.append(blob)
    
    if not files:
        error_msg = f"No se encontraron archivos"
        if file_extension:
            error_msg += f" con extensión '{file_extension}'"
        error_msg += f" en la carpeta 'gs://{bucket_name}/{folder_path}'"
        raise ValueError(error_msg)
    
    # Ordenar por tiempo de actualización (más reciente primero)
    files.sort(key=lambda x: x.time_created, reverse=True)
    
    # Tomar el más reciente
    latest_blob = files[0]
    
    if DEBUG:
        print(f"[DEBUG] Archivos encontrados en la carpeta: {len(files)}")
        for blob in files[:5]:  # Mostrar los primeros 5
            print(f"[DEBUG]   - {blob.name} (creado: {blob.time_created})")
        print(f"[INFO] Usando archivo más reciente: {latest_blob.name} (creado: {latest_blob.time_created})")
    
    return latest_blob.name

def copy_file_from_gcs_uri_to_destination(
    source_gcs_uri: str,
    destination_bucket_name: str,
    destination_folder: str,
    file_extension: Optional[str] = ".xlsx",
    overwrite: bool = True
) -> str:
    """
    Copia un archivo desde una URI completa de GCS a una carpeta destino.
    Permite copiar desde cualquier bucket/proyecto a otro.
    Si la URI apunta a una carpeta, busca el último archivo con la extensión especificada.
    
    Args:
        source_gcs_uri: URI completa del archivo o carpeta origen (ej: 'gs://bucket-name/path/to/file.xlsx' o 'gs://bucket-name/path/to/folder/')
        destination_bucket_name: Nombre del bucket destino
        destination_folder: Carpeta destino (ej: 'data_staging/dpt_planeacion_municipal/ipm/sisben')
        file_extension: Extensión del archivo a buscar si source_gcs_uri es una carpeta (default: '.xlsx')
        overwrite: Si es True, sobrescribe el archivo si ya existe.
    
    Returns:
        URI completa del archivo destino en GCS (gs://bucket/path)
    """
    # Parsear la URI origen
    source_bucket_name, source_path = parse_gcs_uri(source_gcs_uri)
    
    if DEBUG:
        print(f"[INFO] URI origen: {source_gcs_uri}")
        print(f"[INFO] Bucket origen: {source_bucket_name}")
        print(f"[INFO] Ruta origen: {source_path}")
    
    # Determinar si es un archivo específico o una carpeta
    # Si termina con una extensión de archivo común, es un archivo específico
    common_extensions = ('.xlsx', '.xls', '.csv', '.txt', '.json', '.parquet', '.zip', '.gz')
    is_file = source_path.endswith(common_extensions) if source_path else False
    
    if is_file:
        # Es un archivo específico
        source_blob_name = source_path
        if DEBUG:
            print(f"[INFO] URI apunta a un archivo específico: {source_blob_name}")
    else:
        # Es una carpeta, buscar el último archivo
        if DEBUG:
            print(f"[INFO] URI apunta a una carpeta. Buscando último archivo con extensión '{file_extension}'...")
        source_blob_name = get_latest_file_from_gcs_folder(
            bucket_name=source_bucket_name,
            folder_path=source_path,
            file_extension=file_extension
        )
        if DEBUG:
            print(f"[INFO] Archivo encontrado: {source_blob_name}")
    
    # Extraer el nombre del archivo
    file_name = source_blob_name.split('/')[-1]
    
    # Construir la ruta destino
    destination_blob_name = f"{destination_folder}/{file_name}"
    
    # Verificar si origen y destino están en el mismo bucket
    if source_bucket_name == destination_bucket_name:
        # Mismo bucket: usar copy_file_within_gcs (más eficiente)
        if DEBUG:
            print(f"[INFO] Origen y destino en el mismo bucket. Usando copia interna.")
        gcs_uri = copy_file_within_gcs(
            bucket_name=source_bucket_name,
            source_blob_name=source_blob_name,
            destination_blob_name=destination_blob_name,
            overwrite=overwrite
        )
    else:
        # Diferentes buckets: necesitamos copiar entre buckets
        if DEBUG:
            print(f"[INFO] Origen y destino en buckets diferentes. Copiando entre buckets.")
        gcs_uri = copy_file_between_buckets(
            source_bucket_name=source_bucket_name,
            source_blob_name=source_blob_name,
            destination_bucket_name=destination_bucket_name,
            destination_blob_name=destination_blob_name,
            overwrite=overwrite
        )
    
    return gcs_uri

def copy_file_between_buckets(
    source_bucket_name: str,
    source_blob_name: str,
    destination_bucket_name: str,
    destination_blob_name: str,
    overwrite: bool = True
) -> str:
    """
    Copia un archivo entre diferentes buckets de GCS.
    
    Args:
        source_bucket_name: Nombre del bucket origen
        source_blob_name: Ruta del archivo origen
        destination_bucket_name: Nombre del bucket destino
        destination_blob_name: Ruta del archivo destino
        overwrite: Si es True, sobrescribe el archivo si ya existe.
    
    Returns:
        URI completa del archivo destino en GCS (gs://bucket/path)
    """
    gcs_client = _gcs_client()
    
    # Obtener buckets
    try:
        source_bucket = gcs_client.bucket(source_bucket_name)
        destination_bucket = gcs_client.bucket(destination_bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder a los buckets: {e}")
    
    # Normalizar las rutas
    source_blob_name = '/'.join(part.strip('/') for part in source_blob_name.split('/') if part.strip('/'))
    destination_blob_name = '/'.join(part.strip('/') for part in destination_blob_name.split('/') if part.strip('/'))
    
    # Verificar que el archivo origen existe
    source_blob = source_bucket.blob(source_blob_name)
    if not source_blob.exists():
        raise FileNotFoundError(f"El archivo origen no existe: gs://{source_bucket_name}/{source_blob_name}")
    
    if DEBUG:
        print(f"[INFO] Archivo origen encontrado: gs://{source_bucket_name}/{source_blob_name}")
        file_size = source_blob.size
        file_size_mb = file_size / (1024 * 1024) if file_size > 0 else 0
        print(f"[INFO] Tamaño del archivo: {file_size_mb:.2f} MB ({file_size} bytes)")
    
    # Verificar si el archivo destino ya existe
    destination_blob = destination_bucket.blob(destination_blob_name)
    if destination_blob.exists():
        if overwrite:
            if DEBUG:
                print(f"[INFO] El archivo destino ya existe. Se eliminará y se sobrescribirá.")
            destination_blob.delete()
        else:
            if DEBUG:
                print(f"[WARN] El archivo destino ya existe. Se mantendrá el anterior (overwrite=False).")
            gcs_uri = f"gs://{destination_bucket_name}/{destination_blob_name}"
            return gcs_uri
    
    # Copiar entre buckets
    if DEBUG:
        print(f"[INFO] Copiando archivo a: gs://{destination_bucket_name}/{destination_blob_name}")
    
    source_blob_copy = source_bucket.copy_blob(
        blob=source_blob,
        destination_bucket=destination_bucket,
        new_name=destination_blob_name
    )
    
    gcs_uri = f"gs://{destination_bucket_name}/{destination_blob_name}"
    
    if DEBUG:
        print(f"[OK] Archivo copiado exitosamente a: {gcs_uri}")
    
    return gcs_uri

def copy_latest_file_from_tmp_to_sisben(
    bucket_name: str,
    source_folder: str,
    destination_folder: str,
    file_extension: Optional[str] = ".xlsx",
    overwrite: bool = True
) -> str:
    """
    Función completa que encuentra el último archivo en la carpeta temporal
    y lo copia a la carpeta de destino IPM SISBEN.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        source_folder: Carpeta origen (ej: 'data_staging/dpt_planeacion_municipal/tmp')
        destination_folder: Carpeta destino (ej: 'data_staging/dpt_planeacion_municipal/ipm/sisben')
        file_extension: Extensión del archivo a buscar (default: '.xlsx')
        overwrite: Si es True, sobrescribe el archivo si ya existe.
    
    Returns:
        URI completa del archivo destino en GCS (gs://bucket/path)
    """
    if DEBUG:
        print(f"[INFO] Buscando el último archivo en: gs://{bucket_name}/{source_folder}")
    
    # Paso 1: Encontrar el último archivo en la carpeta temporal
    source_blob_name = get_latest_file_from_gcs_folder(
        bucket_name=bucket_name,
        folder_path=source_folder,
        file_extension=file_extension
    )
    
    # Paso 2: Extraer el nombre del archivo (sin la ruta de la carpeta)
    file_name = source_blob_name.split('/')[-1]
    
    # Paso 3: Construir la ruta destino
    destination_blob_name = f"{destination_folder}/{file_name}"
    
    # Paso 4: Copiar el archivo
    gcs_uri = copy_file_within_gcs(
        bucket_name=bucket_name,
        source_blob_name=source_blob_name,
        destination_blob_name=destination_blob_name,
        overwrite=overwrite
    )
    
    return gcs_uri

