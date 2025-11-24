# modules/idc_dictionary_extract.py
"""
Módulo para descargar archivos CSV desde Google Drive usando enlaces públicos
y subirlos a Google Cloud Storage.

Soporta:
- Enlaces públicos de Google Drive (sin autenticación)
- Archivos CSV
"""
from google.cloud import storage
import os
import tempfile
import requests
import re
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
def extract_file_id_from_url(drive_url: str) -> str:
    """
    Extrae el File ID de una URL de Google Drive.
    
    Soporta diferentes formatos:
    - https://drive.google.com/file/d/FILE_ID/view
    - https://drive.google.com/open?id=FILE_ID
    - FILE_ID (si ya es solo el ID)
    """
    # Si ya es solo un ID (sin URL)
    if len(drive_url) < 50 and not drive_url.startswith('http'):
        return drive_url.strip()
    
    # Extraer ID de diferentes formatos de URL
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, drive_url)
        if match:
            return match.group(1)
    
    raise ValueError(f"No se pudo extraer el File ID de la URL: {drive_url}")

def get_public_download_url(file_id: str) -> str:
    """
    Convierte un File ID de Google Drive a un enlace de descarga directa
    para archivos públicos.
    
    Args:
        file_id: ID del archivo en Google Drive
    
    Returns:
        URL de descarga directa
    """
    return f"https://drive.google.com/uc?export=download&id={file_id}"

def download_file_from_public_link(drive_url: str, file_name: Optional[str] = None) -> tuple[str, str]:
    """
    Descarga un archivo CSV desde Google Drive usando un enlace público.
    
    Args:
        drive_url: URL pública de Google Drive o File ID
        file_name: Nombre opcional para el archivo local
    
    Returns:
        Tupla con (ruta local del archivo descargado, nombre original del archivo)
    """
    # Extraer File ID de la URL
    file_id = extract_file_id_from_url(drive_url)
    
    if DEBUG:
        print(f"[DEBUG] File ID extraído: {file_id}")
    
    # Headers para evitar bloqueos
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Intentar descargar
    download_url = get_public_download_url(file_id)
    
    if DEBUG:
        print(f"[DEBUG] Intentando descarga con método: {download_url}")
    
    try:
        response = session.get(download_url, stream=True, allow_redirects=True, headers=headers, timeout=30)
        
        # Si Google Drive muestra la página de advertencia para archivos grandes
        if response.headers.get('Content-Type', '').startswith('text/html'):
            if DEBUG:
                print(f"[DEBUG] Google Drive mostró página HTML, buscando enlace de descarga real...")
            # Buscar el link de descarga real en la página
            content = response.text
            
            # Buscar diferentes patrones de enlaces de descarga
            patterns = [
                r'href="(/uc\?export=download[^"]+)"',
                r'href="(/file/d/[^"]+/[^"]+)"',
                r'id="uc-download-link"[^>]*href="([^"]+)"',
            ]
            
            download_url_found = None
            for pattern in patterns:
                match = re.search(pattern, content)
                if match:
                    download_url_found = match.group(1)
                    if not download_url_found.startswith('http'):
                        download_url_found = 'https://drive.google.com' + download_url_found.replace('&amp;', '&')
                    break
            
            if download_url_found:
                if DEBUG:
                    print(f"[DEBUG] Enlace de descarga encontrado: {download_url_found}")
                response = session.get(download_url_found, stream=True, allow_redirects=True, headers=headers, timeout=30)
        
        # Verificar el estado de la respuesta
        if response.status_code != 200:
            error_msg = f"Error HTTP {response.status_code} al descargar el archivo. "
            if response.status_code == 403:
                error_msg += "El archivo puede no ser público. Verifica que el archivo esté configurado como 'Cualquier persona con el enlace puede ver'."
            elif response.status_code == 404:
                error_msg += "El archivo no fue encontrado. Verifica que la URL sea correcta."
            else:
                error_msg += f"Respuesta: {response.text[:200]}"
            raise Exception(error_msg)
        
        response.raise_for_status()
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al descargar el archivo desde Google Drive: {e}")
    
    # Determinar nombre del archivo ANTES de crear el archivo temporal
    original_file_name = file_name
    if not original_file_name:
        # Intentar obtener el nombre del Content-Disposition header
        content_disposition = response.headers.get('Content-Disposition', '')
        if 'filename=' in content_disposition:
            original_file_name = re.findall(r'filename="?([^"]+)"?', content_disposition)[0]
            # Limpiar el nombre del archivo (remover caracteres problemáticos)
            original_file_name = original_file_name.strip().replace('\n', '').replace('\r', '')
        else:
            original_file_name = f"archivo_{file_id}.csv"
    
    # Crear archivo temporal con extensión CSV
    file_ext = os.path.splitext(original_file_name)[1] or '.csv'
    fd, tmp_path = tempfile.mkstemp(suffix=file_ext)
    os.close(fd)
    
    # Descargar el archivo
    total_size = int(response.headers.get('Content-Length', 0))
    downloaded = 0
    
    with open(tmp_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if DEBUG and total_size > 0:
                    progress = int((downloaded / total_size) * 100)
                    if progress % 25 == 0:  # Mostrar cada 25%
                        print(f"[DEBUG] Descargando: {progress}%")
    
    if DEBUG:
        print(f"[OK] Archivo descargado desde enlace público: {tmp_path}")
        print(f"[DEBUG] Nombre original del archivo: {original_file_name}")
    
    # Retornar tanto la ruta temporal como el nombre original
    return tmp_path, original_file_name

# ---------------------------
# Funciones principales
# ---------------------------
def download_file_from_drive(
    drive_url_or_id: str, 
    file_name: Optional[str] = None
) -> tuple[str, str]:
    """
    Descarga un archivo CSV desde Google Drive usando un enlace público.
    
    Args:
        drive_url_or_id: URL pública de Google Drive o File ID
        file_name: Nombre opcional para el archivo local
    
    Returns:
        Tupla con (ruta local del archivo descargado, nombre original del archivo)
    """
    return download_file_from_public_link(drive_url_or_id, file_name)

def upload_file_to_gcs(
    local_file_path: str,
    bucket_name: str,
    destination_blob_name: str,
    overwrite: bool = True
) -> str:
    """
    Sube un archivo local a Google Cloud Storage.
    Si la carpeta ya existe, usa esa carpeta. Si no existe, la crea automáticamente.
    Si el archivo ya existe, lo sobrescribe (elimina el anterior y sube el nuevo).
    
    Args:
        local_file_path: Ruta local del archivo a subir
        bucket_name: Nombre del bucket en GCS
        destination_blob_name: Nombre del blob (ruta) en GCS (ej: 'data_staging/dpt_planeacion_municipal/idc/archivo.csv')
        overwrite: Si es True, sobrescribe el archivo si ya existe. Si es False, mantiene el anterior.
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/path)
    """
    gcs_client = _gcs_client()
    
    # Obtener el bucket
    try:
        bucket = gcs_client.bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder al bucket '{bucket_name}': {e}")
    
    # Normalizar la ruta: eliminar barras dobles y asegurar formato consistente
    destination_blob_name = '/'.join(part.strip('/') for part in destination_blob_name.split('/') if part.strip('/'))
    
    # Verificar si existe algún objeto en esa carpeta (opcional, solo para logging)
    folder_path = '/'.join(destination_blob_name.split('/')[:-1]) if '/' in destination_blob_name else ''
    if folder_path and DEBUG:
        # Listar objetos en esa carpeta para verificar si existe
        blobs = list(bucket.list_blobs(prefix=folder_path + '/', max_results=1))
        if blobs:
            print(f"[INFO] Carpeta '{folder_path}' ya existe. Usando carpeta existente.")
        else:
            print(f"[INFO] Carpeta '{folder_path}' no existe. Se creará automáticamente al subir el archivo.")
    
    blob = bucket.blob(destination_blob_name)
    
    # Verificar si el archivo ya existe
    if blob.exists():
        if overwrite:
            if DEBUG:
                print(f"[INFO] El archivo '{destination_blob_name}' ya existe. Se eliminará y se sobrescribirá con el nuevo.")
            # Eliminar el archivo existente
            blob.delete()
            if DEBUG:
                print(f"[INFO] Archivo anterior eliminado.")
        else:
            if DEBUG:
                print(f"[WARN] El archivo '{destination_blob_name}' ya existe. Se mantendrá el anterior (overwrite=False).")
    
    # Subir el archivo (GCS crea automáticamente la "carpeta" si no existe)
    blob.upload_from_filename(local_file_path)
    
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    
    if DEBUG:
        print(f"[OK] Archivo subido a GCS: {gcs_uri}")
    
    return gcs_uri

def move_file_from_drive_to_gcs(
    drive_url_or_id: str,
    bucket_name: str,
    folder_name: str = "idc",
    destination_file_name: Optional[str] = None
) -> str:
    """
    Función completa que descarga un archivo CSV desde Google Drive (enlace público) y lo sube a GCS.
    
    Args:
        drive_url_or_id: URL pública de Google Drive o File ID
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket (default: "idc")
        destination_file_name: Nombre opcional para el archivo en GCS. 
                               Si no se proporciona, se usa el nombre original.
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/folder/file.csv)
    """
    # Paso 1: Descargar desde Drive (retorna tupla: path, nombre_original)
    local_file_path, original_file_name = download_file_from_drive(
        drive_url_or_id, 
        destination_file_name
    )
    
    try:
        # Paso 2: Determinar nombre del archivo
        if not destination_file_name:
            # Usar el nombre original del archivo extraído de Drive
            destination_file_name = original_file_name
        
        # Paso 3: Construir ruta destino en GCS (folder/file.csv)
        # Normalizar rutas: eliminar barras duplicadas y espacios
        folder_name = folder_name.strip('/')
        destination_file_name = destination_file_name.strip('/')
        destination_blob_name = f"{folder_name}/{destination_file_name}"
        
        # Paso 4: Subir a GCS
        gcs_uri = upload_file_to_gcs(
            local_file_path=local_file_path,
            bucket_name=bucket_name,
            destination_blob_name=destination_blob_name
        )
        
        return gcs_uri
        
    finally:
        # Limpiar archivo temporal
        try:
            os.unlink(local_file_path)
            if DEBUG:
                print(f"[DEBUG] Archivo temporal eliminado: {local_file_path}")
        except Exception as e:
            print(f"[WARN] No se pudo eliminar archivo temporal {local_file_path}: {e}")

