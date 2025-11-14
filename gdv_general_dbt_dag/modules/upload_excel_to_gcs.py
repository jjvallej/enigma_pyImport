# modules/upload_excel_to_gcs.py
"""
Módulo para descargar archivos Excel desde Google Drive (públicos o privados) 
y subirlos a Google Cloud Storage.

Soporta:
1. Enlaces públicos de Google Drive (más fácil, sin autenticación)
2. Service Account: Para archivos compartidos con la Service Account
3. OAuth 2.0: Para acceder a archivos personales del usuario
"""
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import os
import tempfile
import io
import requests
import re
from typing import Optional
from urllib.parse import urlparse, parse_qs

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True

# Scope necesario para Google Drive API (solo para métodos autenticados)
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# ---------------------------
# Clientes
# ---------------------------
def _gcs_client() -> storage.Client:
    """Crea un cliente de Google Cloud Storage."""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return storage.Client(project=PROJECT_ID)

def _drive_client_service_account():
    """Crea un cliente de Google Drive API usando Service Account."""
    credentials = service_account.Credentials.from_service_account_file(
        SA_PATH, scopes=SCOPES
    )
    return build('drive', 'v3', credentials=credentials, cache_discovery=False)

# ---------------------------
# Funciones de utilidad
# ---------------------------
def extract_file_id_from_url(drive_url: str) -> str:
    """
    Extrae el File ID de una URL de Google Drive.
    
    Soporta diferentes formatos:
    - https://drive.google.com/file/d/FILE_ID/view
    - https://drive.google.com/open?id=FILE_ID
    - https://docs.google.com/spreadsheets/d/FILE_ID/edit
    - FILE_ID (si ya es solo el ID)
    """
    # Si ya es solo un ID (sin URL)
    if len(drive_url) < 50 and not drive_url.startswith('http'):
        return drive_url.strip()
    
    # Extraer ID de diferentes formatos de URL
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'/spreadsheets/d/([a-zA-Z0-9_-]+)',
        r'/document/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)',
        r'/folders/([a-zA-Z0-9_-]+)',
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

def download_file_from_public_link(drive_url: str, file_name: Optional[str] = None) -> str:
    """
    Descarga un archivo desde Google Drive usando un enlace público.
    
    Args:
        drive_url: URL pública de Google Drive o File ID
        file_name: Nombre opcional para el archivo local
    
    Returns:
        Ruta local del archivo descargado (archivo temporal)
    """
    # Extraer File ID de la URL
    file_id = extract_file_id_from_url(drive_url)
    
    if DEBUG:
        print(f"[DEBUG] File ID extraído: {file_id}")
    
    # Obtener URL de descarga directa
    download_url = get_public_download_url(file_id)
    
    # Para archivos grandes, Google Drive muestra una advertencia primero
    # Necesitamos manejar ese caso
    session = requests.Session()
    response = session.get(download_url, stream=True, allow_redirects=True)
    
    # Si Google Drive muestra la página de advertencia para archivos grandes
    if response.headers.get('Content-Type', '').startswith('text/html'):
        # Buscar el link de descarga real en la página
        content = response.text
        match = re.search(r'href="(/uc\?export=download[^"]+)"', content)
        if match:
            download_url = 'https://drive.google.com' + match.group(1).replace('&amp;', '&')
            response = session.get(download_url, stream=True, allow_redirects=True)
    
    response.raise_for_status()
    
    # Determinar nombre del archivo
    if not file_name:
        # Intentar obtener el nombre del Content-Disposition header
        content_disposition = response.headers.get('Content-Disposition', '')
        if 'filename=' in content_disposition:
            file_name = re.findall(r'filename="?([^"]+)"?', content_disposition)[0]
        else:
            file_name = f"archivo_{file_id}.xlsx"
    
    # Crear archivo temporal
    fd, tmp_path = tempfile.mkstemp(suffix=f"_{file_name}")
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
    
    return tmp_path

def download_file_from_drive_api(
    file_id: str, 
    file_name: Optional[str] = None,
    use_service_account: bool = True
) -> str:
    """
    Descarga un archivo desde Google Drive usando la API (requiere autenticación).
    
    Args:
        file_id: ID del archivo en Google Drive
        file_name: Nombre opcional para el archivo local
        use_service_account: Si es True, usa Service Account; si False, requeriría OAuth
    
    Returns:
        Ruta local del archivo descargado (archivo temporal)
    """
    if not use_service_account:
        raise NotImplementedError("OAuth 2.0 no está implementado en esta versión. Usa enlace público o Service Account.")
    
    drive_service = _drive_client_service_account()
    
    # Obtener metadatos del archivo
    file_metadata = drive_service.files().get(fileId=file_id, fields='name, mimeType').execute()
    original_name = file_metadata.get('name', 'downloaded_file')
    
    # Determinar nombre del archivo
    if not file_name:
        file_name = original_name
    
    # Asegurar extensión correcta
    if not file_name.endswith(('.xlsx', '.xls')):
        mime_type = file_metadata.get('mimeType', '')
        if 'excel' in mime_type.lower() or 'spreadsheet' in mime_type.lower():
            if not file_name.endswith('.xlsx'):
                file_name = file_name.rsplit('.', 1)[0] + '.xlsx'
    
    # Descargar el archivo
    request = drive_service.files().get_media(fileId=file_id)
    
    # Crear archivo temporal
    fd, tmp_path = tempfile.mkstemp(suffix=f"_{file_name}")
    os.close(fd)
    
    # Descargar a archivo local
    fh = io.FileIO(tmp_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        if DEBUG:
            print(f"[DEBUG] Descargando: {int(status.progress() * 100)}%")
    
    fh.close()
    
    if DEBUG:
        print(f"[OK] Archivo descargado desde Drive API: {tmp_path} (original: {original_name})")
    
    return tmp_path

# ---------------------------
# Funciones principales
# ---------------------------
def download_file_from_drive(
    drive_url_or_id: str, 
    file_name: Optional[str] = None,
    use_public_link: bool = True
) -> str:
    """
    Descarga un archivo desde Google Drive.
    
    Args:
        drive_url_or_id: URL pública de Google Drive, File ID, o enlace compartido
        file_name: Nombre opcional para el archivo local
        use_public_link: Si es True, usa enlace público (más fácil).
                        Si es False, usa API con Service Account (requiere compartir archivo)
    
    Returns:
        Ruta local del archivo descargado (archivo temporal)
    """
    if use_public_link:
        return download_file_from_public_link(drive_url_or_id, file_name)
    else:
        file_id = extract_file_id_from_url(drive_url_or_id)
        return download_file_from_drive_api(file_id, file_name, use_service_account=True)

def upload_file_to_gcs(
    local_file_path: str,
    bucket_name: str,
    destination_blob_name: str
) -> str:
    """
    Sube un archivo local a Google Cloud Storage.
    Si la carpeta ya existe, usa esa carpeta. Si no existe, la crea automáticamente.
    
    Args:
        local_file_path: Ruta local del archivo a subir
        bucket_name: Nombre del bucket en GCS
        destination_blob_name: Nombre del blob (ruta) en GCS (ej: 'data_staging/dpt_planeacion_municipal/ipm/archivo.xlsx')
    
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
    
    # Subir el archivo (GCS crea automáticamente la "carpeta" si no existe)
    blob.upload_from_filename(local_file_path)
    
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    
    if DEBUG:
        print(f"[OK] Archivo subido a GCS: {gcs_uri}")
    
    return gcs_uri

def move_file_from_drive_to_gcs(
    drive_url_or_id: str,
    bucket_name: str,
    folder_name: str = "IPM",
    destination_file_name: Optional[str] = None,
    use_public_link: bool = True
) -> str:
    """
    Función completa que descarga un archivo desde Google Drive y lo sube a GCS.
    
    Args:
        drive_url_or_id: URL pública de Google Drive, File ID, o enlace compartido
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket (default: "IPM")
        destination_file_name: Nombre opcional para el archivo en GCS. 
                               Si no se proporciona, se usa el nombre original.
        use_public_link: Si es True, usa enlace público (sin autenticación).
                        Si es False, usa API con Service Account (requiere compartir archivo)
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/folder/file.xlsx)
    """
    # Paso 1: Descargar desde Drive
    local_file_path = download_file_from_drive(
        drive_url_or_id, 
        destination_file_name,
        use_public_link=use_public_link
    )
    
    try:
        # Paso 2: Determinar nombre del archivo
        if not destination_file_name:
            # Usar el nombre del archivo descargado (sin el path temporal)
            downloaded_name = os.path.basename(local_file_path)
            # Remover prefijo temporal si existe
            if '_' in downloaded_name:
                parts = downloaded_name.split('_', 1)
                destination_file_name = parts[-1] if len(parts) > 1 else downloaded_name
            else:
                destination_file_name = downloaded_name
        
        # Paso 3: Construir ruta destino en GCS (folder/file.xlsx)
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

