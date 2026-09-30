# modules/ipm/ipm_sisben_ingest.py
"""
Módulo para ingerir archivos Excel desde Google Drive usando enlaces públicos
y subirlos a Google Cloud Storage para IPM SISBEN.

Soporta:
- Enlaces públicos de Google Drive (sin autenticación)
- API de Google Drive con autenticación (para archivos grandes)
"""
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import requests
import re
import os
import tempfile
import io
from datetime import datetime
from typing import Optional
from modules.config import PROJECT_ID, DEFAULT_BUCKET_NAME, CONF
from modules.gcp_utils import get_gcs_client

DEBUG = CONF.global_config.debug

def extract_file_id_from_url(drive_url):
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

def get_public_download_url(file_id: str, is_google_sheet: bool = False) -> str:
    """
    Convierte un File ID de Google Drive a un enlace de descarga directa
    para archivos públicos.
    
    Args:
        file_id: ID del archivo en Google Drive
        is_google_sheet: Si es True, usa el formato de exportación para Google Sheets
    
    Returns:
        URL de descarga directa
    """
    if is_google_sheet:
        # Para Google Sheets, usar formato de exportación Excel
        return f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
    else:
        return f"https://drive.google.com/uc?export=download&id={file_id}"

def download_file_from_public_link(drive_url: str, file_name: Optional[str] = None) -> tuple[str, str]:
    """
    Descarga un archivo desde Google Drive usando un enlace público.
    Soporta tanto archivos Excel (.xlsx) como Google Sheets.
    
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
    
    # Detectar si es un Google Sheet basándose en la URL
    is_google_sheet = '/spreadsheets/d/' in drive_url or '/spreadsheets/' in drive_url
    
    if DEBUG:
        print(f"[DEBUG] Tipo detectado: {'Google Sheet' if is_google_sheet else 'Archivo Excel/Genérico'}")
    
    # Headers para evitar bloqueos
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    # Intentar descargar con el método detectado primero
    download_url = get_public_download_url(file_id, is_google_sheet=is_google_sheet)
    
    if DEBUG:
        print(f"[DEBUG] Intentando descarga con método: {download_url}")
    
    try:
        # Timeout más largo para archivos grandes (10 minutos)
        response = session.get(download_url, stream=True, allow_redirects=True, headers=headers, timeout=600)
        
        # Si obtenemos un error 500 o 403, y no habíamos detectado como Google Sheet,
        # intentar con el método de Google Sheets
        if response.status_code in [403, 500] and not is_google_sheet:
            if DEBUG:
                print(f"[DEBUG] Error {response.status_code} con método genérico. Intentando como Google Sheet...")
            # Intentar como Google Sheet
            sheet_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
            response = session.get(sheet_url, stream=True, allow_redirects=True, headers=headers, timeout=240)
            is_google_sheet = True
        
        # Si Google Drive muestra la página de advertencia para archivos grandes
        if response.headers.get('Content-Type', '').startswith('text/html'):
            if DEBUG:
                print(f"[DEBUG] Google Drive mostró página HTML, buscando enlace de descarga real...")
                print(f"[DEBUG] Status code: {response.status_code}")
                print(f"[DEBUG] URL actual: {response.url}")
            
            # Buscar el link de descarga real en la página
            content = response.text
            
            # Para Google Sheets grandes, intentar método alternativo primero
            if is_google_sheet:
                # Método alternativo: usar el formato de exportación con confirmación
                alt_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx&confirm=t"
                if DEBUG:
                    print(f"[DEBUG] Intentando método alternativo para Google Sheet: {alt_url}")
                try:
                    alt_response = session.get(alt_url, stream=True, allow_redirects=True, headers=headers, timeout=600)
                    if alt_response.headers.get('Content-Type', '').startswith('application/'):
                        if DEBUG:
                            print(f"[DEBUG] Método alternativo funcionó!")
                        response = alt_response
                    else:
                        if DEBUG:
                            print(f"[DEBUG] Método alternativo también devolvió HTML, parseando...")
                except Exception as e:
                    if DEBUG:
                        print(f"[DEBUG] Error con método alternativo: {e}, continuando con parsing...")
            
            # Si aún es HTML, parsear la página
            if response.headers.get('Content-Type', '').startswith('text/html'):
                # Buscar diferentes patrones de enlaces de descarga (más exhaustivos)
                patterns = [
                    r'href="(/uc\?export=download[^"]+)"',
                    r'href="(/file/d/[^"]+/[^"]+)"',
                    r'id="uc-download-link"[^>]*href="([^"]+)"',
                    r'href="(https://drive\.google\.com/uc\?export=download[^"]+)"',
                    r'href="(https://drive\.google\.com/file/d/[^"]+)"',
                    r'action="([^"]*export=download[^"]*)"',
                    r'data-url="([^"]+)"',
                    r'data-src="([^"]+)"',
                    r'window\.location\.href\s*=\s*["\']([^"\']+)["\']',
                    r'location\.href\s*=\s*["\']([^"\']+)["\']',
                ]
                
                download_url_found = None
                for pattern in patterns:
                    matches = re.findall(pattern, content)
                    for match in matches:
                        if match and ('export' in match.lower() or 'download' in match.lower() or 'file/d' in match):
                            download_url_found = match
                            if not download_url_found.startswith('http'):
                                download_url_found = 'https://drive.google.com' + download_url_found.replace('&amp;', '&')
                            break
                    if download_url_found:
                        break
                
                # Si no se encontró con patrones, buscar en formularios
                if not download_url_found:
                    # Buscar formularios con action que contenga export o download
                    form_pattern = r'<form[^>]*action="([^"]*export[^"]*)"[^>]*>'
                    form_match = re.search(form_pattern, content, re.IGNORECASE)
                    if form_match:
                        download_url_found = form_match.group(1)
                        if not download_url_found.startswith('http'):
                            download_url_found = 'https://drive.google.com' + download_url_found.replace('&amp;', '&')
                
                if download_url_found:
                    if DEBUG:
                        print(f"[DEBUG] Enlace de descarga encontrado: {download_url_found}")
                    # Timeout de 10 minutos para archivos grandes
                    response = session.get(download_url_found, stream=True, allow_redirects=True, headers=headers, timeout=600)
                else:
                    # Último intento: usar el método directo con confirmación
                    if is_google_sheet:
                        direct_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx&confirm=t&id={file_id}"
                        if DEBUG:
                            print(f"[DEBUG] Último intento con URL directa: {direct_url}")
                        try:
                            response = session.get(direct_url, stream=True, allow_redirects=True, headers=headers, timeout=600)
                            if response.headers.get('Content-Type', '').startswith('text/html'):
                                raise Exception("Google Drive sigue devolviendo HTML después de todos los intentos")
                        except Exception as e:
                            if DEBUG:
                                print(f"[DEBUG] Error con método directo: {e}")
                            raise Exception(f"No se pudo encontrar el enlace de descarga en la página de Google Drive. El archivo puede ser muy grande (1.5GB) y requerir descarga manual o autenticación. Error: {e}")
                    else:
                        raise Exception("No se pudo encontrar el enlace de descarga en la página de Google Drive")
        
        response.raise_for_status()
        
        # Determinar nombre del archivo
        if not file_name:
            # Intentar obtener el nombre del archivo desde los headers
            content_disposition = response.headers.get('Content-Disposition', '')
            if content_disposition:
                filename_match = re.search(r'filename[^;=\n]*=(([\'"]).*?\2|[^;\n]*)', content_disposition)
                if filename_match:
                    file_name = filename_match.group(1).strip('\'"')
                    # Decodificar si está codificado
                    if file_name.startswith("UTF-8''"):
                        file_name = file_name[7:]
            
            # Si no se pudo obtener del header, usar un nombre por defecto
            if not file_name:
                file_name = f"ipm_sisben_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Verificar que realmente tenemos un archivo, no HTML
        content_type = response.headers.get('Content-Type', '')
        if content_type.startswith('text/html'):
            raise Exception(f"Google Drive devolvió HTML en lugar del archivo. Content-Type: {content_type}. El archivo puede ser muy grande y requerir descarga manual.")
        
        # Crear archivo temporal
        fd, tmp_path = tempfile.mkstemp(suffix='.xlsx')
        os.close(fd)
        
        # Descargar el archivo con chunks más grandes para archivos grandes
        chunk_size = 1024 * 1024  # 1MB por chunk para archivos grandes
        total_size = 0
        
        if DEBUG:
            content_length = response.headers.get('Content-Length')
            if content_length:
                print(f"[DEBUG] Tamaño esperado del archivo: {int(content_length) / (1024*1024):.2f} MB")
        
        with open(tmp_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
                    if DEBUG and total_size % (10 * 1024 * 1024) == 0:  # Log cada 10MB
                        print(f"[DEBUG] Descargados {total_size / (1024*1024):.2f} MB...")
        
        if DEBUG:
            file_size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
            print(f"[OK] Archivo descargado: {file_name} ({file_size_mb:.2f} MB / {os.path.getsize(tmp_path)} bytes)")
        
        return tmp_path, file_name
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al descargar archivo desde Google Drive: {e}")

def download_file_from_drive_api(file_id: str, file_name: Optional[str] = None) -> tuple[str, str]:
    """
    Descarga un archivo desde Google Drive usando la API de Google Drive con autenticación.
    Útil para archivos grandes que requieren autenticación.
    
    Args:
        file_id: File ID de Google Drive
        file_name: Nombre opcional para el archivo local
    
    Returns:
        Tupla con (ruta local del archivo descargado, nombre original del archivo)
    """
    try:
        from google.auth import default
        from google.auth.exceptions import DefaultCredentialsError
        
        # Intentar obtener credenciales usando ADC (Application Default Credentials)
        try:
            credentials, project = default()
            if DEBUG:
                print(f"[DEBUG] Usando credenciales ADC para Google Drive API")
        except DefaultCredentialsError:
            raise Exception("No se encontraron credenciales de Google Cloud. La API de Google Drive requiere autenticación.")
        
        # Construir el servicio de Google Drive
        service = build('drive', 'v3', credentials=credentials)
        
        # Obtener metadatos del archivo
        try:
            file_metadata = service.files().get(fileId=file_id, fields='name, size, mimeType').execute()
        except Exception as e:
            error_msg = str(e).lower()
            if 'not found' in error_msg or 'permission denied' in error_msg or 'insufficient permissions' in error_msg:
                raise Exception(
                    f"El archivo no está accesible con las credenciales actuales. "
                    f"Para archivos grandes (1.5GB), necesitas compartir el archivo de Google Drive "
                    f"con la service account de Composer. "
                    f"Error: {e}"
                )
            raise
        
        if not file_name:
            file_name = file_metadata.get('name', f"ipm_sisben_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
        
        file_size = int(file_metadata.get('size', 0))
        if DEBUG:
            print(f"[DEBUG] Descargando archivo: {file_name} ({file_size / (1024*1024):.2f} MB) usando Google Drive API")
        
        # Para Google Sheets, usar el formato de exportación
        mime_type = file_metadata.get('mimeType', '')
        if 'spreadsheet' in mime_type.lower():
            request = service.files().export_media(fileId=file_id, mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            request = service.files().get_media(fileId=file_id)
        
        # Crear archivo temporal
        fd, tmp_path = tempfile.mkstemp(suffix='.xlsx')
        os.close(fd)
        
        # Descargar el archivo
        with open(tmp_path, 'wb') as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False
            total_downloaded = 0
            
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    total_downloaded = int(status.progress() * file_size) if file_size > 0 else 0
                    if DEBUG and total_downloaded % (10 * 1024 * 1024) == 0:  # Log cada 10MB
                        print(f"[DEBUG] Descargados {total_downloaded / (1024*1024):.2f} MB...")
        
        if DEBUG:
            file_size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
            print(f"[OK] Archivo descargado usando API: {file_name} ({file_size_mb:.2f} MB)")
        
        return tmp_path, file_name
        
    except Exception as e:
        raise Exception(f"Error al descargar archivo desde Google Drive usando API: {e}")

def download_file_from_drive(
    drive_url_or_id: str, 
    file_name: Optional[str] = None
) -> tuple[str, str]:
    """
    Descarga un archivo desde Google Drive.
    Primero intenta con la API de Google Drive (requiere autenticación, funciona con archivos grandes).
    Si falla, intenta con el método de enlace público.
    
    Args:
        drive_url_or_id: URL pública de Google Drive o File ID
        file_name: Nombre opcional para el archivo local
    
    Returns:
        Tupla con (ruta local del archivo descargado, nombre original del archivo)
    """
    # Extraer File ID
    file_id = extract_file_id_from_url(drive_url_or_id)
    
    # Primero intentar con la API de Google Drive (mejor para archivos grandes)
    try:
        if DEBUG:
            print(f"[DEBUG] Intentando descarga usando Google Drive API...")
        return download_file_from_drive_api(file_id, file_name)
    except Exception as api_error:
        if DEBUG:
            print(f"[DEBUG] Error con Google Drive API: {api_error}")
            print(f"[DEBUG] Intentando método de enlace público como fallback...")
        # Si falla la API, intentar con el método público
        try:
            return download_file_from_public_link(drive_url_or_id, file_name)
        except Exception as public_error:
            raise Exception(
                f"No se pudo descargar el archivo. "
                f"Error con API: {api_error}. "
                f"Error con método público: {public_error}. "
                f"El archivo puede requerir permisos especiales o ser demasiado grande."
            )

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
        destination_blob_name: Nombre del blob (ruta) en GCS (ej: 'data_staging/dpt_planeacion_municipal/ipm/sisben/archivo.xlsx')
        overwrite: Si es True, sobrescribe el archivo si ya existe. Si es False, mantiene el anterior.
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/path)
    """
    gcs_client = get_gcs_client()
    
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

def move_file_within_gcs(
    bucket_name: str,
    source_folder: str,
    destination_folder: str,
    file_pattern: Optional[str] = None
) -> str:
    """
    Mueve un archivo dentro del mismo bucket de GCS desde una carpeta origen a una carpeta destino.
    Si hay múltiples archivos, mueve el más reciente.
    
    Args:
        bucket_name: Nombre del bucket en GCS
        source_folder: Carpeta origen (ej: "data_staging/dpt_planeacion_municipal/temp")
        destination_folder: Carpeta destino (ej: "data_staging/dpt_planeacion_municipal/ipm/sisben")
        file_pattern: Patrón opcional para filtrar archivos (ej: "*.xlsx"). Si es None, toma cualquier archivo.
    
    Returns:
        URI completa del archivo movido en GCS (gs://bucket/destination_folder/file.xlsx)
    """
    gcs_client = get_gcs_client()
    
    try:
        bucket = gcs_client.bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder al bucket '{bucket_name}': {e}")
    
    # Normalizar rutas
    source_folder = source_folder.strip('/')
    destination_folder = destination_folder.strip('/')
    
    # Buscar archivos en la carpeta origen
    prefix = f"{source_folder}/" if source_folder else ""
    blobs = list(bucket.list_blobs(prefix=prefix))
    
    if not blobs:
        raise FileNotFoundError(f"No se encontraron archivos en la carpeta '{source_folder}' del bucket '{bucket_name}'")
    
    # Filtrar por patrón si se especifica
    if file_pattern:
        import fnmatch
        # Filtrar solo por el nombre del archivo, no por la ruta completa
        blobs = [blob for blob in blobs if fnmatch.fnmatch(blob.name.split('/')[-1], file_pattern)]
        if not blobs:
            raise FileNotFoundError(f"No se encontraron archivos que coincidan con el patrón '{file_pattern}' en '{source_folder}'")
    
    # Encontrar el archivo más reciente (por tiempo de actualización)
    latest_blob = max(blobs, key=lambda b: b.updated)
    
    if DEBUG:
        print(f"[INFO] Archivo encontrado: {latest_blob.name}")
        print(f"[INFO] Tamaño: {latest_blob.size / (1024*1024):.2f} MB")
        print(f"[INFO] Última actualización: {latest_blob.updated}")
    
    # Extraer nombre del archivo
    file_name = latest_blob.name.split('/')[-1]
    
    # Construir ruta destino
    destination_blob_name = f"{destination_folder}/{file_name}"
    
    # Copiar el archivo a la nueva ubicación
    source_blob = bucket.blob(latest_blob.name)
    destination_blob = bucket.blob(destination_blob_name)
    
    if DEBUG:
        print(f"[INFO] Moviendo archivo de '{latest_blob.name}' a '{destination_blob_name}'")
    
    # Copiar el blob
    bucket.copy_blob(source_blob, bucket, destination_blob_name)
    
    # Eliminar el archivo original
    source_blob.delete()
    
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    
    if DEBUG:
        print(f"[OK] Archivo movido exitosamente a: {gcs_uri}")
    
    return gcs_uri

def move_file_from_drive_to_gcs(
    drive_url_or_id: str,
    bucket_name: str,
    folder_name: str = "data_staging/dpt_planeacion_municipal/ipm/sisben",
    destination_file_name: Optional[str] = None
) -> str:
    """
    Función completa que ingiere un archivo desde Google Drive (enlace público) y lo sube a GCS.
    
    Args:
        drive_url_or_id: URL pública de Google Drive o File ID
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket (default: "data_staging/dpt_planeacion_municipal/ipm/sisben")
        destination_file_name: Nombre opcional para el archivo en GCS. 
                               Si no se proporciona, se usa el nombre original.
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/folder/file.xlsx)
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

