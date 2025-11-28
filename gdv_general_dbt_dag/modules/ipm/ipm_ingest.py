# modules/ipm_ingest.py
"""
Módulo para ingerir archivos Excel desde Google Drive usando enlaces públicos
y subirlos a Google Cloud Storage.

Soporta:
- Enlaces públicos de Google Drive (sin autenticación)
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
        # Formato correcto: /export?format=xlsx (sin parámetros adicionales)
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
    
    # Nota: Una URL con '/spreadsheets/d/' puede ser tanto un Google Sheet nativo
    # como un archivo Excel subido a Google Drive. Intentaremos ambos métodos.
    # Primero intentamos como archivo normal, luego como Google Sheet si falla.
    
    # Headers para evitar bloqueos
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Intentar primero como archivo normal de Google Drive (método más común para archivos Excel subidos)
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    is_google_sheet = False
    
    if DEBUG:
        print(f"[DEBUG] Intentando descarga como archivo normal de Google Drive: {download_url}")
    
    try:
        response = session.get(download_url, stream=True, allow_redirects=True, headers=headers, timeout=30)
        
        # Si Google Drive muestra la página de advertencia para archivos grandes o HTML
        content_type = response.headers.get('Content-Type', '')
        if content_type.startswith('text/html'):
            if DEBUG:
                print(f"[DEBUG] Google Drive mostró página HTML, buscando enlace de descarga real...")
            # Buscar el link de descarga real en la página
            content = response.text
            
            # Buscar diferentes patrones de enlaces de descarga
            patterns = [
                r'href="(/uc\?export=download[^"]+)"',
                r'href="(/file/d/[^"]+/[^"]+)"',
                r'id="uc-download-link"[^>]*href="([^"]+)"',
                r'href="([^"]*uc\?[^"]*export=download[^"]*)"',
                r'action="([^"]*uc\?[^"]*export=download[^"]*)"',
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
                    print(f"[DEBUG] Enlace de descarga encontrado en HTML: {download_url_found}")
                response = session.get(download_url_found, stream=True, allow_redirects=True, headers=headers, timeout=30)
            else:
                # Si no encontramos el enlace, intentar método alternativo: Google Sheets export
                if DEBUG:
                    print(f"[DEBUG] No se encontró enlace en HTML. Intentando método de Google Sheets export...")
                sheet_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
                response = session.get(sheet_url, stream=True, allow_redirects=True, headers=headers, timeout=30)
                is_google_sheet = True
                
                # Si aún devuelve HTML, intentar con confirmación de descarga
                if response.headers.get('Content-Type', '').startswith('text/html'):
                    if DEBUG:
                        print(f"[DEBUG] Google Sheets export también devolvió HTML. Intentando descarga directa con confirmación...")
                    # Método alternativo: usar el endpoint de confirmación con parámetro para archivos grandes
                    # Para archivos grandes, Google Drive requiere un token de confirmación
                    confirm_url = f"https://drive.google.com/uc?export=download&confirm=t&id={file_id}"
                    response = session.get(confirm_url, stream=True, allow_redirects=True, headers=headers, timeout=300)
                    
                    # Si aún devuelve HTML, extraer el token de confirmación de la página
                    if response.headers.get('Content-Type', '').startswith('text/html'):
                        content = response.text
                        # Buscar el token de confirmación en la página HTML
                        token_match = re.search(r'id="downloadForm".*?action="([^"]+)"', content, re.DOTALL)
                        if token_match:
                            download_action = token_match.group(1)
                            if not download_action.startswith('http'):
                                download_action = 'https://drive.google.com' + download_action
                            if DEBUG:
                                print(f"[DEBUG] Token de confirmación encontrado. URL: {download_action}")
                            response = session.get(download_action, stream=True, allow_redirects=True, headers=headers, timeout=300)
        
        # Verificar el estado de la respuesta después de todos los intentos
        # Si aún devuelve HTML, intentar un último método: descarga directa con confirmación y token
        content_type_after = response.headers.get('Content-Type', '')
        if content_type_after.startswith('text/html') and response.status_code not in [200, 302]:
            if DEBUG:
                print(f"[DEBUG] Último intento: descarga directa con confirmación y extracción de token...")
            
            # Leer el contenido HTML para extraer el token
            content = response.text
            
            # Buscar diferentes patrones para el token de confirmación
            token_patterns = [
                r'id="downloadForm".*?action="([^"]+)"',
                r'href="(/uc\?export=download[^"]*confirm=[^"]*)"',
                r'action="(/uc\?export=download[^"]*)"',
            ]
            
            download_url_with_token = None
            for pattern in token_patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    download_url_with_token = match.group(1)
                    if not download_url_with_token.startswith('http'):
                        download_url_with_token = 'https://drive.google.com' + download_url_with_token.replace('&amp;', '&')
                    break
            
            if download_url_with_token:
                if DEBUG:
                    print(f"[DEBUG] Token encontrado. Intentando descarga con: {download_url_with_token[:100]}...")
                response = session.get(download_url_with_token, stream=True, allow_redirects=True, headers=headers, timeout=300)
            else:
                # Último intento: usar el método de confirmación simple
                confirm_url = f"https://drive.google.com/uc?export=download&confirm=t&id={file_id}"
                response = session.get(confirm_url, stream=True, allow_redirects=True, headers=headers, timeout=300)
            
            # Si aún devuelve HTML, el archivo probablemente no es público o es demasiado grande
            if response.headers.get('Content-Type', '').startswith('text/html') and response.status_code not in [200, 302]:
                error_msg = f"Error: Google Drive devolvió una página HTML en lugar del archivo (código {response.status_code}).\n"
                error_msg += "Esto puede ocurrir cuando:\n"
                error_msg += "1. El archivo no está configurado como público\n"
                error_msg += "2. El archivo es muy grande (>100MB) y Google Drive requiere autenticación\n"
                error_msg += "3. El archivo requiere permisos especiales\n\n"
                error_msg += "Sugerencias:\n"
                error_msg += "- Verifica que el archivo esté configurado como 'Cualquier persona con el enlace puede ver'\n"
                error_msg += "- Para archivos muy grandes, considera usar la API de Google Drive con autenticación\n"
                if DEBUG:
                    error_msg += f"\n[DEBUG] Contenido de la respuesta (primeros 500 caracteres): {response.text[:500]}"
                raise Exception(error_msg)
        
        if response.status_code not in [200, 302]:
            error_msg = f"Error HTTP {response.status_code} al descargar el archivo. "
            if response.status_code == 403:
                error_msg += "El archivo puede no ser público. Verifica que el archivo esté configurado como 'Cualquier persona con el enlace puede ver'."
            elif response.status_code == 404:
                error_msg += "El archivo no fue encontrado. Verifica que la URL sea correcta."
            elif response.status_code == 500:
                error_msg += "Error del servidor de Google Drive. Esto puede ocurrir si el archivo es muy grande o requiere autenticación. Verifica que el archivo sea público."
            elif response.status_code == 432:
                error_msg += "Error 432: Google Drive está bloqueando la descarga. Verifica que el archivo esté configurado como público y que el enlace sea correcto."
            else:
                error_msg += f"Respuesta: {response.text[:200]}"
            raise Exception(error_msg)
        
        response.raise_for_status()
        
    except requests.exceptions.RequestException as e:
        # Si falla el método detectado, intentar el método alternativo
        if not is_google_sheet:
            if DEBUG:
                print(f"[DEBUG] Error con método genérico: {e}. Intentando como Google Sheet...")
            try:
                sheet_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx&id={file_id}"
                response = session.get(sheet_url, stream=True, allow_redirects=True, headers=headers, timeout=30)
                response.raise_for_status()
                is_google_sheet = True
            except Exception as e2:
                raise Exception(f"Error al descargar el archivo desde Google Drive. Intenté ambos métodos (Excel y Google Sheets) sin éxito. Error final: {e2}")
        else:
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
            original_file_name = f"archivo_{file_id}.xlsx"
    
    # Crear archivo temporal con extensión pero sin prefijo en el nombre
    # Usamos tempfile pero guardamos el nombre original para usarlo después
    file_ext = os.path.splitext(original_file_name)[1] or '.xlsx'
    fd, tmp_path = tempfile.mkstemp(suffix=file_ext)
    os.close(fd)
    
    # Guardar el nombre original en el contexto (se retornará junto con el path)
    # Lo haremos retornando una tupla o modificando la función para retornar ambos
    
    # Verificar que la respuesta no sea HTML antes de descargar
    content_type = response.headers.get('Content-Type', '')
    if content_type.startswith('text/html'):
        # Leer los primeros bytes para verificar
        first_chunk = next(response.iter_content(chunk_size=1024), b'')
        response.close()
        
        # Verificar si es HTML
        if first_chunk.startswith(b'<!DOCTYPE') or first_chunk.startswith(b'<html') or b'<html' in first_chunk[:500]:
            error_msg = f"Error: Google Drive devolvió una página HTML en lugar del archivo Excel.\n"
            error_msg += f"Esto generalmente ocurre cuando:\n"
            error_msg += f"1. El archivo no está configurado como público\n"
            error_msg += f"2. El archivo es muy grande y Google Drive requiere confirmación\n"
            error_msg += f"3. El archivo requiere autenticación\n\n"
            error_msg += f"Verifica que el archivo esté configurado como 'Cualquier persona con el enlace puede ver' en Google Drive.\n"
            if DEBUG:
                error_msg += f"\n[DEBUG] Primeros 500 caracteres de la respuesta:\n{first_chunk[:500].decode('utf-8', errors='ignore')}"
            raise Exception(error_msg)
    
    # Reiniciar la descarga si ya leímos el primer chunk
    if 'first_chunk' in locals():
        response = session.get(response.url, stream=True, allow_redirects=True, headers=headers, timeout=300)  # Timeout aumentado para archivos grandes
    
    # Descargar el archivo
    total_size = int(response.headers.get('Content-Length', 0))
    downloaded = 0
    file_size_mb = total_size / (1024 * 1024) if total_size > 0 else 0
    
    if DEBUG:
        if total_size > 0:
            print(f"[DEBUG] Iniciando descarga de archivo de {file_size_mb:.2f} MB ({total_size} bytes)")
        else:
            print(f"[DEBUG] Iniciando descarga (tamaño desconocido)")
    
    with open(tmp_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if DEBUG and total_size > 0:
                    progress = int((downloaded / total_size) * 100)
                    if progress % 25 == 0:  # Mostrar cada 25%
                        print(f"[DEBUG] Descargando: {progress}% ({downloaded / (1024*1024):.2f} MB / {file_size_mb:.2f} MB)")
    
    # Verificar que el archivo descargado sea válido
    downloaded_size = os.path.getsize(tmp_path)
    if DEBUG:
        print(f"[DEBUG] Archivo descargado: {downloaded_size} bytes ({downloaded_size / (1024*1024):.2f} MB)")
    
    # Verificar que el archivo no sea HTML
    with open(tmp_path, 'rb') as f:
        first_bytes = f.read(8)
        if first_bytes[:2] != b'PK':
            # Verificar si es HTML
            f.seek(0)
            first_chunk = f.read(1024)
            if b'<!DOCTYPE' in first_chunk or b'<html' in first_chunk:
                error_msg = f"Error: El archivo descargado es HTML, no un archivo Excel.\n"
                error_msg += f"Tamaño descargado: {downloaded_size} bytes\n"
                error_msg += f"Esto indica que Google Drive bloqueó la descarga del archivo.\n"
                error_msg += f"Verifica que el archivo esté configurado como público.\n"
                if DEBUG:
                    error_msg += f"\n[DEBUG] Primeros 500 caracteres:\n{first_chunk[:500].decode('utf-8', errors='ignore')}"
                os.remove(tmp_path)  # Eliminar archivo inválido
                raise Exception(error_msg)
            else:
                error_msg = f"Error: El archivo descargado no parece ser un Excel válido.\n"
                error_msg += f"Tamaño descargado: {downloaded_size} bytes\n"
                error_msg += f"Primeros bytes (hex): {first_bytes.hex()}\n"
                error_msg += f"Los archivos .xlsx deben comenzar con la firma ZIP 'PK'."
                os.remove(tmp_path)  # Eliminar archivo inválido
                raise Exception(error_msg)
    
    # Verificar que el tamaño coincida (si se conoce)
    if total_size > 0 and downloaded_size != total_size:
        error_msg = f"Error: El archivo no se descargó completamente.\n"
        error_msg += f"Tamaño esperado: {total_size} bytes ({file_size_mb:.2f} MB)\n"
        error_msg += f"Tamaño descargado: {downloaded_size} bytes ({downloaded_size / (1024*1024):.2f} MB)"
        os.remove(tmp_path)  # Eliminar archivo incompleto
        raise Exception(error_msg)
    
    if DEBUG:
        print(f"[OK] Archivo descargado correctamente desde enlace público: {tmp_path}")
        print(f"[DEBUG] Nombre original del archivo: {original_file_name}")
        print(f"[DEBUG] Tamaño final: {downloaded_size} bytes ({downloaded_size / (1024*1024):.2f} MB)")
    
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
    Descarga un archivo desde Google Drive usando un enlace público.
    
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
        destination_blob_name: Nombre del blob (ruta) en GCS (ej: 'data_staging/dpt_planeacion_municipal/ipm/archivo.xlsx')
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
    folder_name: str = "IPM",
    destination_file_name: Optional[str] = None
) -> str:
    """
    Función completa que ingiere un archivo desde Google Drive (enlace público) y lo sube a GCS.
    
    Args:
        drive_url_or_id: URL pública de Google Drive o File ID
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket (default: "IPM")
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
