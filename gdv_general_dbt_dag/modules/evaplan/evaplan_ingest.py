# modules/evaplan_ingest.py
"""
Módulo para ingerir datos desde la API de Evaplan.
Consume endpoints de la API, obtiene tokens de autenticación y almacena respuestas JSON en GCS.
"""
from google.cloud import storage
import os
import json
import requests
from typing import Optional, Dict, Any
from datetime import datetime

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True

# Configuración de la API
API_BASE_URL = "http://207.246.89.62/ApiEvaplan"
AUTH_ENDPOINT = f"{API_BASE_URL}/auth/login"
PERIODOS_ENDPOINT = f"{API_BASE_URL}/datos/periodos"

# Credenciales de autenticación
AUTH_CREDENTIALS = {
    "usuario": "usuario_api",
    "password": "ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94f"
}

# ---------------------------
# Clientes
# ---------------------------
def _gcs_client() -> storage.Client:
    """Crea un cliente de Google Cloud Storage."""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return storage.Client(project=PROJECT_ID)

# ---------------------------
# Funciones de API
# ---------------------------
def authenticate() -> str:
    """
    Autentica con la API de Evaplan y obtiene un token de acceso.
    
    Returns:
        Token de autenticación (Bearer token)
    
    Raises:
        Exception: Si la autenticación falla
    """
    if DEBUG:
        print(f"[INFO] Autenticando con la API de Evaplan...")
        print(f"[DEBUG] Endpoint: {AUTH_ENDPOINT}")
    
    try:
        response = requests.post(
            AUTH_ENDPOINT,
            json=AUTH_CREDENTIALS,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de autenticación: {json.dumps(result, indent=2)}")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido en la autenticación")
            raise Exception(f"Error en autenticación: {error_msg}")
        
        # Extraer token
        data = result.get("data", {})
        token = data.get("token")
        
        if not token:
            raise Exception("No se recibió token en la respuesta de autenticación")
        
        if DEBUG:
            print(f"[OK] Autenticación exitosa. Token obtenido.")
            print(f"[DEBUG] Token expira en: {data.get('expira_en', 'N/A')} segundos")
        
        return token
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al autenticar con la API de Evaplan: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error al parsear respuesta JSON de autenticación: {e}")
    except Exception as e:
        raise Exception(f"Error inesperado en autenticación: {e}")

def get_periodos(token: str) -> Dict[str, Any]:
    """
    Obtiene la lista de periodos desde la API de Evaplan.
    
    Args:
        token: Token de autenticación Bearer
    
    Returns:
        Diccionario con la respuesta completa de la API
    
    Raises:
        Exception: Si la petición falla
    """
    if DEBUG:
        print(f"[INFO] Obteniendo periodos desde la API de Evaplan...")
        print(f"[DEBUG] Endpoint: {PERIODOS_ENDPOINT}")
    
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(
            PERIODOS_ENDPOINT,
            headers=headers,
            timeout=30
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de periodos: {json.dumps(result, indent=2)}")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener periodos")
            raise Exception(f"Error al obtener periodos: {error_msg}")
        
        # Validar que existan periodos
        data = result.get("data", {})
        periodos = data.get("periodos", [])
        
        if not periodos:
            raise Exception("No se encontraron periodos en la respuesta")
        
        if DEBUG:
            print(f"[OK] Se obtuvieron {len(periodos)} periodo(s)")
            # Mostrar el periodo más reciente
            if periodos:
                periodo_mas_reciente = max(periodos, key=lambda p: p.get("peri_idp", 0))
                print(f"[INFO] Periodo más reciente: {periodo_mas_reciente.get('peri_nombre', 'N/A')} (ID: {periodo_mas_reciente.get('peri_idp', 'N/A')})")
        
        return result
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al obtener periodos desde la API de Evaplan: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error al parsear respuesta JSON de periodos: {e}")
    except Exception as e:
        raise Exception(f"Error inesperado al obtener periodos: {e}")

def get_periodo_mas_reciente(periodos_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Obtiene el periodo más reciente de la respuesta de periodos.
    El periodo más reciente es el que tiene el mayor peri_idp.
    
    Args:
        periodos_data: Diccionario con la respuesta completa de la API de periodos
    
    Returns:
        Diccionario con el periodo más reciente, o None si no hay periodos
    """
    data = periodos_data.get("data", {})
    periodos = data.get("periodos", [])
    
    if not periodos:
        return None
    
    # Encontrar el periodo con el mayor peri_idp
    periodo_mas_reciente = max(periodos, key=lambda p: p.get("peri_idp", 0))
    
    if DEBUG:
        print(f"[INFO] Periodo más reciente seleccionado: {periodo_mas_reciente.get('peri_nombre', 'N/A')} (ID: {periodo_mas_reciente.get('peri_idp', 'N/A')})")
    
    return periodo_mas_reciente

# ---------------------------
# Funciones de GCS
# ---------------------------
def upload_json_to_gcs(
    json_data: Dict[str, Any],
    bucket_name: str,
    destination_blob_name: str,
    overwrite: bool = True
) -> str:
    """
    Sube un diccionario JSON a Google Cloud Storage.
    Si la carpeta no existe, se crea automáticamente.
    Si el archivo ya existe, se sobrescribe por defecto.
    
    Args:
        json_data: Diccionario con los datos JSON a subir
        bucket_name: Nombre del bucket en GCS
        destination_blob_name: Nombre del blob (ruta) en GCS 
                              (ej: 'data_staging/dpt_planeacion_municipal/evaplan/periodos.json')
        overwrite: Si es True, sobrescribe el archivo si ya existe
    
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
    
    # Convertir diccionario a JSON string
    json_string = json.dumps(json_data, indent=2, ensure_ascii=False)
    
    # Subir el archivo (GCS crea automáticamente la "carpeta" si no existe)
    blob.upload_from_string(json_string, content_type='application/json')
    
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    
    if DEBUG:
        print(f"[OK] Archivo JSON subido a GCS: {gcs_uri}")
        print(f"[DEBUG] Tamaño del JSON: {len(json_string)} bytes")
    
    return gcs_uri

def save_periodos_to_gcs(
    periodos_data: Dict[str, Any],
    bucket_name: str,
    folder_name: str = "data_staging/dpt_planeacion_municipal/evaplan/periodos",
    file_name: Optional[str] = None
) -> str:
    """
    Guarda la respuesta de periodos en un archivo JSON en GCS.
    
    Args:
        periodos_data: Diccionario con la respuesta completa de la API de periodos
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket (default: "data_staging/dpt_planeacion_municipal/evaplan/periodos")
        file_name: Nombre opcional para el archivo. Si no se proporciona, se genera automáticamente
                   con formato: evaplan_periodos_YYYYMMDD_HHMMSS.json
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/folder/file.json)
    """
    # Generar nombre de archivo si no se proporciona
    if not file_name:
        # Intentar obtener fecha y hora de consulta desde la respuesta de la API
        fecha_consulta = None
        hora_consulta = None
        
        try:
            data = periodos_data.get("data", {})
            fecha_consulta_str = data.get("fecha_consulta")
            
            if fecha_consulta_str:
                # Parsear fecha_consulta (formato esperado: "2025-11-25 10:45:53")
                fecha_hora = datetime.strptime(fecha_consulta_str, "%Y-%m-%d %H:%M:%S")
                fecha_consulta = fecha_hora.strftime("%Y%m%d")
                hora_consulta = fecha_hora.strftime("%H%M%S")
        except Exception as e:
            if DEBUG:
                print(f"[WARN] No se pudo extraer fecha_consulta de la respuesta. Usando fecha/hora actual: {e}")
        
        # Si no se pudo obtener de la API, usar fecha/hora actual
        if not fecha_consulta or not hora_consulta:
            now = datetime.now()
            fecha_consulta = now.strftime("%Y%m%d")
            hora_consulta = now.strftime("%H%M%S")
        
        file_name = f"evaplan_periodos_{fecha_consulta}_{hora_consulta}.json"
    
    # Asegurar extensión .json
    if not file_name.endswith('.json'):
        file_name = f"{file_name}.json"
    
    # Construir ruta destino en GCS
    folder_name = folder_name.strip('/')
    destination_blob_name = f"{folder_name}/{file_name}"
    
    if DEBUG:
        print(f"[INFO] Guardando periodos en GCS...")
        print(f"[DEBUG] Bucket: {bucket_name}")
        print(f"[DEBUG] Carpeta: {folder_name}")
        print(f"[DEBUG] Archivo: {file_name}")
    
    # Subir a GCS
    gcs_uri = upload_json_to_gcs(
        json_data=periodos_data,
        bucket_name=bucket_name,
        destination_blob_name=destination_blob_name
    )
    
    return gcs_uri

