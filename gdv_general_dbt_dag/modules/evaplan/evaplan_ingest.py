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
AVANCE_MR_ENDPOINT = f"{API_BASE_URL}/datos/AvanceMR"
AVANCE_MP_ENDPOINT = f"{API_BASE_URL}/datos/AvanceMP"
AVANCE_X_SUBPROGRAMA_ENDPOINT = f"{API_BASE_URL}/datos/AvanceXSubprograma"
AVANCE_GENERAL_ENDPOINT = f"{API_BASE_URL}/datos/AvanceGeneral"

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

def get_avance_mr(token: str, peri_idp: int) -> Dict[str, Any]:
    """
    Obtiene los datos de AvanceMR desde la API de Evaplan.
    
    Args:
        token: Token de autenticación Bearer
        peri_idp: ID del periodo
    
    Returns:
        Diccionario con la respuesta completa de la API
    
    Raises:
        Exception: Si la petición falla
    """
    if DEBUG:
        print(f"[INFO] Obteniendo AvanceMR desde la API de Evaplan...")
        print(f"[DEBUG] Endpoint: {AVANCE_MR_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        params = {"id": peri_idp}
        
        response = requests.get(
            AVANCE_MR_ENDPOINT,
            headers=headers,
            params=params,
            timeout=30
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceMR recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceMR")
            raise Exception(f"Error al obtener AvanceMR: {error_msg}")
        
        if DEBUG:
            data = result.get("data", {})
            avance_mr = data.get("AvanceMR", [])
            print(f"[OK] Se obtuvieron {len(avance_mr)} registro(s) de AvanceMR")
        
        return result
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al obtener AvanceMR desde la API de Evaplan: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error al parsear respuesta JSON de AvanceMR: {e}")
    except Exception as e:
        raise Exception(f"Error inesperado al obtener AvanceMR: {e}")

def get_avance_mp(token: str, peri_idp: int) -> Dict[str, Any]:
    """
    Obtiene los datos de AvanceMP desde la API de Evaplan.
    
    Args:
        token: Token de autenticación Bearer
        peri_idp: ID del periodo
    
    Returns:
        Diccionario con la respuesta completa de la API
    
    Raises:
        Exception: Si la petición falla
    """
    if DEBUG:
        print(f"[INFO] Obteniendo AvanceMP desde la API de Evaplan...")
        print(f"[DEBUG] Endpoint: {AVANCE_MP_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        params = {"id": peri_idp}
        
        response = requests.get(
            AVANCE_MP_ENDPOINT,
            headers=headers,
            params=params,
            timeout=30
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceMP recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceMP")
            raise Exception(f"Error al obtener AvanceMP: {error_msg}")
        
        if DEBUG:
            data = result.get("data", {})
            avance_mp = data.get("AvanceMP", [])
            print(f"[OK] Se obtuvieron {len(avance_mp)} registro(s) de AvanceMP")
        
        return result
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al obtener AvanceMP desde la API de Evaplan: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error al parsear respuesta JSON de AvanceMP: {e}")
    except Exception as e:
        raise Exception(f"Error inesperado al obtener AvanceMP: {e}")

def get_avance_x_subprograma(token: str, peri_idp: int) -> Dict[str, Any]:
    """
    Obtiene los datos de AvanceXSubprograma desde la API de Evaplan.
    
    Args:
        token: Token de autenticación Bearer
        peri_idp: ID del periodo
    
    Returns:
        Diccionario con la respuesta completa de la API
    
    Raises:
        Exception: Si la petición falla
    """
    if DEBUG:
        print(f"[INFO] Obteniendo AvanceXSubprograma desde la API de Evaplan...")
        print(f"[DEBUG] Endpoint: {AVANCE_X_SUBPROGRAMA_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        params = {"id": peri_idp}
        
        response = requests.get(
            AVANCE_X_SUBPROGRAMA_ENDPOINT,
            headers=headers,
            params=params,
            timeout=30
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceXSubprograma recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceXSubprograma")
            raise Exception(f"Error al obtener AvanceXSubprograma: {error_msg}")
        
        if DEBUG:
            data = result.get("data", {})
            avance_x_subprograma = data.get("AvanceXSubprograma", [])
            print(f"[OK] Se obtuvieron {len(avance_x_subprograma)} registro(s) de AvanceXSubprograma")
        
        return result
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al obtener AvanceXSubprograma desde la API de Evaplan: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error al parsear respuesta JSON de AvanceXSubprograma: {e}")
    except Exception as e:
        raise Exception(f"Error inesperado al obtener AvanceXSubprograma: {e}")

def get_avance_general(token: str, peri_idp: int) -> Dict[str, Any]:
    """
    Obtiene los datos de AvanceGeneral desde la API de Evaplan.
    
    Args:
        token: Token de autenticación Bearer
        peri_idp: ID del periodo
    
    Returns:
        Diccionario con la respuesta completa de la API
    
    Raises:
        Exception: Si la petición falla
    """
    if DEBUG:
        print(f"[INFO] Obteniendo AvanceGeneral desde la API de Evaplan...")
        print(f"[DEBUG] Endpoint: {AVANCE_GENERAL_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        params = {"id": peri_idp}
        
        response = requests.get(
            AVANCE_GENERAL_ENDPOINT,
            headers=headers,
            params=params,
            timeout=30
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceGeneral recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceGeneral")
            raise Exception(f"Error al obtener AvanceGeneral: {error_msg}")
        
        if DEBUG:
            data = result.get("data", {})
            avance_general = data.get("AvanceGeneral", [])
            print(f"[OK] Se obtuvieron {len(avance_general)} registro(s) de AvanceGeneral")
        
        return result
        
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error al obtener AvanceGeneral desde la API de Evaplan: {e}")
    except json.JSONDecodeError as e:
        raise Exception(f"Error al parsear respuesta JSON de AvanceGeneral: {e}")
    except Exception as e:
        raise Exception(f"Error inesperado al obtener AvanceGeneral: {e}")

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
                              (ej: 'data_staging/dpt_planeacion_municipal/api_evaplan/periodos.json')
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
    folder_name: str = "data_staging/dpt_planeacion_municipal/api_evaplan/periodos",
    file_name: Optional[str] = None
) -> str:
    """
    Guarda la respuesta de periodos en un archivo JSON en GCS.
    
    Args:
        periodos_data: Diccionario con la respuesta completa de la API de periodos
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket (default: "data_staging/dpt_planeacion_municipal/api_evaplan/periodos")
        file_name: Nombre opcional para el archivo. Si no se proporciona, se genera automáticamente
                   con formato: evaplan_periodos_YYYYMMDD_HHMMSS.json
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/folder/file.json)
    """
    # Generar nombre de archivo si no se proporciona
    if not file_name:
        # Intentar obtener fecha de consulta desde la respuesta de la API
        fecha_consulta = None
        
        try:
            data = periodos_data.get("data", {})
            fecha_consulta_str = data.get("fecha_consulta")
            
            if fecha_consulta_str:
                # Parsear fecha_consulta (formato esperado: "2025-11-25 10:45:53")
                fecha_hora = datetime.strptime(fecha_consulta_str, "%Y-%m-%d %H:%M:%S")
                fecha_consulta = fecha_hora.strftime("%Y%m%d")
        except Exception as e:
            if DEBUG:
                print(f"[WARN] No se pudo extraer fecha_consulta de la respuesta. Usando fecha actual: {e}")
        
        # Si no se pudo obtener de la API, usar fecha actual
        if not fecha_consulta:
            now = datetime.now()
            fecha_consulta = now.strftime("%Y%m%d")
        
        file_name = f"periodo_{fecha_consulta}.json"
    
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

def save_avance_to_gcs(
    avance_data: Dict[str, Any],
    bucket_name: str,
    folder_name: str,
    tipo_avance: str,  # "AvanceMR", "AvanceMP", "AvanceXSubprograma", "AvanceGeneral"
    peri_idp: int
) -> str:
    """
    Guarda la respuesta de AvanceMR, AvanceMP, AvanceXSubprograma o AvanceGeneral en un archivo JSON en GCS.
    
    Args:
        avance_data: Diccionario con la respuesta completa de la API
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket (ej: "data_staging/dpt_planeacion_municipal/api_evaplan/avance_mr")
        tipo_avance: Tipo de avance ("AvanceMR", "AvanceMP", "AvanceXSubprograma", "AvanceGeneral")
        peri_idp: ID del periodo
    
    Returns:
        URI completa del archivo en GCS (gs://bucket/folder/file.json)
    """
    # Mapeo de tipos de avance a nombres en minúsculas con guiones bajos
    tipo_avance_map = {
        "AvanceMR": "avance_mr",
        "AvanceMP": "avance_mp",
        "AvanceXSubprograma": "avance_x_subprograma",
        "AvanceGeneral": "avance_general"
    }
    
    # Convertir tipo_avance a formato minúsculas con guiones bajos
    tipo_avance_normalizado = tipo_avance_map.get(tipo_avance)
    if not tipo_avance_normalizado:
        raise ValueError(f"Tipo de avance no reconocido: {tipo_avance}. Valores permitidos: {list(tipo_avance_map.keys())}")
    
    # Intentar obtener fecha de consulta desde la respuesta de la API
    fecha_consulta = None
    
    try:
        data = avance_data.get("data", {})
        fecha_consulta_str = data.get("fecha_consulta")
        
        if fecha_consulta_str:
            # Parsear fecha_consulta (formato esperado: "2025-11-22 13:33:26")
            fecha_hora = datetime.strptime(fecha_consulta_str, "%Y-%m-%d %H:%M:%S")
            fecha_consulta = fecha_hora.strftime("%Y%m%d")
    except Exception as e:
        if DEBUG:
            print(f"[WARN] No se pudo extraer fecha_consulta de la respuesta. Usando fecha actual: {e}")
    
    # Si no se pudo obtener de la API, usar fecha actual
    if not fecha_consulta:
        now = datetime.now()
        fecha_consulta = now.strftime("%Y%m%d")
    
    # Generar nombre de archivo con el formato: avance_mr_{fechadeconsulta}.json
    file_name = f"{tipo_avance_normalizado}_{fecha_consulta}.json"
    
    # Construir ruta destino en GCS
    folder_name = folder_name.strip('/')
    destination_blob_name = f"{folder_name}/{file_name}"
    
    if DEBUG:
        print(f"[INFO] Guardando {tipo_avance} en GCS...")
        print(f"[DEBUG] Bucket: {bucket_name}")
        print(f"[DEBUG] Carpeta: {folder_name}")
        print(f"[DEBUG] Archivo: {file_name}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    # Subir a GCS
    gcs_uri = upload_json_to_gcs(
        json_data=avance_data,
        bucket_name=bucket_name,
        destination_blob_name=destination_blob_name
    )
    
    return gcs_uri

