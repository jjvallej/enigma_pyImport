# modules/evaplan_ingest.py
"""
Módulo para ingerir datos desde la API de Evaplan.
Consume endpoints de la API, obtiene tokens de autenticación y almacena respuestas JSON en GCS.
"""
import requests
import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urlencode
from modules.config import PROJECT_ID, DEFAULT_BUCKET_NAME, CONF
from modules.gcp_utils import get_gcs_client

# === CONFIGURACIÓN ===
AUTH_ENDPOINT = CONF.evaplan.auth_endpoint
API_BASE_URL = CONF.evaplan.api_base_url
DEBUG = CONF.global_config.debug

# Construir endpoints completos
PERIODOS_ENDPOINT = f"{API_BASE_URL}{CONF.evaplan.endpoints.periodos}"
AVANCE_MR_ENDPOINT = f"{API_BASE_URL}{CONF.evaplan.endpoints.avance_mr}"
AVANCE_MP_ENDPOINT = f"{API_BASE_URL}{CONF.evaplan.endpoints.avance_mp}"
AVANCE_X_SUBPROGRAMA_ENDPOINT = f"{API_BASE_URL}{CONF.evaplan.endpoints.avance_x_subprograma}"
AVANCE_GENERAL_ENDPOINT = f"{API_BASE_URL}{CONF.evaplan.endpoints.avance_general}"

def get_auth_credentials() -> Dict[str, str]:
    """
    Obtiene las credenciales de autenticación desde config.yaml.
    
    Returns:
        Diccionario con usuario y password (formato requerido por la API)
    
    Raises:
        ValueError: Si las credenciales no están configuradas en config.yaml
    """
    try:
        config_creds = CONF.evaplan.credentials
        # Acceder a los atributos del SimpleNamespace
        usuario = getattr(config_creds, "usuario", "") or ""
        password = getattr(config_creds, "password", "") or ""
        
        # Convertir a string y limpiar espacios
        usuario = str(usuario).strip() if usuario else ""
        password = str(password).strip() if password else ""
        
    except (AttributeError, KeyError) as e:
        raise ValueError(
            f"No se pudieron leer las credenciales desde config.yaml: {e}\n"
            "Por favor, configura evaplan.credentials.usuario y evaplan.credentials.password en config.yaml"
        )
    
    # Validar que ambas credenciales estén presentes
    if not usuario or not password:
        raise ValueError(
            "Las credenciales de Evaplan no están configuradas en config.yaml.\n"
            "Por favor, configura evaplan.credentials.usuario y evaplan.credentials.password en config.yaml"
        )
    
    return {
        "usuario": usuario,
        "password": password
    }

def authenticate():
    """
    Autentica con la API de Evaplan y obtiene un token de acceso.
    
    Returns:
        Token de autenticación (Bearer token)
    
    Raises:
        Exception: Si la autenticación falla
    """
    # Print de versión para verificar que el archivo está actualizado
    print("[VERSION_CHECK] evaplan_ingest.py v2.0 - Usando get_auth_credentials() desde config.yaml")
    print("[VERSION_CHECK] Si ves este mensaje, el archivo está actualizado correctamente")
    
    if DEBUG:
        print(f"[INFO] Autenticando con la API de Evaplan...")
        print(f"[DEBUG] Endpoint: {AUTH_ENDPOINT}")
        print(f"[DEBUG] Timeout configurado: 90 segundos")
    
    # Obtener credenciales
    auth_credentials = get_auth_credentials()
    print(f"[DEBUG] Credenciales obtenidas desde config.yaml (usuario: {auth_credentials.get('usuario', 'N/A')[:10]}...)")
    
    # Intentar hacer un test de conectividad básico
    import socket
    try:
        host = "207.246.89.62"
        port = 80
        print(f"[DEBUG] Intentando conectar a {host}:{port}...")
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.settimeout(5)
        result = test_socket.connect_ex((host, port))
        test_socket.close()
        if result == 0:
            print(f"[DEBUG] ✓ Conectividad básica OK a {host}:{port}")
        else:
            print(f"[WARN] ✗ No se pudo conectar a {host}:{port} (código: {result})")
            print(f"[WARN] Esto puede indicar un problema de firewall/VPC en Composer")
    except Exception as e:
        print(f"[WARN] Error al probar conectividad: {e}")
    
    try:
        print(f"[DEBUG] Iniciando petición POST a {AUTH_ENDPOINT}...")
        # Usar Session como en los otros módulos (IDC, IPM) para mejor compatibilidad
        session = requests.Session()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = session.post(
            AUTH_ENDPOINT,
            json=auth_credentials,
            headers=headers,
            timeout=240  # Timeout de 4 minutos para dar más tiempo a la conexión desde Composer
        )
        print(f"[DEBUG] ✓ Petición completada, status code: {response.status_code}")
        
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
        # Usar Session como en los otros módulos para mejor compatibilidad
        session = requests.Session()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        response = session.get(
            PERIODOS_ENDPOINT,
            headers=headers,
            timeout=240  # Timeout de 4 minutos para dar más tiempo a la conexión desde Composer
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
        print(f"[DEBUG] Endpoint base: {AVANCE_MR_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        # Usar Session como en los otros módulos para mejor compatibilidad
        session = requests.Session()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        params = {"id": peri_idp}
        
        # Construir URL completa para logging
        full_url = f"{AVANCE_MR_ENDPOINT}?{urlencode(params)}"
        print(f"[DEBUG] URL completa con ID: {full_url}")
        
        response = session.get(
            AVANCE_MR_ENDPOINT,
            headers=headers,
            params=params,
            timeout=240  # Timeout de 4 minutos para dar más tiempo a la conexión desde Composer
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceMR recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceMR")
            raise Exception(f"Error al obtener AvanceMR: {error_msg}")
        
        # Agregar peri_idp al nivel raíz de la respuesta
        result["peri_idp"] = peri_idp
        
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
        print(f"[DEBUG] Endpoint base: {AVANCE_MP_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        # Usar Session como en los otros módulos para mejor compatibilidad
        session = requests.Session()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        params = {"id": peri_idp}
        
        # Construir URL completa para logging
        full_url = f"{AVANCE_MP_ENDPOINT}?{urlencode(params)}"
        print(f"[DEBUG] URL completa con ID: {full_url}")
        
        response = session.get(
            AVANCE_MP_ENDPOINT,
            headers=headers,
            params=params,
            timeout=240  # Timeout de 4 minutos para dar más tiempo a la conexión desde Composer
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceMP recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceMP")
            raise Exception(f"Error al obtener AvanceMP: {error_msg}")
        
        # Agregar peri_idp al nivel raíz de la respuesta
        result["peri_idp"] = peri_idp
        
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
        print(f"[DEBUG] Endpoint base: {AVANCE_X_SUBPROGRAMA_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        # Usar Session como en los otros módulos para mejor compatibilidad
        session = requests.Session()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        params = {"id": peri_idp}
        
        # Construir URL completa para logging
        full_url = f"{AVANCE_X_SUBPROGRAMA_ENDPOINT}?{urlencode(params)}"
        print(f"[DEBUG] URL completa con ID: {full_url}")
        
        response = session.get(
            AVANCE_X_SUBPROGRAMA_ENDPOINT,
            headers=headers,
            params=params,
            timeout=240  # Timeout de 4 minutos para dar más tiempo a la conexión desde Composer
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceXSubprograma recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceXSubprograma")
            raise Exception(f"Error al obtener AvanceXSubprograma: {error_msg}")
        
        # Agregar peri_idp al nivel raíz de la respuesta
        result["peri_idp"] = peri_idp
        
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
        print(f"[DEBUG] Endpoint base: {AVANCE_GENERAL_ENDPOINT}")
        print(f"[DEBUG] Periodo ID: {peri_idp}")
    
    try:
        # Usar Session como en los otros módulos para mejor compatibilidad
        session = requests.Session()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        params = {"id": peri_idp}
        
        # Construir URL completa para logging
        full_url = f"{AVANCE_GENERAL_ENDPOINT}?{urlencode(params)}"
        print(f"[DEBUG] URL completa con ID: {full_url}")
        
        response = session.get(
            AVANCE_GENERAL_ENDPOINT,
            headers=headers,
            params=params,
            timeout=240  # Timeout de 4 minutos para dar más tiempo a la conexión desde Composer
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if DEBUG:
            print(f"[DEBUG] Respuesta de AvanceGeneral recibida")
        
        # Validar estructura de respuesta
        if not result.get("success"):
            error_msg = result.get("message", "Error desconocido al obtener AvanceGeneral")
            raise Exception(f"Error al obtener AvanceGeneral: {error_msg}")
        
        # Agregar peri_idp al nivel raíz de la respuesta
        result["peri_idp"] = peri_idp
        
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

def read_latest_periodos_json_from_gcs(
    bucket_name: str,
    folder_name: str = "data_staging/dpt_planeacion_municipal/api_evaplan/periodos"
) -> Dict[str, Any]:
    """
    Lee el JSON más reciente de periodos desde GCS (el de la fecha actual).
    
    Args:
        bucket_name: Nombre del bucket en GCS
        folder_name: Nombre de la carpeta dentro del bucket
    
    Returns:
        Diccionario con el JSON completo de periodos
    
    Raises:
        Exception: Si no se encuentra el archivo o hay error al leerlo
    """
    gcs_client = get_gcs_client()
    
    # Normalizar la ruta de la carpeta
    if not folder_name.endswith('/'):
        folder_name = folder_name + '/'
    
    # Obtener el bucket
    try:
        bucket = gcs_client.bucket(bucket_name)
    except Exception as e:
        raise ValueError(f"No se pudo acceder al bucket '{bucket_name}': {e}")
    
    # Obtener fecha actual en formato YYYYMMDD
    fecha_actual = datetime.now().strftime("%Y%m%d")
    
    # Buscar archivo con la fecha actual
    file_name = f"periodo_{fecha_actual}.json"
    blob_name = f"{folder_name}{file_name}"
    
    if DEBUG:
        print(f"[INFO] Buscando archivo de periodos más reciente: {blob_name}")
    
    try:
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            raise Exception(f"No se encontró el archivo de periodos para la fecha actual: {blob_name}")
        
        # Descargar y leer el JSON
        json_string = blob.download_as_text()
        periodos_data = json.loads(json_string)
        
        if DEBUG:
            print(f"[OK] Archivo de periodos leído exitosamente: {blob_name}")
            data = periodos_data.get("data", {})
            periodos = data.get("periodos", [])
            print(f"[INFO] Se encontraron {len(periodos)} periodo(s) en el JSON")
        
        return periodos_data
        
    except Exception as e:
        raise Exception(f"Error al leer el archivo de periodos desde GCS: {e}")

def get_all_periodos_from_json(periodos_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extrae todos los periodos del JSON de periodos.
    
    Args:
        periodos_data: Diccionario con la respuesta completa de la API de periodos
    
    Returns:
        Lista de diccionarios con todos los periodos
    """
    data = periodos_data.get("data", {})
    periodos = data.get("periodos", [])
    
    if not periodos:
        raise Exception("No se encontraron periodos en el JSON")
    
    if DEBUG:
        print(f"[INFO] Se extrajeron {len(periodos)} periodo(s) del JSON")
        for periodo in periodos:
            print(f"[DEBUG]   - {periodo.get('peri_nombre', 'N/A')} (ID: {periodo.get('peri_idp', 'N/A')})")
    
    return periodos

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
    
    # Generar nombre de archivo con el formato: avance_mr_{fechadeconsulta}_peri_idp_{peri_idp}.json
    file_name = f"{tipo_avance_normalizado}_{fecha_consulta}_peri_idp_{peri_idp}.json"
    
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

