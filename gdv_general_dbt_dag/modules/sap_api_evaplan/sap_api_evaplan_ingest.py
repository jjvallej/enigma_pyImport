"""
Ingesta desde la API SAP (zimportdata): consulta por periodo ini/fin (YYYYMM),
guarda la respuesta como JSONL en GCS (data_staging/dpt_planeacion_municipal/sap_api_evaplan).
"""
import json
from typing import Any, List, Optional

import requests
from requests.auth import HTTPBasicAuth

from modules.config import CONF, DEFAULT_BUCKET_NAME
from modules.gcp_utils import get_gcs_client


def _get_cfg() -> Any:
    return getattr(CONF, "sap_api_evaplan", None)


def _build_params(ini_yyyymm: str, fin_yyyymm: str) -> dict:
    """Construye los query params para la API SAP (sap-client, ini, fin, p1-p4)."""
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")
    p = cfg.params
    return {
        "sap-client": getattr(p, "sap_client", "710"),
        "ini": ini_yyyymm,
        "fin": fin_yyyymm,
        "p1": getattr(p, "p1", "X"),
        "p2": getattr(p, "p2", ""),
        "p3": getattr(p, "p3", ""),
        "p4": getattr(p, "p4", ""),
    }


def fetch_sap_api(ini_yyyymm: str, fin_yyyymm: str) -> List[dict]:
    """
    Llama a la API SAP con autenticación básica y params ini/fin.
    Devuelve la lista de registros (asume que la API devuelve un array JSON o un objeto con lista).
    """
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")

    url = cfg.url
    params = _build_params(ini_yyyymm, fin_yyyymm)
    usuario = getattr(cfg.credentials, "usuario", "") or ""
    password = getattr(cfg.credentials, "password", "") or ""
    timeout = getattr(cfg, "timeout_seconds", 900)

    response = requests.get(
        url,
        params=params,
        auth=HTTPBasicAuth(usuario, password),
        timeout=timeout,
    )
    response.raise_for_status()

    data = response.json()
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Si devuelve un objeto, intentar una clave común de lista
        for key in ("data", "items", "results", "records"):
            if key in data and isinstance(data[key], list):
                return data[key]
        # Un solo registro como objeto -> lista de un elemento
        return [data]
    return []


def run_ingest(
    ds_nodash: str,
    ini_yyyymm: Optional[str] = None,
    fin_yyyymm: Optional[str] = None,
    bucket_name: Optional[str] = None,
) -> str:
    """
    Consulta la API SAP para el periodo ini/fin, escribe JSONL en GCS y devuelve la URI.

    ini/fin se toman en este orden: argumentos de la función → config.yaml (ini, fin) → ds_nodash (YYYYMM).
    """
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")

    if ini_yyyymm is None:
        ini_yyyymm = getattr(cfg, "ini", None) or ds_nodash[:6]
    if fin_yyyymm is None:
        fin_yyyymm = getattr(cfg, "fin", None) or ds_nodash[:6]
    # Asegurar string (por si en YAML vienen como número)
    ini_yyyymm = str(ini_yyyymm).strip()
    fin_yyyymm = str(fin_yyyymm).strip()

    records = fetch_sap_api(ini_yyyymm, fin_yyyymm)

    bucket = bucket_name or DEFAULT_BUCKET_NAME
    base_folder = (cfg.gcs_base_folder or "").rstrip("/")
    export_filename = (cfg.export_filename or "sap_api_evaplan.json").replace(
        "{{ ds_nodash }}", ds_nodash
    )
    object_path = f"{base_folder}/{export_filename}"

    # Escribir JSONL (una línea por objeto) para carga en BigQuery
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    content = "\n".join(lines).encode("utf-8")

    gcs_client = get_gcs_client()
    bucket_obj = gcs_client.bucket(bucket)
    blob = bucket_obj.blob(object_path)
    blob.upload_from_string(content, content_type="application/json")

    return f"gs://{bucket}/{object_path}"


def ensure_gcs_folder(bucket_name: Optional[str] = None) -> None:
    """Crea el prefijo en GCS si no existe."""
    cfg = _get_cfg()
    if not cfg:
        return
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    bucket = bucket_name or DEFAULT_BUCKET_NAME
    prefix = (cfg.gcs_base_folder or "").rstrip("/") + "/"
    hook = GCSHook()
    blobs = list(hook.list(bucket_name=bucket, prefix=prefix, max_results=1))
    if blobs:
        return
    hook.upload(
        bucket_name=bucket,
        object_name=prefix,
        data=b"",
        mime_type="application/x-directory",
    )
