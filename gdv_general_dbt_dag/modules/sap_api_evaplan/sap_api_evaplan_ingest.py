"""
Ingesta desde la API SAP (zimportdata): consulta por periodo ini/fin (YYYYMM),
guarda la respuesta como JSONL en GCS (data_staging/dpt_planeacion_municipal/sap_api_evaplan).
Cada registro incluye fecha_lectura, run_ts, periodo_ini y periodo_fin para histórico en bronze
(fecha_lectura y run_ts en hora local Colombia, America/Bogota).
"""
import json
from datetime import datetime
from typing import Any, List, Optional
from zoneinfo import ZoneInfo

import requests
from requests.auth import HTTPBasicAuth

from modules.config import CONF, DEFAULT_BUCKET_NAME
from modules.gcp_utils import get_gcs_client
from modules.sap_api_evaplan.sap_api_evaplan_period_resolve import (
    resolve_sap_api_evaplan_periods,
)

# Hora local Colombia (COT, UTC−5) para fecha_lectura, run_ts y nombre de archivo en GCS.
_COLOMBIA_TZ = ZoneInfo("America/Bogota")


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
        # Clave configurada en config (ej. "infoz080") donde viene la lista de registros
        list_key = getattr(cfg, "response_list_key", None) or "infoz080"
        if list_key in data and isinstance(data[list_key], list):
            return data[list_key]
        # Fallback: claves habituales
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
    dag_conf: Optional[dict] = None,
    script_ini: Optional[str] = None,
    script_fin: Optional[str] = None,
    bucket_name: Optional[str] = None,
) -> str:
    """
    Consulta la API SAP para el periodo ini/fin, escribe JSONL en GCS y devuelve la URI.

    Resolución de periodo (si ini_yyyymm o fin_yyyymm no se pasan explícito): ver
    ``resolve_sap_api_evaplan_periods`` (conf del DAG > script > config; fin por defecto = mes actual en CO).
    """
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")

    if ini_yyyymm is None or fin_yyyymm is None:
        r_ini, r_fin = resolve_sap_api_evaplan_periods(
            cfg, ds_nodash, dag_conf=dag_conf or {}, script_ini=script_ini, script_fin=script_fin
        )
        ini_yyyymm = ini_yyyymm if ini_yyyymm is not None else r_ini
        fin_yyyymm = fin_yyyymm if fin_yyyymm is not None else r_fin

    ini_yyyymm = str(ini_yyyymm).strip()
    fin_yyyymm = str(fin_yyyymm).strip()

    # Timestamp exacto del momento de consumo del API (se toma antes del request HTTP), en hora Colombia.
    now_co = datetime.now(tz=_COLOMBIA_TZ)
    records = fetch_sap_api(ini_yyyymm, fin_yyyymm)

    # Mismo formato STRING en todo el pipeline (ingest → load → transform). Zona: America/Bogota.
    fecha_lectura = run_ts = now_co.strftime("%Y-%m-%d %H:%M:%S")

    # Añadir metadatos de ejecución a cada registro
    for r in records:
        r["fecha_lectura"] = fecha_lectura
        r["run_ts"] = run_ts
        r["periodo_ini"] = ini_yyyymm
        r["periodo_fin"] = fin_yyyymm

    bucket = bucket_name or DEFAULT_BUCKET_NAME
    base_folder = (cfg.gcs_base_folder or "").rstrip("/")
    yyyy = now_co.strftime("%Y")
    mm = now_co.strftime("%m")
    ts_compacto = now_co.strftime("%Y%m%d_%H%M%S")
    export_filename = f"sap_api_evaplan_{ts_compacto}.json"
    object_path = f"{base_folder}/{yyyy}/{mm}/{export_filename}"

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
