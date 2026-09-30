"""Módulo de carga y resolución de configuración multientorno (dev, qa, prod) para pyimport.
Lee y parsea el archivo `config.yaml` resolviendo el entorno activo.
"""

from __future__ import annotations

from datetime import date
import os
from pathlib import Path
from typing import Any, Dict


DEFAULT_ENV = "dev"

# Fecha inicial fija del rango de datos del pipeline.
PIPELINE_START_DATE = date(2000, 1, 1)


DEFAULT_CONNECTIONS: Dict[str, Any] = {
    "sipsa_dane": "sipsa_dane",
    "noaa_oni": "noaa_oni",
    "gobernacion_valle": "gobernacion_valle",
    "google_cloud_default": "google_cloud_default",
    "ckan_comentarios": [],
}

DEFAULT_COMPOSER: Dict[str, str] = {
    "environment": "composer-gdv",
    "location": "us-east1",
    "project": "datagov-477214",
}

DEFAULT_CONFIG: Dict[str, Any] = {
    "active_environment": "dev",
    "environments": {
        "dev": {
            "gcs_bucket": "gs://datalake_gdv_pdn/data_staging/valledata",
            "local_raw_path": "data/raw",
            "local_output_path": "data",
            "composer": dict(DEFAULT_COMPOSER),
            "connections": dict(DEFAULT_CONNECTIONS),
            "bigquery": {
                "project_id": "datagov-477214",
                "location": "US",
                "datasets": {
                    "bronze": "valledata",
                    "silver": "valledata",
                },
                "buckets": {
                    "bronze": "gs://valledata-bronze-dev",
                    "silver": "gs://valledata-silver-dev",
                },
                "tables": {
                    "sipsa_bronze": "bronze_agri_sipsa",
                    "sipsa_silver": "silver_agri_sipsa",
                    "oni_bronze": "bronze_oni_climatico",
                    "oni_silver": "silver_oni_climatico",
                    "cultivos_bronze": "bronze_cultivos_valle",
                    "cultivos_silver": "silver_cultivos_valle",
                    "consolidado_silver": "silver_agri_consolidado",
                    "comentarios_bronze": "bronze_comentarios",
                    "comentarios_silver": "silver_comentarios",
                },
            },
        },
        "qa": {
            "gcs_bucket": "gs://datalake_gdv_pdn/data_staging/valledata",
            "local_raw_path": "data/raw",
            "local_output_path": "data",
            "composer": dict(DEFAULT_COMPOSER),
            "connections": dict(DEFAULT_CONNECTIONS),
            "bigquery": {
                "project_id": "datagov-477214",
                "location": "US",
                "datasets": {
                    "bronze": "valledata",
                    "silver": "valledata",
                },
                "buckets": {
                    "bronze": "gs://valledata-bronze-qa",
                    "silver": "gs://valledata-silver-qa",
                },
                "tables": {
                    "sipsa_bronze": "bronze_agri_sipsa",
                    "sipsa_silver": "silver_agri_sipsa",
                    "oni_bronze": "bronze_oni_climatico",
                    "oni_silver": "silver_oni_climatico",
                    "cultivos_bronze": "bronze_cultivos_valle",
                    "cultivos_silver": "silver_cultivos_valle",
                    "consolidado_silver": "silver_agri_consolidado",
                    "comentarios_bronze": "bronze_comentarios",
                    "comentarios_silver": "silver_comentarios",
                },
            },
        },
        "prod": {
            "gcs_bucket": "gs://datalake_gdv_pdn/data_staging/valledata",
            "local_raw_path": "data/raw",
            "local_output_path": "data",
            "composer": dict(DEFAULT_COMPOSER),
            "connections": dict(DEFAULT_CONNECTIONS),
            "bigquery": {
                "project_id": "datagov-477214",
                "location": "US",
                "datasets": {
                    "bronze": "valledata",
                    "silver": "valledata",
                },
                "buckets": {
                    "bronze": "gs://valledata-bronze-prod",
                    "silver": "gs://valledata-silver-prod",
                },
                "tables": {
                    "sipsa_bronze": "bronze_agri_sipsa",
                    "sipsa_silver": "silver_agri_sipsa",
                    "oni_bronze": "bronze_oni_climatico",
                    "oni_silver": "silver_oni_climatico",
                    "cultivos_bronze": "bronze_cultivos_valle",
                    "cultivos_silver": "silver_cultivos_valle",
                    "consolidado_silver": "silver_agri_consolidado",
                    "comentarios_bronze": "bronze_comentarios",
                    "comentarios_silver": "silver_comentarios",
                },
            },
        },
    },
    "sipsa": {
        "base_url": "https://www.dane.gov.co",
        "earliest_period": "2012-01",
        "output_filename": "sipsa_precios.csv",
        "path_templates": [
            "/files/investigaciones/agropecuario/sipsa/anex_mensual_{mes}_{anio}.xls",
            "/files/investigaciones/agropecuario/sipsa/anex_mensual_{mes}_{anio}.xlsx",
            "/files/investigaciones/agropecuario/sipsa/anexo_mensual_SIPSA_mayoristas_{mes}_{anio}.xlsx",
            "/files/operaciones/SIPSA/anex-SIPSAMensual-{mes}{anio}.xlsx",
        ],
    },
    "oni": {
        "base_url": "https://www.cpc.ncep.noaa.gov",
        "path": "/products/analysis_monitoring/enso/oni/v5/",
        "url": "https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/enso/oni/v5/",
        "raw_filename": "oni_v5.html",
        "output_filename": "oni_promedio_anual.csv",
    },
    "cultivos": {
        "base_url": "https://datosabiertos.valledelcauca.gov.co",
        "permanentes_path": (
            "/dataset/55a3d384-fec1-4267-8541-7d62a3fc9223/resource/"
            "7c578f9f-094d-4e6e-b1db-2f5e3bde32c9/download/cultivos_permanentes.csv"
        ),
        "transitorios_path": (
            "/dataset/16a0cede-1b2f-4db8-8a42-ca10d0223cee/resource/"
            "f0ed7211-5ab5-4fa0-885d-e343cc906f2c/download/cultivos_transitorios.csv"
        ),
        "permanentes_filename": "cultivos_permanentes.csv",
        "transitorios_filename": "cultivos_transitorios.csv",
        "output_filename": "cultivos_valle.csv",
        "consolidado_filename": "dataset_consolidado_valle.csv",
    },
    "ckan_comentarios": {
        "table": "comment",
        "schema": "public",
        "raw_subdir": "ckan_comentarios",
        "raw_filename_template": "comment_{conn_id}.csv",
        "output_filename": "ckan_comentarios.csv",
        "silver_filename": "silver_comentarios.csv",
        "max_connections": 14,
        "sentiment_lang": "es",
        "use_test_data": True,
        "test_data_records": 1000,
        "test_data_seed": 42,
    },
}

def _active_env_spec(cfg: Dict[str, Any] | None) -> Dict[str, Any]:
    """Obtiene el bloque del entorno activo (dev/qa/prod) desde el YAML o el cfg resuelto."""
    if not cfg:
        return {}
    env_spec = cfg.get("env_spec")
    if isinstance(env_spec, dict) and (env_spec.get("connections") or env_spec.get("composer")):
        return env_spec
    envs = cfg.get("environments") or {}
    active = cfg.get("active_env") or cfg.get("active_environment") or DEFAULT_ENV
    if isinstance(active, str):
        active = active.lower()
    block = envs.get(active) if isinstance(envs, dict) else None
    return block if isinstance(block, dict) else {}


def get_connection_params(cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Lee connections del entorno activo (environments.dev|qa|prod.connections).

    Valores escalares (HTTP/GCP) se normalizan a str. Arreglos/objetos
    (p. ej. ckan_comentarios) se preservan sin convertir a string.
    """
    merged: Dict[str, Any] = dict(DEFAULT_CONNECTIONS)
    if not cfg:
        return merged
    env_spec = _active_env_spec(cfg)
    raw = env_spec.get("connections") or cfg.get("connections") or {}
    for key, value in raw.items():
        if value is None:
            continue
        if isinstance(value, (list, dict)):
            merged[str(key)] = value
        else:
            merged[str(key)] = str(value)
    return merged


def get_connection_id(cfg: Dict[str, Any] | None, name: str) -> str:
    """Consulta el parámetro de conexión escalar `name` del entorno activo.

    Ejemplo: get_connection_id(cfg, "sipsa_dane") lee
    environments.<env>.connections.sipsa_dane.

    Para arreglos (ckan_comentarios) use get_connection_list().
    """
    value = get_connection_params(cfg).get(name, name)
    if isinstance(value, (list, dict)):
        raise TypeError(
            f"connections.{name} es un arreglo/objeto; use get_connection_list(cfg, '{name}')"
        )
    return str(value)


def get_connection_list(cfg: Dict[str, Any] | None, name: str) -> list[dict[str, str]]:
    """Normaliza connections.<name> (arreglo) a lista de dicts {{conn_id, municipio}}.

    Acepta ítems:
      - "ckan_pg_01"
      - {{conn_id: ckan_pg_01, municipio: alcala}}
    """
    raw = get_connection_params(cfg).get(name)
    domain = (cfg or {}).get(name) if isinstance(cfg, dict) else None
    if raw in (None, "", []):
        if isinstance(domain, dict):
            raw = domain.get("connections")
    if raw in (None, ""):
        return []
    if not isinstance(raw, list):
        raise TypeError(f"connections.{name} debe ser un arreglo, recibido: {type(raw).__name__}")

    max_n = 14
    if isinstance(domain, dict) and domain.get("max_connections"):
        try:
            max_n = int(domain["max_connections"])
        except (TypeError, ValueError):
            max_n = 14

    items: list[dict[str, str]] = []
    for idx, item in enumerate(raw):
        if idx >= max_n:
            print(
                f"⚠️ [CONFIG] connections.{name} tiene {len(raw)} ítems; "
                f"se usan solo los primeros {max_n}."
            )
            break
        if isinstance(item, str):
            conn_id = item.strip()
            municipio = ""
        elif isinstance(item, dict):
            conn_id = str(item.get("conn_id") or item.get("connection_id") or "").strip()
            municipio = str(item.get("municipio") or item.get("municipality") or "").strip()
        else:
            raise TypeError(
                f"connections.{name}[{idx}] inválido: se espera str o dict, "
                f"recibido {type(item).__name__}"
            )
        if not conn_id:
            raise ValueError(f"connections.{name}[{idx}] sin conn_id")
        items.append({"conn_id": conn_id, "municipio": municipio})
    return items


def safe_sql_ident(name: str) -> str:
    """Valida identificador SQL simple (schema/tabla) para evitar inyección."""
    import re

    value = str(name or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Identificador SQL inválido: {name!r}")
    return value


def get_postgres_connection(conn_id: str):
    """Abre conexión Postgres vía Airflow PostgresHook (conn_id configurado en UI)."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        return PostgresHook(postgres_conn_id=conn_id).get_conn()
    except Exception as exc:
        raise RuntimeError(
            f"No se pudo abrir Postgres con conn_id={conn_id!r}. "
            "Configure la conexión tipo postgres en Airflow "
            "(host/ruta, puerto, schema, login, password)."
        ) from exc


def get_composer_params(cfg: Dict[str, Any] | None = None) -> Dict[str, str]:
    """Lee composer del entorno activo (environments.dev|qa|prod.composer)."""
    merged = dict(DEFAULT_COMPOSER)
    if not cfg:
        return merged
    env_spec = _active_env_spec(cfg)
    raw = env_spec.get("composer") or cfg.get("composer") or {}
    merged.update({str(k): str(v) for k, v in raw.items() if v is not None})
    return merged


def get_bigquery_client(cfg: Dict[str, Any] | None, project_id: str, location: str = "US"):
    """Crea el cliente BigQuery usando connections.google_cloud_default."""
    gcp_conn_id = get_connection_id(cfg, "google_cloud_default")
    try:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

        hook = BigQueryHook(gcp_conn_id=gcp_conn_id, location=location)
        return hook.get_client(project_id=project_id)
    except Exception:
        from google.cloud import bigquery

        return bigquery.Client(project=project_id, location=location)


def compute_execution_date_range(today: date | None = None) -> Dict[str, str]:
    """Calcula el rango de fechas del pipeline.

    - Inicio: 01 de enero de 2000
    - Fin: último día del año anterior a la fecha de ejecución
    """
    ref = today or date.today()
    prev_year = ref.year - 1
    end = date(prev_year, 12, 31)
    return {
        "start_date": PIPELINE_START_DATE.isoformat(),
        "end_date": end.isoformat(),
        "start_period": f"{PIPELINE_START_DATE.year}-{PIPELINE_START_DATE.month:02d}",
        "end_period": f"{end.year}-{end.month:02d}",
    }


def find_config_file(config_path: str | Path | None = None) -> Path | None:
    """Busca el archivo config.yaml en la ruta especificada o en ubicaciones estándar."""
    if config_path:
        p = Path(config_path)
        if p.exists():
            return p

    script_dir = Path(__file__).resolve().parent

    candidates = [
        Path("/home/airflow/gcs/dags/gdv_general_dbt_dag/config/config.yaml"),
        Path("dags/gdv_general_dbt_dag/config/config.yaml"),
        script_dir / "config" / "config.yaml",
        Path("/home/airflow/gcs/dags/gdv_general_dbt_dag/dags_valledata/config/config.yaml"),
        Path("config/config.yaml"),
        script_dir.parent / "config" / "config.yaml",
        script_dir.parents[1] / "config" / "config.yaml",
        Path("config.yaml"),
        script_dir.parents[1] / "config.yaml",
    ]
    for c in candidates:
        if c.exists():
            return c
    return None


def load_config(
    config_path: str | Path | None = None,
    env: str | None = None,
) -> Dict[str, Any]:
    """Carga y parsea el archivo de configuración YAML resolviendo el entorno (dev, qa, prod)."""
    file_path = find_config_file(config_path)
    raw_cfg = DEFAULT_CONFIG.copy()

    if file_path:
        try:
            import yaml
            with open(file_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if isinstance(data, dict):
                    raw_cfg = data
        except Exception as exc:
            print(f"⚠️ Advertencia al cargar {file_path}: {exc}. Usando defaults.")

    # Determinar el entorno activo
    target_env = env or os.environ.get("ENV") or raw_cfg.get("active_environment") or DEFAULT_ENV
    target_env = target_env.lower()

    envs_map = raw_cfg.get("environments", {})
    env_spec = envs_map.get(target_env) or envs_map.get(DEFAULT_ENV) or DEFAULT_CONFIG["environments"]["dev"]

    # Generar configuración resuelta con propiedades planas para fácil consumo
    resolved_cfg = {**raw_cfg}
    resolved_cfg["active_env"] = target_env
    resolved_cfg["env_spec"] = env_spec

    # Mapeo de atajos compatibles (nombres de archivo desde secciones de dominio)
    output_dir = env_spec.get("local_output_path")
    raw_dir = env_spec.get("local_raw_path")
    if not output_dir or not raw_dir:
        raise ValueError(
            "environments.<env>.local_raw_path y local_output_path son obligatorios en config.yaml"
        )

    sipsa_cfg = dict(resolved_cfg.get("sipsa") or {})
    oni_cfg = resolved_cfg.get("oni") or {}
    cultivos_cfg = resolved_cfg.get("cultivos") or {}

    date_range = compute_execution_date_range()
    resolved_cfg["date_range"] = date_range
    # Periodos SIPSA: fin = año anterior; inicio = max(pipeline, earliest_period DANE).
    start_period = date_range["start_period"]
    earliest = str(sipsa_cfg.get("earliest_period") or "").strip()
    if earliest and earliest > start_period:
        start_period = earliest
    sipsa_cfg["start_period"] = start_period
    sipsa_cfg["end_period"] = date_range["end_period"]
    sipsa_cfg["start_date"] = date_range["start_date"]
    sipsa_cfg["end_date"] = date_range["end_date"]
    resolved_cfg["sipsa"] = sipsa_cfg

    sipsa_out = sipsa_cfg.get("output_filename")
    oni_out = oni_cfg.get("output_filename")
    cultivos_out = cultivos_cfg.get("output_filename")
    consolidado_out = cultivos_cfg.get("consolidado_filename")
    if not all([sipsa_out, oni_out, cultivos_out, consolidado_out]):
        raise ValueError(
            "sipsa.output_filename, oni.output_filename, "
            "cultivos.output_filename y cultivos.consolidado_filename son obligatorios"
        )

    ckan_cfg = dict(resolved_cfg.get("ckan_comentarios") or {})
    ckan_out = ckan_cfg.get("output_filename") or "ckan_comentarios.csv"
    ckan_silver_out = ckan_cfg.get("silver_filename") or "silver_comentarios.csv"
    ckan_gold_out = ckan_cfg.get("gold_filename") or "gold_comentarios_sentimiento.csv"
    resolved_cfg["ckan_comentarios"] = ckan_cfg

    resolved_cfg["paths"] = {
        "raw_dir": raw_dir,
        "output_dir": output_dir,
        "sipsa_csv": f"{output_dir}/{sipsa_out}",
        "oni_csv": f"{output_dir}/{oni_out}",
        "cultivos_csv": f"{output_dir}/{cultivos_out}",
        "dataset_consolidado": f"{output_dir}/{consolidado_out}",
        "ckan_comentarios_csv": f"{output_dir}/{ckan_out}",
        "ckan_comentarios_silver_csv": f"{output_dir}/{ckan_silver_out}",
        "ckan_comentarios_gold_csv": f"{output_dir}/{ckan_gold_out}",
    }
    resolved_cfg["gcs_bucket"] = env_spec.get("gcs_bucket")
    resolved_cfg["bigquery"] = env_spec.get("bigquery", {})
    resolved_cfg["connections"] = get_connection_params({"env_spec": env_spec})
    resolved_cfg["composer"] = get_composer_params({"env_spec": env_spec})

    return resolved_cfg


def require_config_value(cfg: Dict[str, Any], *keys: str) -> Any:
    """Obtiene un valor anidado de config; falla si falta."""
    node: Any = cfg
    trail: list[str] = []
    for key in keys:
        trail.append(key)
        if not isinstance(node, dict) or key not in node or node[key] in (None, ""):
            raise KeyError(f"Falta parámetro de config: {'.'.join(trail)}")
        node = node[key]
    return node


def build_url(base_url: str, path: str) -> str:
    """Une base_url (conexión) con un path relativo de config."""
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"
