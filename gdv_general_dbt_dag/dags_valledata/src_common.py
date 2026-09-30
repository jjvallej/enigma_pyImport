"""Runtime común desplegable en Composer (nombre src_*).

Incluye carga de config.yaml, conexiones Airflow y helpers usados por los scripts src_*.
No depende del paquete pyimport.
"""

from __future__ import annotations

from datetime import date
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict
import unicodedata


DEFAULT_ENV = "dev"

# Fecha inicial fija del rango de datos del pipeline.
PIPELINE_START_DATE = date(2000, 1, 1)


DEFAULT_CONNECTIONS: Dict[str, Any] = {
    "sipsa_dane": "sipsa_dane",
    "noaa_oni": "noaa_oni",
    "gobernacion_valle": "gobernacion_valle",
    "google_cloud_default": "google_cloud_default",
    # Arreglo de conexiones Postgres CKAN (hasta 14); se completa en config.yaml.
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
        "test_data_records": 1200,
        "test_data_seed": 42,
    },
}


def clean_str(text: Any) -> str:
    """Normaliza texto: elimina asteriscos, quita acentos/tildes vía Unicode NFKD,
    remueve espacios extra y convierte a minúsculas."""
    if not isinstance(text, str) or not text:
        return ""
    text = re.sub(r"[*]+", "", text)
    nfkd = unicodedata.normalize("NFKD", text)
    without_accents = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(without_accents.strip().split()).lower()


def normalize_municipio(text: Any) -> str:
    """Normaliza nombres de municipios para asegurar coincidencias en cruces/enriquecimientos."""
    return clean_str(text)


def normalize_cultivo(text: Any) -> str:
    """Normaliza nombres de cultivos para asegurar coincidencias en cruces/enriquecimientos."""
    return clean_str(text)


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
    """Busca config.yaml priorizando la ruta central del repositorio / Cloud Composer ('gdv_general_dbt_dag/config/config.yaml')."""
    if config_path:
        p = Path(config_path)
        if p.exists():
            return p

    candidates: List[Path | None] = []

    # 1. Ruta absoluta oficial en Cloud Composer bucket (us-east1-composer-gdv-edf456f5-bucket/dags/gdv_general_dbt_dag/config/config.yaml)
    candidates.extend([
        Path("/home/airflow/gcs/dags/gdv_general_dbt_dag/config/config.yaml"),
        Path("/home/airflow/gcs/dags/gdv_general_dbt_dag/dags_valledata/config/config.yaml"),
        Path("/home/airflow/gcs/dags/dags_valledata/config/config.yaml"),
    ])

    # 2. Relativa a la ubicación del script (dags_valledata/.. -> gdv_general_dbt_dag/config/config.yaml)
    try:
        script_dir = Path(__file__).resolve().parent
        candidates.append(script_dir.parent / "config" / "config.yaml")
        candidates.append(script_dir / "config" / "config.yaml")
        candidates.append(script_dir / "config.yaml")
    except Exception:
        script_dir = None

    # 3. Al mismo nivel del DAG principal en ejecución
    try:
        main_mod = sys.modules.get("__main__")
        if main_mod and hasattr(main_mod, "__file__") and main_mod.__file__:
            main_dir = Path(main_mod.__file__).resolve().parent
            candidates.append(main_dir.parent / "config" / "config.yaml")
            candidates.append(main_dir / "config" / "config.yaml")
    except Exception:
        pass

    # 4. Rutas relativas del repositorio local
    candidates.extend([
        Path("dags/gdv_general_dbt_dag/config/config.yaml"),
        Path("dags/gdv_general_dbt_dag/dags_valledata/config/config.yaml"),
        Path("config/config.yaml"),
        Path("config.yaml"),
    ])

    for c in candidates:
        if c is not None and c.exists():
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
    valledata_block = raw_cfg.get("valledata")
    if isinstance(valledata_block, dict):
        for k, v in valledata_block.items():
            if k not in resolved_cfg or not resolved_cfg[k]:
                resolved_cfg[k] = v
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
    ckan_silver_out = ckan_cfg.get("silver_filename") or "comentarios_silver.csv"
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
    }
    resolved_cfg["gcs_bucket"] = env_spec.get("gcs_bucket")
    resolved_cfg["bigquery"] = env_spec.get("bigquery", {})
    resolved_cfg["connections"] = get_connection_params({"env_spec": env_spec})
    resolved_cfg["composer"] = get_composer_params({"env_spec": env_spec})

    return resolved_cfg


def require_config_value(cfg: Dict[str, Any], *keys: str) -> Any:
    """Obtiene un valor anidado de config; falla si falta."""
    node: Any = cfg
    if keys and isinstance(cfg, dict) and keys[0] not in cfg and "valledata" in cfg and isinstance(cfg["valledata"], dict) and keys[0] in cfg["valledata"]:
        node = cfg["valledata"]
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

def get_connection_base_url(conn_id: str, default_host: str | None = None) -> str:
    """Obtiene la URL base desde una Conexión HTTP de Airflow, o el host por defecto."""
    host = (default_host or "").strip().rstrip("/")
    try:
        try:
            from airflow.sdk.bases.hook import BaseHook
        except ImportError:
            from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        if conn and conn.host:
            value = conn.host.strip()
            if not value.startswith(("http://", "https://")):
                schema = (conn.schema or "https").strip()
                value = f"{schema}://{value}"
            return value.rstrip("/")
    except Exception:
        pass
    return host

def running_in_composer() -> bool:
    """True cuando el código corre en Airflow/Composer (no en CLI local)."""
    return bool(
        os.environ.get("COMPOSER_ENVIRONMENT")
        or os.environ.get("AIRFLOW_CTX_DAG_ID")
        or os.environ.get("AIRFLOW_CTX_TASK_ID")
    )


def get_raw_root(cfg: Dict[str, Any]) -> str:
    """Raíz de raw: gcs_bucket (staging de ingesta) en Composer, o local_raw_path en local."""
    if running_in_composer():
        return str(require_config_value(cfg, "gcs_bucket")).rstrip("/")
    return str(require_config_value(cfg, "paths", "raw_dir")).rstrip("/")


def storage_join(root: str, *parts: str) -> str:
    """Une raíz local o gs:// con segmentos de ruta."""
    cleaned = "/".join(str(p).strip("/").replace("\\", "/") for p in parts if str(p).strip("/"))
    if not cleaned:
        return root.rstrip("/")
    if root.startswith("gs://"):
        return f"{root.rstrip('/')}/{cleaned}"
    return str(Path(root) / cleaned)


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"URI GCS inválida: {uri}")
    without = uri[5:]
    bucket, _, blob = without.partition("/")
    if not bucket or not blob:
        raise ValueError(f"URI GCS incompleta (falta objeto): {uri}")
    return bucket, blob


def _gcs_client(cfg: Dict[str, Any] | None = None):
    try:
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        conn_id = get_connection_id(cfg, "google_cloud_default") if cfg else "google_cloud_default"
        return GCSHook(gcp_conn_id=conn_id).get_conn()
    except Exception:
        from google.cloud import storage

        return storage.Client()


def storage_exists(uri_or_path: str, cfg: Dict[str, Any] | None = None) -> bool:
    if uri_or_path.startswith("gs://"):
        bucket_name, blob_name = _parse_gcs_uri(uri_or_path)
        client = _gcs_client(cfg)
        return client.bucket(bucket_name).blob(blob_name).exists()
    return Path(uri_or_path).exists()


def write_bytes(uri_or_path: str, data: bytes, cfg: Dict[str, Any] | None = None) -> str:
    """Escribe bytes en local o GCS. Local: sin mkdir; si existe, unlink y recrea."""
    if uri_or_path.startswith("gs://"):
        bucket_name, blob_name = _parse_gcs_uri(uri_or_path)
        client = _gcs_client(cfg)
        blob = client.bucket(bucket_name).blob(blob_name)
        blob.upload_from_string(data)
        return uri_or_path

    path = Path(uri_or_path)
    if path.exists():
        path.unlink()
    path.write_bytes(data)
    return str(path)


def write_text(uri_or_path: str, text: str, encoding: str = "utf-8", cfg: Dict[str, Any] | None = None) -> str:
    return write_bytes(uri_or_path, text.encode(encoding), cfg=cfg)


def read_bytes(uri_or_path: str, cfg: Dict[str, Any] | None = None) -> bytes:
    if uri_or_path.startswith("gs://"):
        bucket_name, blob_name = _parse_gcs_uri(uri_or_path)
        client = _gcs_client(cfg)
        return client.bucket(bucket_name).blob(blob_name).download_as_bytes()
    return Path(uri_or_path).read_bytes()


def materialize_local(uri_or_path: str, cfg: Dict[str, Any] | None = None) -> Path:
    """Devuelve un Path local; si es gs://, descarga a un temp conservando el nombre original.

    Importante: el basename del URI (p. ej. anex_mensual_ene_2024.xlsx) se preserva
    porque load SIPSA/cultivos parsean mes/año desde el nombre del archivo.
    """
    if not uri_or_path.startswith("gs://"):
        return Path(uri_or_path)

    import tempfile

    original_name = Path(uri_or_path).name or "download.bin"
    # Sufijo aleatorio en el directorio, nombre final = original (evita colisiones).
    tmp_dir = Path(tempfile.mkdtemp(prefix="pyimport_"))
    path = tmp_dir / original_name
    path.write_bytes(read_bytes(uri_or_path, cfg=cfg))
    return path


def ensure_xlrd():
    """Importa xlrd; si no está en el entorno Composer, usa src_xlrd_vendor.zip junto a los DAGs."""
    try:
        import xlrd

        return xlrd
    except ImportError:
        pass

    vendor = Path(__file__).resolve().parent / "src_xlrd_vendor.zip"
    if not vendor.exists():
        raise ImportError(
            "Falta xlrd para leer archivos .xls. "
            "Despliegue src_xlrd_vendor.zip junto a los scripts src_* o instale xlrd en Composer."
        )
    vendor_str = str(vendor)
    if vendor_str not in sys.path:
        sys.path.insert(0, vendor_str)
    import xlrd

    return xlrd


def get_bq_table_ref(cfg: Dict[str, Any], dataset_key: str, table_key: str) -> str:
    """Arma project.dataset.table desde config (p. ej. bronze + sipsa_bronze)."""
    bq_cfg = require_config_value(cfg, "bigquery")
    project_id = require_config_value(bq_cfg, "project_id")
    dataset = require_config_value(bq_cfg, "datasets", dataset_key)
    table = require_config_value(bq_cfg, "tables", table_key)
    return f"{project_id}.{dataset}.{table}"


def read_bq_dataframe(cfg: Dict[str, Any], table_ref: str):
    """Lee una tabla BigQuery a pandas.DataFrame."""
    import pandas as pd

    bq_cfg = require_config_value(cfg, "bigquery")
    project_id = require_config_value(bq_cfg, "project_id")
    location = require_config_value(bq_cfg, "location")
    client = get_bigquery_client(cfg, project_id, location)
    query = f"SELECT * FROM `{table_ref}`"
    print(f"📥 [BQ] Leyendo {table_ref}...", flush=True)
    return client.query(query).to_dataframe(create_bqstorage_client=False)


def list_storage(root: str, name_prefix: str = "", cfg: Dict[str, Any] | None = None) -> list[str]:
    """Lista objetos bajo root (local o gs://). name_prefix filtra por prefijo de nombre."""
    if root.startswith("gs://"):
        without = root[5:]
        bucket_name, _, prefix = without.partition("/")
        prefix = f"{prefix.rstrip('/')}/" if prefix else ""
        client = _gcs_client(cfg)
        uris: list[str] = []
        for blob in client.list_blobs(bucket_name, prefix=prefix):
            relative = blob.name[len(prefix):] if prefix and blob.name.startswith(prefix) else blob.name
            if not relative or "/" in relative:
                continue
            if name_prefix and not relative.startswith(name_prefix):
                continue
            uris.append(f"gs://{bucket_name}/{blob.name}")
        return sorted(uris)

    path = Path(root)
    if not path.exists():
        return []
    pattern = f"{name_prefix}*" if name_prefix else "*"
    return [str(p) for p in sorted(path.glob(pattern)) if p.is_file()]


# ---------------------------------------------------------------------------
# Alarmas / alertas Airflow
# ---------------------------------------------------------------------------

DEFAULT_AIRFLOW_ALERTS: Dict[str, Any] = {
    "enabled": True,
    "email_on_failure": True,
    "emails": [],
    "retries": 1,
    "retry_delay_minutes": 5,
    # Estados de resultado dict que disparan alarma (además de excepciones).
    "fail_statuses": ["ERROR", "PARTIAL", "FAILED"],
}


def get_airflow_alerts(cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Lee bloque airflow_alerts (global o por entorno)."""
    merged = dict(DEFAULT_AIRFLOW_ALERTS)
    if not cfg:
        return merged
    env_spec = _active_env_spec(cfg)
    raw = {}
    if isinstance(env_spec, dict) and isinstance(env_spec.get("airflow_alerts"), dict):
        raw = env_spec["airflow_alerts"]
    elif isinstance(cfg.get("airflow_alerts"), dict):
        raw = cfg["airflow_alerts"]
    for key, value in raw.items():
        if value is not None:
            merged[key] = value
    return merged


def _format_alarm_message(
    *,
    dag_id: str,
    task_id: str,
    run_id: str,
    exception: Any,
    try_number: Any = None,
) -> str:
    return (
        f"ALARM ValleDATA | dag={dag_id} | task={task_id} | run_id={run_id} | "
        f"try={try_number} | error={exception}"
    )


def airflow_failure_alarm(context: Dict[str, Any]) -> None:
    """Callback Airflow on_failure: registra alarma y notifica por email si está configurado.

    Composer/Airflow marca la task como failed; este callback emite un log CRITICAL
    con prefijo ALARM (útil para alertas de Cloud Logging) y opcionalmente email.
    """
    import logging

    logger = logging.getLogger("valledata.airflow_alarm")
    dag = context.get("dag")
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "unknown_dag"
    task_id = getattr(ti, "task_id", None) or context.get("task_id") or "unknown_task"
    run_id = context.get("run_id") or getattr(ti, "run_id", None) or "unknown_run"
    try_number = getattr(ti, "try_number", None)
    exception = context.get("exception")
    msg = _format_alarm_message(
        dag_id=str(dag_id),
        task_id=str(task_id),
        run_id=str(run_id),
        exception=exception,
        try_number=try_number,
    )
    logger.critical(msg)
    print(f"🚨 {msg}", flush=True)

    # Email opcional (requiere SMTP configurado en Airflow/Composer)
    try:
        cfg = load_config()
        alerts = get_airflow_alerts(cfg)
        if not alerts.get("enabled", True):
            return
        emails = list(alerts.get("emails") or [])
        if not emails or not alerts.get("email_on_failure", True):
            return
        from airflow.utils.email import send_email

        subject = f"[ALARM] ValleDATA falló: {dag_id}.{task_id}"
        body = (
            f"<h3>Alarma de pipeline ValleDATA</h3>"
            f"<p><b>DAG:</b> {dag_id}<br/>"
            f"<b>Task:</b> {task_id}<br/>"
            f"<b>Run:</b> {run_id}<br/>"
            f"<b>Try:</b> {try_number}<br/>"
            f"<b>Error:</b> {exception}</p>"
            f"<pre>{exception}</pre>"
        )
        send_email(to=emails, subject=subject, html_content=body)
        print(f"📧 [ALARM] Email enviado a {emails}", flush=True)
    except Exception as notify_exc:
        print(f"⚠️ [ALARM] No se pudo notificar por email: {notify_exc}", flush=True)


def raise_airflow_alarm(message: str, *, cause: BaseException | None = None) -> None:
    """Lanza excepción que falla la task Airflow y dispara on_failure_callback."""
    text = f"ALARM ValleDATA | {message}"
    print(f"🚨 {text}", flush=True)
    try:
        from airflow.exceptions import AirflowException

        if cause is not None:
            raise AirflowException(text) from cause
        raise AirflowException(text)
    except ImportError:
        if cause is not None:
            raise RuntimeError(text) from cause
        raise RuntimeError(text)


def raise_if_failed(result: Any, *, context_label: str = "pipeline") -> Any:
    """Si el resultado es un dict con status de fallo, lanza alarma Airflow."""
    if not isinstance(result, dict):
        return result
    status = str(result.get("status") or "").upper()
    alerts = get_airflow_alerts()
    fail_statuses = {str(s).upper() for s in (alerts.get("fail_statuses") or [])}
    if status in fail_statuses:
        errors = result.get("errors") or result.get("error") or ""
        raise_airflow_alarm(
            f"{context_label} status={status} | errors={errors} | result={result}"
        )
    # En Composer, SIMULATED de carga BQ también es fallo operativo
    if running_in_composer() and status == "SIMULATED":
        raise_airflow_alarm(
            f"{context_label} status=SIMULATED en Composer (falló integración real) | result={result}"
        )
    return result


def run_with_airflow_alarm(fn, *args, context_label: str | None = None, **kwargs):
    """Ejecuta fn y convierte errores / status fallido en alarma Airflow."""
    label = context_label or getattr(fn, "__name__", "task")
    try:
        result = fn(*args, **kwargs)
        return raise_if_failed(result, context_label=label)
    except Exception as exc:
        # Re-lanza como AirflowException para asegurar callback de fallo
        if exc.__class__.__name__ == "AirflowException":
            raise
        raise_airflow_alarm(f"{label} falló: {exc}", cause=exc)


def get_airflow_dag_kwargs(cfg: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Kwargs comunes para @dag: default_args con alarma on_failure + retries."""
    from datetime import timedelta

    alerts = get_airflow_alerts(cfg)
    emails = list(alerts.get("emails") or [])
    default_args: Dict[str, Any] = {
        "owner": "valledata",
        "depends_on_past": False,
        "retries": int(alerts.get("retries") or 1),
        "retry_delay": timedelta(minutes=int(alerts.get("retry_delay_minutes") or 5)),
        "email_on_failure": bool(alerts.get("email_on_failure", True)) and bool(emails),
        "email_on_retry": False,
        "on_failure_callback": airflow_failure_alarm,
    }
    if emails:
        default_args["email"] = emails
    return {"default_args": default_args}

