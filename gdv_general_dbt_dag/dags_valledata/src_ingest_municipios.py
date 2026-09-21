"""ETAPA INGEST MUNICIPIOS (src_ingest_municipios.py)
Descarga el dataset de Datos Geográficos de los Municipios del Valle del Cauca
(polígonos/posiciones, latitud, longitud, pisos térmicos, altura, temperatura)
desde el portal oficial datos.gov.co (dataset ID: iryd-wvq5) hacia el bucket de GCS /
almacenamiento local raw parametrizado en config.yaml.
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Dict
import urllib.request

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    build_url,
    get_composer_params,
    get_connection_base_url,
    get_connection_id,
    get_raw_root,
    load_config,
    require_config_value,
    storage_join,
    write_bytes,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)


def download_municipios_dataset(url: str, dest: str, cfg: Dict[str, Any] | None = None) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) pyimport-municipios/0.1"},
    )
    with urllib.request.urlopen(req, timeout=60.0) as resp:
        data = resp.read()
    return write_bytes(dest, data, cfg=cfg)


def run_ingest_municipios(config: Dict[str, Any] | None = None) -> str:
    cfg = config or load_config()
    municipios_cfg = require_config_value(cfg, "municipios")
    datos_gov_co = get_connection_id(cfg, "datos_gov_co")
    composer = get_composer_params(cfg)
    gcs_bucket = require_config_value(cfg, "gcs_bucket")

    raw_root = get_raw_root(cfg)
    raw_name = require_config_value(municipios_cfg, "raw_filename")
    dest_path = storage_join(raw_root, raw_name)

    config_base = require_config_value(municipios_cfg, "base_url")
    download_path = municipios_cfg.get("download_path", "/api/views/iryd-wvq5/rows.csv?accessType=DOWNLOAD")
    base_url = get_connection_base_url(datos_gov_co, default_host=config_base) or config_base
    url = build_url(base_url, download_path)

    print(
        f"🌐 [SRC_INGEST_MUNICIPIOS] Bucket Target: {gcs_bucket} | raw_root={raw_root} | "
        f"Composer={composer['environment']} ({composer['location']}) | conn={datos_gov_co}"
    )
    print(f"📥 Descargando datos de municipios del Valle desde {url}...")
    download_municipios_dataset(url, dest_path, cfg=cfg)

    print(f"✅ [SRC_INGEST_MUNICIPIOS] Guardado exitosamente en {dest_path}")
    return dest_path


if __name__ == "__main__":
    run_ingest_municipios()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    @dag(
        dag_id="src_ingest_municipios",
        description="Etapa Ingest Municipios Valle (Descarga CSV datos.gov.co iryd-wvq5)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "municipios", "ingest", "connection:datos_gov_co"],
        **get_airflow_dag_kwargs(),
    )
    def ingest_municipios_dag():
        @task(task_id="run_ingest_municipios")
        def execute_ingest() -> str:
            return run_with_airflow_alarm(run_ingest_municipios)

        execute_ingest()

    dag = ingest_municipios_dag()
except ImportError:
    pass
