"""ETAPA INGEST ONI (src_ingest_oni.py)
Descarga la tabla del índice climático ONI desde el portal NOAA CPC
hacia el bucket de GCS / almacenamiento local raw parametrizado en config.yaml.
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
    write_text,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)


def download_oni_html(url: str, dest: str, cfg: Dict[str, Any] | None = None) -> str:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    )
    with urllib.request.urlopen(req, timeout=60.0) as resp:
        text = resp.read().decode("utf-8", errors="ignore")
    return write_text(dest, text, cfg=cfg)


def run_ingest_oni(config: Dict[str, Any] | None = None) -> str:
    cfg = config or load_config()
    oni_cfg = require_config_value(cfg, "oni")
    noaa_oni = get_connection_id(cfg, "noaa_oni")
    composer = get_composer_params(cfg)
    gcs_bucket = require_config_value(cfg, "gcs_bucket")

    raw_root = get_raw_root(cfg)
    html_dest = storage_join(raw_root, require_config_value(oni_cfg, "raw_filename"))
    
    config_base = require_config_value(oni_cfg, "base_url")
    path = require_config_value(oni_cfg, "path")
    base_url = get_connection_base_url(noaa_oni, default_host=config_base) or config_base
    url = build_url(base_url, path)

    print(
        f"🌐 [SRC_INGEST_ONI] Target: {gcs_bucket} | raw_root={raw_root} | Base URL: {base_url} | "
        f"Composer={composer['environment']} ({composer['location']}) | conn={noaa_oni}"
    )
    print(f"📥 Descargando tabla ONI desde {url} hacia {html_dest}...")
    download_oni_html(url=url, dest=html_dest, cfg=cfg)

    print(f"✅ [SRC_INGEST_ONI] Descarga completada en {html_dest}")
    return html_dest


if __name__ == "__main__":
    run_ingest_oni()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    @dag(
        dag_id="src_ingest_oni",
        description="Etapa Ingest Clima ONI (Descarga HTML NOAA)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "oni", "ingest", "connection:noaa_oni"],
        **get_airflow_dag_kwargs(),
    )
    def ingest_oni_dag():
        @task(task_id="run_ingest_oni")
        def execute_ingest() -> str:
            return run_with_airflow_alarm(run_ingest_oni)

        execute_ingest()

    dag = ingest_oni_dag()
except ImportError:
    pass
