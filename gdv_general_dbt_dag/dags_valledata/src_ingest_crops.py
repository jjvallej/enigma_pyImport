"""ETAPA INGEST CULTIVOS (src_ingest_crops.py)
Descarga los datasets de cultivos permanentes y transitorios desde el portal web oficial
de la Gobernación del Valle del Cauca hacia el bucket de GCS / almacenamiento local raw
parametrizado en config.yaml.
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


def download_cultivo_dataset(url: str, dest: str, cfg: Dict[str, Any] | None = None) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "pyimport-cultivos/0.1"})
    with urllib.request.urlopen(req, timeout=60.0) as resp:
        data = resp.read()
    return write_bytes(dest, data, cfg=cfg)


def run_ingest_crops(config: Dict[str, Any] | None = None) -> Dict[str, str]:
    cfg = config or load_config()
    cultivos_cfg = require_config_value(cfg, "cultivos")
    gobernacion_valle = get_connection_id(cfg, "gobernacion_valle")
    composer = get_composer_params(cfg)
    gcs_bucket = require_config_value(cfg, "gcs_bucket")

    raw_root = get_raw_root(cfg)

    perm_name = require_config_value(cultivos_cfg, "permanentes_filename")
    trans_name = require_config_value(cultivos_cfg, "transitorios_filename")
    perm_path = require_config_value(cultivos_cfg, "permanentes_path")
    trans_path = require_config_value(cultivos_cfg, "transitorios_path")

    config_base = require_config_value(cultivos_cfg, "base_url")
    base_url = get_connection_base_url(gobernacion_valle, default_host=config_base) or config_base

    perm_url = build_url(base_url, perm_path)
    trans_url = build_url(base_url, trans_path)
    perm_dest = storage_join(raw_root, perm_name)
    trans_dest = storage_join(raw_root, trans_name)

    print(
        f"🌐 [SRC_INGEST_CROPS] Bucket Target: {gcs_bucket} | raw_root={raw_root} | "
        f"Composer={composer['environment']} ({composer['location']}) | conn={gobernacion_valle}"
    )
    print(f"📥 Descargando cultivos permanentes desde {perm_url}...")
    download_cultivo_dataset(perm_url, perm_dest, cfg=cfg)

    print(f"📥 Descargando cultivos transitorios desde {trans_url}...")
    download_cultivo_dataset(trans_url, trans_dest, cfg=cfg)

    print(f"✅ [SRC_INGEST_CROPS] Guardados en {perm_dest} y {trans_dest}")
    return {"permanentes": perm_dest, "transitorios": trans_dest}


if __name__ == "__main__":
    run_ingest_crops()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    @dag(
        dag_id="src_ingest_crops",
        description="Etapa Ingest Cultivos Valle (Descarga CSVs)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "cultivos", "ingest", "connection:gobernacion_valle"],
        **get_airflow_dag_kwargs(),
    )
    def ingest_crops_dag():
        @task(task_id="run_ingest_crops")
        def execute_ingest() -> dict[str, str]:
            return run_with_airflow_alarm(run_ingest_crops)

        execute_ingest()

    dag = ingest_crops_dag()
except ImportError:
    pass
