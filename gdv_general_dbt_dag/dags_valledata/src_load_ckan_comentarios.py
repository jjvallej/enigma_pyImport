"""ETAPA LOAD CKAN COMENTARIOS (src_load_ckan_comentarios.py)

Lee TODOS los CSV de staging (uno por municipio/conexión CKAN), los consolida
en UN SOLO archivo CSV y carga BigQuery Bronze:
  datagov-477214.valledata.bronze_comentarios

Salida local (config): data/ckan_comentarios.csv
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Dict, List

import pandas as pd

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    get_bigquery_client,
    get_composer_params,
    get_connection_id,
    get_raw_root,
    list_storage,
    load_config,
    materialize_local,
    require_config_value,
    storage_join,
    write_text,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)


def _list_comment_files(cfg: Dict[str, Any]) -> List[str]:
    """Lista todos los CSV de comentarios en staging (todos los municipios)."""
    domain = require_config_value(cfg, "ckan_comentarios")
    raw_subdir = str(require_config_value(domain, "raw_subdir"))
    raw_root = storage_join(get_raw_root(cfg), raw_subdir)
    files = list_storage(raw_root, name_prefix="comment_", cfg=cfg)
    if not files:
        files = [u for u in list_storage(raw_root, cfg=cfg) if u.lower().endswith(".csv")]
    return sorted(files)


def consolidate_comment_csvs(file_uris: List[str], cfg: Dict[str, Any]) -> pd.DataFrame:
    """Consolida todos los CSV municipales en un único DataFrame."""
    frames: List[pd.DataFrame] = []
    for uri in file_uris:
        local = materialize_local(uri, cfg=cfg)
        frame = pd.read_csv(local, dtype=str, keep_default_na=False)
        frame["source_file"] = Path(uri).name
        if "municipio" not in frame.columns:
            # Deriva municipio del nombre comment_<conn>.csv si falta
            stem = Path(uri).stem.replace("comment_", "", 1)
            frame["municipio"] = stem
        if "source_conn_id" not in frame.columns:
            frame["source_conn_id"] = Path(uri).stem.replace("comment_", "", 1)
        frames.append(frame)
        n_mun = frame["municipio"].nunique() if "municipio" in frame.columns else 0
        print(
            f"📄 [LOAD] {Path(uri).name} -> {len(frame)} filas | municipios_en_archivo={n_mun}",
            flush=True,
        )

    if not frames:
        return pd.DataFrame()

    consolidated = pd.concat(frames, ignore_index=True, sort=False)
    # Orden estable: municipio, luego fecha si existe
    sort_cols = [c for c in ("municipio", "created_at", "id") if c in consolidated.columns]
    if sort_cols:
        consolidated = consolidated.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    return consolidated


def _write_consolidated_csv(df: pd.DataFrame, output_csv: Path, cfg: Dict[str, Any]) -> str:
    """Escribe un único CSV consolidado (local y, si aplica, también URI de staging)."""
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    csv_text = df.to_csv(index=False)
    output_csv.write_text(csv_text, encoding="utf-8")

    # Copia opcional al staging/raw para trazabilidad
    domain = require_config_value(cfg, "ckan_comentarios")
    raw_subdir = str(require_config_value(domain, "raw_subdir"))
    staging_copy = storage_join(
        get_raw_root(cfg),
        raw_subdir,
        "comentarios_consolidado.csv",
    )
    write_text(staging_copy, csv_text, cfg=cfg)
    return staging_copy


def run_load_ckan_comentarios(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    paths_cfg = require_config_value(cfg, "paths")
    bq_cfg = require_config_value(cfg, "bigquery")
    google_cloud_default = get_connection_id(cfg, "google_cloud_default")
    composer = get_composer_params(cfg)

    output_csv = Path(require_config_value(paths_cfg, "ckan_comentarios_csv"))
    files = _list_comment_files(cfg)

    print(
        f"📦 [SRC_LOAD_CKAN_COMENTARIOS] Consolidando {len(files)} archivos municipales "
        f"-> UN CSV: {output_csv} | "
        f"Composer={composer['environment']} ({composer['location']}) | "
        f"conn={google_cloud_default}",
        flush=True,
    )

    if not files:
        print(
            "⏭️ [SRC_LOAD_CKAN_COMENTARIOS] SKIPPED: no hay CSV en staging. "
            "Ejecute src_ingest_ckan_comentarios primero."
        )
        return {"status": "SKIPPED", "files": 0, "rows": 0, "municipios": 0}

    df = consolidate_comment_csvs(files, cfg)
    staging_copy = _write_consolidated_csv(df, output_csv, cfg)

    municipios = sorted(df["municipio"].dropna().unique().tolist()) if "municipio" in df.columns else []
    print(
        f"🧩 [LOAD] CSV único consolidado | filas={len(df)} | "
        f"municipios={len(municipios)} -> {municipios} | "
        f"salida={output_csv} | staging={staging_copy}",
        flush=True,
    )
    if "municipio" in df.columns:
        counts = df["municipio"].value_counts().to_dict()
        for mun, n in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"   - {mun}: {n} comentarios", flush=True)

    project_id = require_config_value(bq_cfg, "project_id")
    dataset_bronze = require_config_value(bq_cfg, "datasets", "bronze")
    table_bronze = require_config_value(bq_cfg, "tables", "comentarios_bronze")
    table_ref = f"{project_id}.{dataset_bronze}.{table_bronze}"

    result: Dict[str, Any] = {
        "files": len(files),
        "rows": int(len(df)),
        "municipios": len(municipios),
        "municipio_list": municipios,
        "output": str(output_csv),
        "staging_copy": staging_copy,
        "bigquery_table": table_ref,
    }

    try:
        from google.cloud import bigquery

        client = get_bigquery_client(cfg, project_id, require_config_value(bq_cfg, "location"))
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(output_csv, "rb") as sf:
            job = client.load_table_from_file(sf, table_ref, job_config=job_config)
        job.result()
        result["status"] = "SUCCESS"
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación Load BQ CKAN comentarios] {exc}")
        result["status"] = "SIMULATED"

    print(
        f"✅ [SRC_LOAD_CKAN_COMENTARIOS] Un CSV consolidado listo | "
        f"filas={result['rows']} | municipios={result['municipios']} | "
        f"archivo={output_csv} | tabla={table_ref} | status={result['status']}"
    )
    return result


if __name__ == "__main__":
    run_load_ckan_comentarios()

# Airflow DAG (Composer 3: airflow.sdk; Composer 2: airflow.decorators).
try:
    try:
        from airflow.sdk import dag as _af_dag, task as _af_task
    except ImportError:
        from airflow.decorators import dag as _af_dag, task as _af_task
except ImportError:
    _af_dag = _af_task = None

if _af_dag is not None:
    from datetime import datetime

    @_af_dag(
        dag_id="src_load_ckan_comentarios",
        description="Consolida comentarios de todos los municipios en un CSV y carga Bronze",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "ckan", "comentarios", "load"],
        **get_airflow_dag_kwargs(),
    )
    def load_ckan_comentarios_dag():
        @_af_task(task_id="run_load_ckan_comentarios")
        def execute_load() -> Dict[str, Any]:
            return run_with_airflow_alarm(run_load_ckan_comentarios)

        execute_load()

    dag = load_ckan_comentarios_dag()
    DAG = dag  # noqa: N816 — token de descubrimiento de Airflow
