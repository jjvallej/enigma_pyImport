"""ETAPA LOAD MUNICIPIOS (src_load_municipios.py)
Lee los datos de municipios descargados en la etapa de ingesta (data/raw/municipios_valle.csv),
procesa y limpia los atributos geográficos (latitud, longitud, pisos térmicos, altura, temperatura),
guarda el CSV limpio local/staging y carga la tabla Bronze en BigQuery:
  datagov-477214.valledata.bronze_municipios_valle
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
import sys
from typing import Any, Dict, List
import unicodedata

import pandas as pd

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    clean_str,
    normalize_municipio,
    get_bigquery_client,
    get_bq_table_ref,
    get_composer_params,
    get_connection_id,
    get_raw_root,
    load_config,
    materialize_local,
    require_config_value,
    storage_exists,
    storage_join,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)


def parse_numeric(val: Any) -> float | None:
    if pd.isna(val) or val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace(" ", "").replace('"', "")
    if not s or s == "-":
        return None
    s = s.replace(".", "").replace(",", ".") if s.count(".") > 1 else s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def clean_municipio_name(text: Any) -> str:
    if not isinstance(text, str) or not text:
        return ""
    text = re.sub(r"[*]+", "", text)
    return " ".join(text.strip().split())


def process_municipios_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    normalized_cols = {
        str(c).strip().lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n"): c
        for c in df.columns
    }

    def find_col(*candidates: str) -> str:
        # 1. Exact match search
        for cand in candidates:
            cand_norm = cand.strip().lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
            for key, orig in normalized_cols.items():
                if cand_norm == key:
                    return orig
        # 2. Substring search
        for cand in candidates:
            cand_norm = cand.strip().lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
            for key, orig in normalized_cols.items():
                if cand_norm in key:
                    return orig
        raise KeyError(f"No se encontró ninguna columna entre {candidates}. Disponibles: {list(df.columns)}")

    col_code = find_col("codigo de municipio", "codigo_municipio", "cod_municipio")
    col_mun = find_col("municipio")
    col_lat = find_col("latitud")
    col_lon = find_col("longitud")
    col_calido = find_col("superficie tipo de piso calido", "piso calido")
    col_medio = find_col("superficie tipo de piso medio", "piso medio")
    col_frio = find_col("superficie tipo de piso frio", "piso frio")
    col_paramo = find_col("superficie tipo de piso paramo", "piso paramo")
    col_altura = find_col("altura sobre nivel del mar", "altura")
    col_temp = find_col("temperatura media", "temperatura")

    out = pd.DataFrame()
    out["codigo_municipio"] = pd.to_numeric(df[col_code], errors="coerce").fillna(0).astype(int)
    out["municipio"] = df[col_mun].apply(clean_municipio_name)
    out["clean_mun"] = df[col_mun].apply(normalize_municipio)
    out["latitud"] = df[col_lat].apply(parse_numeric)
    out["longitud"] = df[col_lon].apply(parse_numeric)
    out["superficie_piso_calido"] = df[col_calido].apply(parse_numeric).fillna(0.0)
    out["superficie_piso_medio"] = df[col_medio].apply(parse_numeric).fillna(0.0)
    out["superficie_piso_frio"] = df[col_frio].apply(parse_numeric).fillna(0.0)
    out["superficie_piso_paramo"] = df[col_paramo].apply(parse_numeric).fillna(0.0)
    out["altura_snm"] = df[col_altura].apply(parse_numeric)
    out["temperatura_media"] = df[col_temp].apply(parse_numeric)

    return out


def run_load_municipios(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    paths_cfg = cfg.get("paths", {})
    municipios_cfg = require_config_value(cfg, "municipios")
    bq_cfg = require_config_value(cfg, "bigquery")
    composer = get_composer_params(cfg)
    gcs_bucket = require_config_value(cfg, "gcs_bucket")

    raw_root = get_raw_root(cfg)
    raw_name = require_config_value(municipios_cfg, "raw_filename")
    raw_uri = storage_join(raw_root, raw_name)

    if not storage_exists(raw_uri, cfg=cfg):
        raise FileNotFoundError(f"Archivo raw no encontrado en {raw_uri}. Ejecute primero src_ingest_municipios.")

    raw_file = materialize_local(raw_uri, cfg=cfg)
    print(f"📥 [SRC_LOAD_MUNICIPIOS] Leyendo municipios desde {raw_file}...", flush=True)

    df_raw = pd.read_csv(raw_file, encoding="utf-8", low_memory=False)
    df_clean = process_municipios_dataframe(df_raw)

    output_dir = Path(paths_cfg.get("output_dir", "data"))
    output_dir.mkdir(parents=True, exist_ok=True)
    out_name = municipios_cfg.get("output_filename", "municipios_valle_clean.csv")
    output_csv = output_dir / out_name
    df_clean.to_csv(output_csv, index=False, encoding="utf-8")

    table_ref = get_bq_table_ref(cfg, "bronze", "municipios_bronze")
    print(
        f"✨ [SRC_LOAD_MUNICIPIOS] Processed {len(df_clean)} municipios -> {output_csv} | BQ={table_ref} | "
        f"Composer={composer['environment']}",
        flush=True,
    )

    try:
        from google.cloud import bigquery

        client = get_bigquery_client(
            cfg,
            require_config_value(bq_cfg, "project_id"),
            require_config_value(bq_cfg, "location"),
        )
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("codigo_municipio", "INTEGER"),
                bigquery.SchemaField("municipio", "STRING"),
                bigquery.SchemaField("latitud", "FLOAT"),
                bigquery.SchemaField("longitud", "FLOAT"),
                bigquery.SchemaField("superficie_piso_calido", "FLOAT"),
                bigquery.SchemaField("superficie_piso_medio", "FLOAT"),
                bigquery.SchemaField("superficie_piso_frio", "FLOAT"),
                bigquery.SchemaField("superficie_piso_paramo", "FLOAT"),
                bigquery.SchemaField("altura_snm", "FLOAT"),
                bigquery.SchemaField("temperatura_media", "FLOAT"),
            ],
        )
        with output_csv.open("rb") as sf:
            job = client.load_table_from_file(sf, table_ref, job_config=job_config)
        job.result()
        table = client.get_table(table_ref)
        status, nrows = "SUCCESS", table.num_rows
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación BQ Bronze Municipios] {exc}", flush=True)
        status, nrows = "SIMULATED", len(df_clean)

    return {
        "status": status,
        "output_csv": str(output_csv),
        "bronze_table": table_ref,
        "total_rows": nrows,
    }


if __name__ == "__main__":
    run_load_municipios()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    @dag(
        dag_id="src_load_municipios",
        description="Etapa Load Municipios Valle (Limpia CSV y carga BQ Bronze)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "municipios", "load", "bronze"],
        **get_airflow_dag_kwargs(),
    )
    def load_municipios_dag():
        @task(task_id="run_load_municipios")
        def execute_load() -> dict[str, Any]:
            return run_with_airflow_alarm(run_load_municipios)

        execute_load()

    dag = load_municipios_dag()
except ImportError:
    pass
