"""ETAPA TRANSFORM CONSOLIDADO (src_transform_consolidado.py)

Único script Silver: une Cultivos + SIPSA (bronze_agri_sipsa) + ONI en una sola tabla:
  datagov-477214.valledata.silver_agri_consolidado

Granularidad: anio + municipio + cultivo (+ semestre para transitorios).

Columnas:
  tipo_cultivo, anio, semestre, municipio, codigo_cultivo, nombre_cultivo,
  hectareas_sembradas, hectareas_cosechadas, precio, indice_oni
"""

from __future__ import annotations

from pathlib import Path
import re
import sys
from typing import Any, Dict
import unicodedata

import pandas as pd

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    clean_str,
    normalize_municipio,
    normalize_cultivo,
    get_bigquery_client,
    get_bq_table_ref,
    get_composer_params,
    get_connection_id,
    get_raw_root,
    load_config,
    materialize_local,
    read_bq_dataframe,
    require_config_value,
    storage_exists,
    storage_join,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)


def parse_numeric(val: Any) -> float:
    if pd.isna(val) or val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return 0.0
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return 0.0


def normalize_semestre(ciclo: Any, tipo: str) -> str:
    """Mapea Ciclo -> semestre. Transitorios: 1/2; permanentes: vacío."""
    if tipo.lower().startswith("perman"):
        return ""
    raw = str(ciclo or "").strip().lower()
    if not raw or raw in {"anual", "nan", "none"}:
        return ""
    if "2" in raw or raw in {"b", "semestre 2", "semestre2", "ii"}:
        return "2"
    if "1" in raw or raw in {"a", "semestre 1", "semestre1", "i"}:
        return "1"
    return raw


def _find_column(df: pd.DataFrame, *candidates: str) -> str:
    normalized = {
        str(c)
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n"): c
        for c in df.columns
    }
    for candidate in candidates:
        key = (
            candidate.strip()
            .lower()
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
            .replace("ñ", "n")
        )
        if key in normalized:
            return normalized[key]
    raise KeyError(f"No se encontró columna entre {candidates}. Disponibles: {list(df.columns)}")


def _prepare_cultivos_frame(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    out = df.copy()
    try:
        tipo_src = _find_column(out, "Tipo_cultivo")
        out = out.drop(columns=[tipo_src])
    except KeyError:
        pass

    col_anio = _find_column(out, "Año", "Anio")
    col_mun_id = _find_column(out, "Id_municipio")
    col_mun = _find_column(out, "Municipio")
    col_cultivo_id = _find_column(out, "Id_cultivo")
    col_cultivo = _find_column(out, "Cultivo")
    col_sem = _find_column(out, "Hectareas_sembradas")
    col_cos = _find_column(out, "Hectareas_cosechadas")

    rename = {
        col_anio: "Año",
        col_mun_id: "Id_municipio",
        col_mun: "Municipio",
        col_cultivo_id: "Id_cultivo",
        col_cultivo: "Cultivo",
        col_sem: "Hectareas_sembradas",
        col_cos: "Hectareas_cosechadas",
    }
    out = out.rename(columns=rename)
    out["tipo_cultivo"] = tipo
    out["Hectareas_sembradas"] = out["Hectareas_sembradas"].apply(parse_numeric)
    out["Hectareas_cosechadas"] = out["Hectareas_cosechadas"].apply(parse_numeric)

    try:
        col_ciclo = _find_column(out, "Ciclo")
        out["semestre"] = out[col_ciclo].apply(lambda v: normalize_semestre(v, tipo))
    except KeyError:
        out["semestre"] = "" if tipo.lower().startswith("perman") else ""

    return out


def _load_sipsa(cfg: Dict[str, Any], paths_cfg: Dict[str, Any]) -> pd.DataFrame | None:
    sipsa_bronze_ref = get_bq_table_ref(cfg, "bronze", "sipsa_bronze")
    try:
        df = read_bq_dataframe(cfg, sipsa_bronze_ref)
        print(f"💰 SIPSA Bronze {sipsa_bronze_ref}: {len(df)} filas", flush=True)
        return df
    except Exception as exc:
        sipsa_file = Path(require_config_value(paths_cfg, "sipsa_csv"))
        if sipsa_file.exists():
            print(f"ℹ️ BQ SIPSA no disponible ({exc}); CSV {sipsa_file}", flush=True)
            return pd.read_csv(sipsa_file, low_memory=False)
        print(f"⚠️ Sin SIPSA ({exc})", flush=True)
        return None


def _load_oni(cfg: Dict[str, Any], paths_cfg: Dict[str, Any]) -> pd.DataFrame | None:
    try:
        oni_ref = get_bq_table_ref(cfg, "bronze", "oni_bronze")
        df = read_bq_dataframe(cfg, oni_ref)
        print(f"🌡️ ONI Bronze {oni_ref}: {len(df)} filas", flush=True)
        return df
    except Exception as exc:
        oni_file = Path(require_config_value(paths_cfg, "oni_csv"))
        if oni_file.exists():
            print(f"ℹ️ BQ ONI no disponible ({exc}); CSV {oni_file}", flush=True)
            return pd.read_csv(oni_file, low_memory=False)
        print(f"⚠️ Sin ONI ({exc})", flush=True)
        return None


def _load_municipios(cfg: Dict[str, Any], paths_cfg: Dict[str, Any]) -> pd.DataFrame | None:
    try:
        mun_ref = get_bq_table_ref(cfg, "bronze", "municipios_bronze")
        df = read_bq_dataframe(cfg, mun_ref)
        print(f"🗺️ Municipios Bronze {mun_ref}: {len(df)} filas", flush=True)
        return df
    except Exception as exc:
        output_dir = Path(paths_cfg.get("output_dir", "data"))
        mun_cfg = cfg.get("municipios", {})
        out_name = mun_cfg.get("output_filename", "municipios_valle_clean.csv")
        mun_file = output_dir / out_name
        if mun_file.exists():
            print(f"ℹ️ BQ Municipios no disponible ({exc}); CSV {mun_file}", flush=True)
            return pd.read_csv(mun_file, low_memory=False)
        print(f"⚠️ Sin Municipios ({exc})", flush=True)
        return None


def _attach_sipsa(df_crops: pd.DataFrame, df_sipsa: pd.DataFrame) -> pd.DataFrame:
    if "cultivo" in df_sipsa.columns:
        name_col = "cultivo"
    elif "alimento" in df_sipsa.columns:
        name_col = "alimento"
    else:
        raise KeyError(f"SIPSA sin cultivo/alimento: {list(df_sipsa.columns)}")

    if "precio_promedio_cali" in df_sipsa.columns:
        price_col = "precio_promedio_cali"
    elif "valor" in df_sipsa.columns:
        price_col = "valor"
    else:
        raise KeyError(f"SIPSA sin precio: {list(df_sipsa.columns)}")

    df_sipsa = df_sipsa.copy()
    df_sipsa["clean_alimento"] = df_sipsa[name_col].apply(normalize_cultivo)
    df_sipsa["precio"] = pd.to_numeric(df_sipsa[price_col], errors="coerce")
    df_sipsa["anio"] = pd.to_numeric(df_sipsa["anio"], errors="coerce").fillna(0).astype(int)

    if price_col == "valor" or "mes" in df_sipsa.columns:
        sipsa_avg = df_sipsa.groupby(["anio", "clean_alimento"], as_index=False)["precio"].mean()
    else:
        sipsa_avg = df_sipsa[["anio", "clean_alimento", "precio"]].drop_duplicates(
            subset=["anio", "clean_alimento"]
        )

    crop_unique = sorted(df_crops["nombre_cultivo"].dropna().apply(normalize_cultivo).unique())
    sipsa_unique = sorted(sipsa_avg["clean_alimento"].dropna().unique())
    crop_to_sipsa: dict[str, str] = {}
    for c in crop_unique:
        if c in sipsa_unique:
            crop_to_sipsa[c] = c
        else:
            matches = [s for s in sipsa_unique if s.startswith(c) or c in s]
            if matches:
                crop_to_sipsa[c] = matches[0]

    df_crops = df_crops.copy()
    df_crops["clean_crop"] = df_crops["nombre_cultivo"].apply(normalize_cultivo)
    df_crops["mapped_sipsa"] = df_crops["clean_crop"].map(crop_to_sipsa)
    df_crops = pd.merge(
        df_crops,
        sipsa_avg,
        left_on=["anio", "mapped_sipsa"],
        right_on=["anio", "clean_alimento"],
        how="left",
    )
    return df_crops.drop(columns=["clean_crop", "mapped_sipsa", "clean_alimento"], errors="ignore")


def consolidar_bronze_a_silver(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Consolida cultivos + SIPSA + ONI + Municipios en una sola tabla Silver."""
    cfg = config or load_config()
    paths_cfg = require_config_value(cfg, "paths")
    bq_cfg = require_config_value(cfg, "bigquery")
    google_cloud_default = get_connection_id(cfg, "google_cloud_default")
    composer = get_composer_params(cfg)

    raw_root = get_raw_root(cfg)
    output_dir = Path(paths_cfg.get("output_dir", "data")) / "silver"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_csv = Path(
        paths_cfg.get("dataset_consolidado")
        or str(output_dir / require_config_value(cfg, "cultivos", "consolidado_filename"))
    )
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    perm_uri = storage_join(raw_root, require_config_value(cfg, "cultivos", "permanentes_filename"))
    trans_uri = storage_join(raw_root, require_config_value(cfg, "cultivos", "transitorios_filename"))

    if not storage_exists(perm_uri, cfg=cfg) or not storage_exists(trans_uri, cfg=cfg):
        raise FileNotFoundError("Debe ejecutar primero ingest/load de cultivos.")

    perm_file = materialize_local(perm_uri, cfg=cfg)
    trans_file = materialize_local(trans_uri, cfg=cfg)

    print("🌾 Leyendo cultivos permanentes...", flush=True)
    df_perm = _prepare_cultivos_frame(
        pd.read_csv(perm_file, sep=";", encoding="latin1", low_memory=False),
        "permanente",
    )
    print("🌾 Leyendo cultivos transitorios (con semestre)...", flush=True)
    df_trans = _prepare_cultivos_frame(
        pd.read_csv(trans_file, sep=";", encoding="latin1", low_memory=False),
        "transitorio",
    )

    # Agrupar por año, municipio, cultivo y semestre (transitorios no se suman entre semestres)
    group_cols = [
        "tipo_cultivo",
        "Año",
        "semestre",
        "Id_municipio",
        "Municipio",
        "Id_cultivo",
        "Cultivo",
    ]
    df_perm_g = df_perm.groupby(group_cols, as_index=False).agg(
        {"Hectareas_sembradas": "sum", "Hectareas_cosechadas": "sum"}
    )
    df_trans_g = df_trans.groupby(group_cols, as_index=False).agg(
        {"Hectareas_sembradas": "sum", "Hectareas_cosechadas": "sum"}
    )

    df_crops = pd.concat([df_perm_g, df_trans_g], ignore_index=True)
    df_crops.rename(
        columns={
            "Año": "anio",
            "Municipio": "municipio",
            "Id_cultivo": "codigo_cultivo",
            "Cultivo": "nombre_cultivo",
            "Hectareas_sembradas": "hectareas_sembradas",
            "Hectareas_cosechadas": "hectareas_cosechadas",
        },
        inplace=True,
    )
    df_crops["anio"] = pd.to_numeric(df_crops["anio"], errors="coerce").fillna(0).astype(int)
    df_crops["semestre"] = df_crops["semestre"].fillna("").astype(str)

    df_mun = _load_municipios(cfg, paths_cfg)
    if df_mun is not None and not df_mun.empty:
        df_mun = df_mun.copy()
        df_mun["clean_mun"] = df_mun["municipio"].apply(normalize_municipio)
        df_mun_unique = df_mun.drop_duplicates(subset=["clean_mun"])

        df_crops["clean_mun"] = df_crops["municipio"].apply(normalize_municipio)

        geo_cols = [
            "clean_mun",
            "codigo_municipio",
            "latitud",
            "longitud",
            "altura_snm",
            "temperatura_media",
            "superficie_piso_calido",
            "superficie_piso_medio",
            "superficie_piso_frio",
            "superficie_piso_paramo",
        ]
        available_geo_cols = [c for c in geo_cols if c in df_mun_unique.columns]
        df_crops = pd.merge(df_crops, df_mun_unique[available_geo_cols], on="clean_mun", how="left")
        df_crops.drop(columns=["clean_mun"], inplace=True, errors="ignore")
    else:
        for c in (
            "codigo_municipio",
            "latitud",
            "longitud",
            "altura_snm",
            "temperatura_media",
            "superficie_piso_calido",
            "superficie_piso_medio",
            "superficie_piso_frio",
            "superficie_piso_paramo",
        ):
            df_crops[c] = None

    df_sipsa = _load_sipsa(cfg, paths_cfg)
    if df_sipsa is not None and not df_sipsa.empty:
        df_crops = _attach_sipsa(df_crops, df_sipsa)
    else:
        df_crops["precio"] = None

    df_oni = _load_oni(cfg, paths_cfg)
    if df_oni is not None and not df_oni.empty:
        df_oni = df_oni.copy()
        df_oni["anio"] = pd.to_numeric(df_oni["anio"], errors="coerce").fillna(0).astype(int)
        oni_col = "promedio_oni" if "promedio_oni" in df_oni.columns else None
        if oni_col is None:
            for c in df_oni.columns:
                if "oni" in str(c).lower():
                    oni_col = c
                    break
        if oni_col:
            df_oni[oni_col] = pd.to_numeric(df_oni[oni_col], errors="coerce")
            df_crops = pd.merge(df_crops, df_oni[["anio", oni_col]], on="anio", how="left")
            df_crops.rename(columns={oni_col: "indice_oni"}, inplace=True)
        else:
            df_crops["indice_oni"] = None
    else:
        df_crops["indice_oni"] = None

    final_cols = [
        "tipo_cultivo",
        "anio",
        "semestre",
        "codigo_municipio",
        "municipio",
        "latitud",
        "longitud",
        "altura_snm",
        "temperatura_media",
        "superficie_piso_calido",
        "superficie_piso_medio",
        "superficie_piso_frio",
        "superficie_piso_paramo",
        "codigo_cultivo",
        "nombre_cultivo",
        "hectareas_sembradas",
        "hectareas_cosechadas",
        "precio",
        "indice_oni",
    ]
    for c in final_cols:
        if c not in df_crops.columns:
            df_crops[c] = None

    df_final = df_crops[final_cols].copy()

    for col in ("tipo_cultivo", "municipio", "nombre_cultivo"):
        df_final[col] = df_final[col].apply(clean_str)

    df_final["hectareas_sembradas"] = df_final["hectareas_sembradas"].round(2)
    df_final["hectareas_cosechadas"] = df_final["hectareas_cosechadas"].round(2)
    df_final["precio"] = pd.to_numeric(df_final["precio"], errors="coerce").round(2)
    df_final["indice_oni"] = pd.to_numeric(df_final["indice_oni"], errors="coerce").round(3)

    df_final = df_final.sort_values(
        ["anio", "municipio", "tipo_cultivo", "nombre_cultivo", "semestre"]
    ).reset_index(drop=True)
    df_final.to_csv(output_csv, index=False, encoding="utf-8")

    table_ref = get_bq_table_ref(cfg, "silver", "consolidado_silver")
    print(
        f"✨ Consolidado {len(df_final)} filas -> {output_csv} | BQ={table_ref} | "
        f"Composer={composer['environment']} | conn={google_cloud_default}",
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
                bigquery.SchemaField("tipo_cultivo", "STRING"),
                bigquery.SchemaField("anio", "INTEGER"),
                bigquery.SchemaField("semestre", "STRING"),
                bigquery.SchemaField("codigo_municipio", "INTEGER"),
                bigquery.SchemaField("municipio", "STRING"),
                bigquery.SchemaField("latitud", "FLOAT"),
                bigquery.SchemaField("longitud", "FLOAT"),
                bigquery.SchemaField("altura_snm", "FLOAT"),
                bigquery.SchemaField("temperatura_media", "FLOAT"),
                bigquery.SchemaField("superficie_piso_calido", "FLOAT"),
                bigquery.SchemaField("superficie_piso_medio", "FLOAT"),
                bigquery.SchemaField("superficie_piso_frio", "FLOAT"),
                bigquery.SchemaField("superficie_piso_paramo", "FLOAT"),
                bigquery.SchemaField("codigo_cultivo", "STRING"),
                bigquery.SchemaField("nombre_cultivo", "STRING"),
                bigquery.SchemaField("hectareas_sembradas", "FLOAT"),
                bigquery.SchemaField("hectareas_cosechadas", "FLOAT"),
                bigquery.SchemaField("precio", "FLOAT"),
                bigquery.SchemaField("indice_oni", "FLOAT"),
            ],
        )
        with output_csv.open("rb") as sf:
            job = client.load_table_from_file(sf, table_ref, job_config=job_config)
        job.result()
        table = client.get_table(table_ref)
        status, nrows = "SUCCESS", table.num_rows
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación BQ Silver] {exc}", flush=True)
        status, nrows = "SIMULATED", len(df_final)

    return {
        "status": status,
        "output_csv": str(output_csv),
        "silver_table": table_ref,
        "sipsa_bronze_table": (
            get_bq_table_ref(cfg, "bronze", "sipsa_bronze")
            if (cfg.get("bigquery") or {}).get("datasets", {}).get("bronze")
            and (cfg.get("bigquery") or {}).get("tables", {}).get("sipsa_bronze")
            else None
        ),
        "total_rows": nrows,
    }


def run_transform_consolidado(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    print("🔄 [SRC_TRANSFORM_CONSOLIDADO] Bronze (crops+sipsa+oni) -> Silver único...", flush=True)
    res = consolidar_bronze_a_silver(cfg)
    print(f"✅ Silver: {res.get('silver_table')} ({res.get('total_rows')} filas)", flush=True)
    return res


# Alias para compatibilidad
run_transform_crops = run_transform_consolidado


if __name__ == "__main__":
    run_transform_consolidado()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    @dag(
        dag_id="src_transform_consolidado",
        description="Único Silver: cultivos+SIPSA+ONI por año/municipio/cultivo/semestre",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "consolidado", "transform", "silver"],
        **get_airflow_dag_kwargs(),
    )
    def transform_consolidado_dag():
        @task(task_id="run_transform_consolidado")
        def execute_transform() -> dict[str, object]:
            return run_with_airflow_alarm(run_transform_consolidado)

        execute_transform()

    dag = transform_consolidado_dag()
except ImportError:
    pass
