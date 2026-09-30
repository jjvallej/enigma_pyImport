"""Módulo de consolidación Bronze -> Silver.
Combina los datasets de Cultivos (Permanentes y Transitorios), Precios Mayoristas SIPSA (Cali) e Índice Climático ONI (NOAA)
en un único dataset consolidado maestro para la capa Silver.

Columnas resultantes:
  1. tipo_cultivo ("Permanente" o "Transitorio")
  2. anio
  3. municipio
  4. codigo_cultivo
  5. nombre_cultivo
  6. hectareas_sembradas
  7. hectareas_cosechadas
  8. precio (Promedio $/Kg SIPSA Cali)
  9. indice_oni (Promedio anual ONI NOAA)
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict
import unicodedata

import pandas as pd

from pyimport.config_loader import load_config, require_config_value
from pyimport.src_common import clean_str, normalize_municipio, normalize_cultivo


def parse_numeric(val: Any) -> float:
    """Parsea valores numéricos convirtiendo formato español (1.234,56 -> 1234.56)."""
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
    """Usa la columna Cultivo como nombre; descarta Tipo_cultivo de categoría."""
    out = df.copy()
    try:
        tipo_src = _find_column(out, "Tipo_cultivo")
        out = out.drop(columns=[tipo_src])
    except KeyError:
        pass

    rename_map = {
        _find_column(out, "Año", "Anio"): "Año",
        _find_column(out, "Id_municipio"): "Id_municipio",
        _find_column(out, "Municipio"): "Municipio",
        _find_column(out, "Id_cultivo"): "Id_cultivo",
        _find_column(out, "Cultivo"): "Cultivo",
        _find_column(out, "Hectareas_sembradas"): "Hectareas_sembradas",
        _find_column(out, "Hectareas_cosechadas"): "Hectareas_cosechadas",
        _find_column(out, "Produccion_toneladas"): "Produccion_toneladas",
    }
    out = out.rename(columns=rename_map)
    out["tipo_cultivo"] = tipo
    for col in ("Hectareas_sembradas", "Hectareas_cosechadas", "Produccion_toneladas"):
        out[col] = out[col].apply(parse_numeric)
    try:
        col_ciclo = _find_column(out, "Ciclo")
        raw = out[col_ciclo].astype(str)
        out["ciclo"] = raw.fillna("Anual" if tipo.lower().startswith("perman") else "Semestre 1").astype(str)
        val_lower = raw.str.lower()
        if tipo.lower().startswith("perman"):
            out["semestre"] = "Anual"
        else:
            out["semestre"] = val_lower.map(
                lambda v: "2" if ("2" in v or v.strip() in {"b", "ii"}) else ("1" if ("1" in v or v.strip() in {"a", "i"}) else "1")
            )
    except KeyError:
        out["ciclo"] = "Anual" if tipo.lower().startswith("perman") else "Semestre 1"
        out["semestre"] = "Anual" if tipo.lower().startswith("perman") else "1"
    return out


def consolidar_bronze_a_silver(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Lee datasets Bronze (Cultivos, Precios SIPSA, Clima ONI) y genera el dataset consolidado Silver."""
    cfg = config or load_config()
    paths_cfg = require_config_value(cfg, "paths")
    bq_cfg = require_config_value(cfg, "bigquery")

    raw_dir = Path(require_config_value(paths_cfg, "raw_dir"))
    output_csv = Path(require_config_value(paths_cfg, "dataset_consolidado"))
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    perm_file = raw_dir / require_config_value(cfg, "cultivos", "permanentes_filename")
    trans_file = raw_dir / require_config_value(cfg, "cultivos", "transitorios_filename")
    sipsa_file = Path(require_config_value(paths_cfg, "sipsa_csv"))
    oni_file = Path(require_config_value(paths_cfg, "oni_csv"))

    if not perm_file.exists() or not trans_file.exists():
        raise FileNotFoundError("Debe ejecutar primero la ingesta/carga de cultivos (src_ingest_crops / src_load_crops).")

    print("🌾 [CONSOLIDACIÓN BRONZE -> SILVER] Leyendo dataset de Cultivos Permanentes...")
    df_perm = _prepare_cultivos_frame(
        pd.read_csv(perm_file, sep=";", encoding="latin1", low_memory=False),
        "Permanente",
    )

    print("🌾 [CONSOLIDACIÓN BRONZE -> SILVER] Leyendo dataset de Cultivos Transitorios...")
    df_trans = _prepare_cultivos_frame(
        pd.read_csv(trans_file, sep=";", encoding="latin1", low_memory=False),
        "Transitorio",
    )

    # Mantener semestre (no sumar A+B en una sola fila)
    print("🔄 [TRANSITORIOS] Agrupando por año, municipio, cultivo y semestre...")
    group_cols = ["tipo_cultivo", "Año", "semestre", "Id_municipio", "Municipio", "Id_cultivo", "Cultivo"]
    df_trans_grouped = df_trans.groupby(group_cols, as_index=False).agg({
        "Hectareas_sembradas": "sum",
        "Hectareas_cosechadas": "sum",
        "Produccion_toneladas": "sum",
    })
    df_perm_grouped = df_perm.groupby(group_cols, as_index=False).agg({
        "Hectareas_sembradas": "sum",
        "Hectareas_cosechadas": "sum",
        "Produccion_toneladas": "sum",
    })

    common_cols = group_cols + ["Hectareas_sembradas", "Hectareas_cosechadas"]
    df_perm_sel = df_perm_grouped[common_cols].copy()
    df_trans_sel = df_trans_grouped[common_cols].copy()

    df_crops = pd.concat([df_perm_sel, df_trans_sel], ignore_index=True)
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

    # --------------------------------------------------------------------------
    # Cruce de Precios SIPSA (DANE - Cali) por Año y Cultivo
    # --------------------------------------------------------------------------
    print("💰 [CRUCE PRECIOS SIPSA] Calculando promedios anuales de precios SIPSA Cali por año y cultivo...")
    if sipsa_file.exists():
        df_sipsa = pd.read_csv(sipsa_file, low_memory=False)
        df_sipsa["clean_alimento"] = df_sipsa["alimento"].apply(clean_str)
        df_sipsa["valor"] = pd.to_numeric(df_sipsa["valor"], errors="coerce")
        df_sipsa["anio"] = pd.to_numeric(df_sipsa["anio"], errors="coerce").fillna(0).astype(int)

        # Promedio anual por alimento
        sipsa_avg = df_sipsa.groupby(["anio", "clean_alimento"])["valor"].mean().reset_index()

        # Generar mapeo inteligente de cultivos a alimentos SIPSA
        crop_unique = sorted(df_crops["nombre_cultivo"].dropna().apply(clean_str).unique())
        sipsa_unique = sorted(sipsa_avg["clean_alimento"].dropna().unique())

        crop_to_sipsa = {}
        for c in crop_unique:
            if c in sipsa_unique:
                crop_to_sipsa[c] = c
            else:
                matches = [s for s in sipsa_unique if s.startswith(c) or c in s]
                if matches:
                    crop_to_sipsa[c] = matches[0]

        df_crops["clean_crop"] = df_crops["nombre_cultivo"].apply(clean_str)
        df_crops["mapped_sipsa"] = df_crops["clean_crop"].map(crop_to_sipsa)

        df_crops = pd.merge(
            df_crops,
            sipsa_avg,
            left_on=["anio", "mapped_sipsa"],
            right_on=["anio", "clean_alimento"],
            how="left",
        )
        df_crops.rename(columns={"valor": "precio"}, inplace=True)
        df_crops.drop(columns=["clean_crop", "mapped_sipsa", "clean_alimento"], errors="ignore", inplace=True)
    else:
        df_crops["precio"] = None

    # --------------------------------------------------------------------------
    # Cruce del Índice Climático ONI (NOAA) por Año
    # --------------------------------------------------------------------------
    print("🌡️ [CRUCE CLIMA ONI] Asignando promedio anual del índice ONI NOAA...")
    if oni_file.exists():
        df_oni = pd.read_csv(oni_file, low_memory=False)
        df_oni["anio"] = pd.to_numeric(df_oni["anio"], errors="coerce").fillna(0).astype(int)
        df_oni["promedio_oni"] = pd.to_numeric(df_oni["promedio_oni"], errors="coerce")

        df_crops = pd.merge(df_crops, df_oni[["anio", "promedio_oni"]], on="anio", how="left")
        df_crops.rename(columns={"promedio_oni": "indice_oni"}, inplace=True)
    else:
        df_crops["indice_oni"] = None

    # Reordenar columnas exactas solicitadas
    final_cols = [
        "tipo_cultivo",
        "anio",
        "semestre",
        "municipio",
        "codigo_cultivo",
        "nombre_cultivo",
        "hectareas_sembradas",
        "hectareas_cosechadas",
        "precio",
        "indice_oni",
    ]
    df_final = df_crops[final_cols].copy()

    # Redondear numéricos
    df_final["hectareas_sembradas"] = df_final["hectareas_sembradas"].round(2)
    df_final["hectareas_cosechadas"] = df_final["hectareas_cosechadas"].round(2)
    df_final["precio"] = df_final["precio"].round(2)
    df_final["indice_oni"] = df_final["indice_oni"].round(3)

    # Escribir CSV consolidado Silver
    df_final.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"✨ [CONSOLIDACIÓN COMPLETADA] Archivo Silver generado: {output_csv}")
    print(f"   - Total filas resultantes: {len(df_final)}")
    print(f"   - Filas con precio SIPSA asignado: {df_final['precio'].notna().sum()}")
    print(f"   - Filas con índice ONI asignado: {df_final['indice_oni'].notna().sum()}")

    # Ingesta / Carga a BigQuery Silver
    project_id = require_config_value(bq_cfg, "project_id")
    dataset_silver = require_config_value(bq_cfg, "datasets", "silver")
    table_silver = require_config_value(bq_cfg, "tables", "consolidado_silver")

    try:
        from google.cloud import bigquery
        client = bigquery.Client(project=project_id, location=require_config_value(bq_cfg, "location"))
        table_ref = f"{project_id}.{dataset_silver}.{table_silver}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(output_csv, "rb") as sf:
            job = client.load_table_from_file(sf, table_ref, job_config=job_config)
        job.result()
        bq_status = "SUCCESS"
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación BQ Silver] {exc}")
        table_ref = f"{project_id}.{dataset_silver}.{table_silver}"
        bq_status = "SIMULATED"

    return {
        "status": bq_status,
        "output_csv": str(output_csv),
        "silver_table": table_ref,
        "total_rows": len(df_final),
    }


if __name__ == "__main__":
    consolidar_bronze_a_silver()
