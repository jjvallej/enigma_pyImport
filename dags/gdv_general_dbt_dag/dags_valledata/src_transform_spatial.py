"""ETAPA TRANSFORM SPATIAL (src_transform_spatial.py)
Transformación y consolidación de datos georreferenciados (GIS).
Combina el dataset de cultivos con los polígonos/centroides de los 42 municipios del Valle del Cauca,
pisos térmicos, distancia logístic a Cavasa (Cali), índice climático ONI y precios SIPSA DANE.
Genera los datasets Gold GeoJSON/WKT para análisis espacial y mapas interactivos.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Tuple

import pandas as pd

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    normalize_municipio,
    get_bigquery_client,
    get_bq_table_ref,
    get_composer_params,
    get_raw_root,
    load_config,
    materialize_local,
    read_bq_dataframe,
    require_config_value,
    running_in_composer,
    storage_exists,
    storage_join,
    write_bytes,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)

# Centroide oficial de Cali / Cavasa
CAVASA_LAT = 3.42158
CAVASA_LON = -76.5205


def parse_lat_lon(val: Any, default: float) -> float:
    """Convierte coordenadas representadas como enteros compactos o flotantes a grados decimales WGS84."""
    if pd.isna(val) or val is None:
        return default
    try:
        f = float(val)
        if abs(f) < 180.0:
            return f
        sign = -1.0 if f < 0 else 1.0
        abs_f = abs(f)
        s = f"{abs_f:.0f}"
        if s.startswith(("75", "76", "77", "78")):
            # Longitud en Colombia (ej. -765205 -> -76.5205)
            deg = float(s[:2])
            dec_str = s[2:]
            dec = float("0." + dec_str) if dec_str else 0.0
            return sign * (deg + dec)
        elif s.startswith(("3", "4", "5")):
            # Latitud en Colombia (ej. 342158 -> 3.42158)
            deg = float(s[:1])
            dec_str = s[1:]
            dec = float("0." + dec_str) if dec_str else 0.0
            return sign * (deg + dec)
        return f / 100000.0
    except Exception:
        return default


def haversine_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calcula la distancia geodésica en kilómetros entre dos puntos (fórmula de Haversine)."""
    r = 6371.0  # Radio medio de la Tierra en km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return round(r * c, 2)


def generate_approx_polygon_wkt(lat: float, lon: float, radius_km: float = 8.0) -> str:
    """Genera un polígono hexagonal de aproximación WKT (Well-Known Text) centrado en lat/lon."""
    num_sides = 6
    coords = []
    lat_deg_per_km = 1.0 / 111.0
    lon_deg_per_km = 1.0 / (111.0 * math.cos(math.radians(lat)))

    for i in range(num_sides + 1):
        angle = 2.0 * math.pi * i / num_sides
        d_lat = radius_km * math.sin(angle) * lat_deg_per_km
        d_lon = radius_km * math.cos(angle) * lon_deg_per_km
        p_lat = round(lat + d_lat, 6)
        p_lon = round(lon + d_lon, 6)
        coords.append(f"{p_lon} {p_lat}")

    poly_str = ", ".join(coords)
    return f"POLYGON(({poly_str}))"


def generate_geojson_feature_collection(df_muni: pd.DataFrame) -> Dict[str, Any]:
    """Genera una FeatureCollection GeoJSON con los polígonos de los municipios."""
    features = []
    for _, row in df_muni.iterrows():
        lat = row["latitud_dec"]
        lon = row["longitud_dec"]
        muni_name = str(row["municipio"])
        muni_code = int(row["codigo_municipio"])

        # Crear coordenadas de polígono
        lat_deg_per_km = 1.0 / 111.0
        lon_deg_per_km = 1.0 / (111.0 * math.cos(math.radians(lat)))
        radius_km = 7.5
        poly_coords = []
        for i in range(7):
            angle = 2.0 * math.pi * i / 6
            d_lat = radius_km * math.sin(angle) * lat_deg_per_km
            d_lon = radius_km * math.cos(angle) * lon_deg_per_km
            poly_coords.append([round(lon + d_lon, 6), round(lat + d_lat, 6)])

        feature = {
            "type": "Feature",
            "id": str(muni_code),
            "properties": {
                "codigo_municipio": muni_code,
                "municipio": muni_name,
                "latitud": lat,
                "longitud": lon,
                "altura_snm": float(row.get("altura_snm", 0) or 0),
                "temperatura_media": float(row.get("temperatura_media", 0) or 0),
                "distancia_cavasa_km": float(row.get("distancia_cavasa_km", 0) or 0),
                "piso_predominante": str(row.get("piso_predominante", "Calido")),
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [poly_coords],
            },
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "name": "municipios_valle_poligonos",
        "features": features,
    }


def determine_predominant_thermal_floor(row: pd.Series) -> str:
    """Determina el piso térmico predominante según las hectáreas de superficie."""
    calido = float(row.get("superficie_piso_calido", 0) or 0)
    medio = float(row.get("superficie_piso_medio", 0) or 0)
    frio = float(row.get("superficie_piso_frio", 0) or 0)
    paramo = float(row.get("superficie_piso_paramo", 0) or 0)

    floors = [("Calido", calido), ("Medio", medio), ("Frio", frio), ("Paramo", paramo)]
    floors.sort(key=lambda x: x[1], reverse=True)
    return floors[0][0] if floors[0][1] > 0 else "Calido"


def _load_municipios_df(cfg: Dict[str, Any], base_dir: Path) -> pd.DataFrame:
    try:
        mun_ref = get_bq_table_ref(cfg, "bronze", "municipios_bronze")
        df = read_bq_dataframe(cfg, mun_ref)
        if df is not None and not df.empty:
            print(f"🗺️ [SRC_TRANSFORM_SPATIAL] Cargados {len(df)} municipios desde BigQuery: {mun_ref}", flush=True)
            return df
    except Exception as exc:
        print(f"ℹ️ [SRC_TRANSFORM_SPATIAL] No se pudo leer BQ municipios ({exc}); usando CSV...", flush=True)

    muni_file = base_dir / "municipios_valle_clean.csv"
    if not muni_file.exists():
        raw_muni = base_dir / "raw" / "municipios_valle.csv"
        if raw_muni.exists():
            from src_load_municipios import process_municipios_dataframe
            df_muni_raw = pd.read_csv(raw_muni)
            df_muni = process_municipios_dataframe(df_muni_raw)
            df_muni.to_csv(muni_file, index=False)
            return df_muni
        else:
            raise FileNotFoundError("Debe ejecutar primero src_ingest_municipios y src_load_municipios.")
    return pd.read_csv(muni_file)


def _load_master_df(cfg: Dict[str, Any], base_dir: Path) -> pd.DataFrame | None:
    try:
        silver_ref = get_bq_table_ref(cfg, "silver", "consolidado_silver")
        df = read_bq_dataframe(cfg, silver_ref)
        if df is not None and not df.empty:
            print(f"🌾 [SRC_TRANSFORM_SPATIAL] Cargados {len(df)} registros consolidados desde BigQuery: {silver_ref}", flush=True)
            return df
    except Exception as exc:
        print(f"ℹ️ [SRC_TRANSFORM_SPATIAL] No se pudo leer BQ consolidado Silver ({exc}); usando CSV...", flush=True)

    master_file = base_dir / "dataset_consolidado_valle.csv"
    if not master_file.exists():
        master_file = base_dir / "silver" / "silver_agri_consolidado.csv"
    if not master_file.exists():
        master_file = base_dir / "cultivos_valle.csv"

    if master_file.exists():
        print(f"🌾 [SRC_TRANSFORM_SPATIAL] Cargando consolidado desde CSV local: {master_file}", flush=True)
        return pd.read_csv(master_file, low_memory=False)
    return None


def run_transform_spatial(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    paths_cfg = cfg.get("paths", {})
    base_dir = Path(paths_cfg.get("output_dir", "data"))
    base_dir.mkdir(parents=True, exist_ok=True)

    # 1. Cargar municipios limpios
    df_muni = _load_municipios_df(cfg, base_dir)

    # 2. Normalizar coordenadas y calcular métricas espaciales
    df_muni["latitud_dec"] = df_muni.apply(
        lambda r: parse_lat_lon(r["latitud"], 3.42158 if r["codigo_municipio"] == 76001 else 3.8),
        axis=1,
    )
    df_muni["longitud_dec"] = df_muni.apply(
        lambda r: parse_lat_lon(r["longitud"], -76.5205 if r["codigo_municipio"] == 76001 else -76.3),
        axis=1,
    )

    df_muni["distancia_cavasa_km"] = df_muni.apply(
        lambda r: haversine_distance_km(r["latitud_dec"], r["longitud_dec"], CAVASA_LAT, CAVASA_LON),
        axis=1,
    )
    df_muni["wkt_geometry"] = df_muni.apply(
        lambda r: generate_approx_polygon_wkt(r["latitud_dec"], r["longitud_dec"]),
        axis=1,
    )
    df_muni["piso_predominante"] = df_muni.apply(determine_predominant_thermal_floor, axis=1)

    # 3. Exportar GeoJSON de Municipios
    geojson_data = generate_geojson_feature_collection(df_muni)
    geojson_path = base_dir / "municipios_valle_poligono_geojson.json"
    with open(geojson_path, "w", encoding="utf-8") as f:
        json.dump(geojson_data, f, ensure_ascii=False, indent=2)

    # 4. Cruce con Dataset Consolidado / Cultivos
    df_master = _load_master_df(cfg, base_dir)
    if df_master is not None and not df_master.empty:
        # Remover columnas espaciales previas o brutas de df_master para evitar duplicados _x/_y
        cols_to_drop = [
            "latitud", "longitud", "altura_snm", "temperatura_media",
            "superficie_piso_calido", "superficie_piso_medio",
            "superficie_piso_frio", "superficie_piso_paramo"
        ]
        df_master = df_master.drop(columns=[c for c in cols_to_drop if c in df_master.columns], errors="ignore")

        muni_cols = [
            "codigo_municipio", "latitud_dec", "longitud_dec", "altura_snm",
            "temperatura_media", "distancia_cavasa_km", "wkt_geometry",
            "piso_predominante", "superficie_piso_calido", "superficie_piso_medio",
            "superficie_piso_frio", "superficie_piso_paramo"
        ]

        if "codigo_municipio" in df_master.columns:
            df_geo = pd.merge(df_master, df_muni[[c for c in muni_cols if c in df_muni.columns]], on="codigo_municipio", how="left")
        else:
            if "clean_mun" not in df_master.columns and "municipio" in df_master.columns:
                df_master["clean_mun"] = df_master["municipio"].apply(normalize_municipio)
            if "clean_mun" not in df_muni.columns and "municipio" in df_muni.columns:
                df_muni["clean_mun"] = df_muni["municipio"].apply(normalize_municipio)
            muni_cols_clean = ["clean_mun"] + [c for c in muni_cols if c != "codigo_municipio"]
            df_geo = pd.merge(df_master, df_muni[[c for c in muni_cols_clean if c in df_muni.columns]], on="clean_mun", how="left").drop(columns=["clean_mun"], errors="ignore")

        # Renombrar latitud_dec y longitud_dec a latitud y longitud oficiales WGS84
        if "latitud_dec" in df_geo.columns:
            df_geo["latitud"] = df_geo["latitud_dec"]
            df_geo.drop(columns=["latitud_dec"], inplace=True)
        if "longitud_dec" in df_geo.columns:
            df_geo["longitud"] = df_geo["longitud_dec"]
            df_geo.drop(columns=["longitud_dec"], inplace=True)
    else:
        df_geo = df_muni.copy()
        if "latitud_dec" in df_geo.columns:
            df_geo["latitud"] = df_geo["latitud_dec"]
            df_geo.drop(columns=["latitud_dec"], inplace=True)
        if "longitud_dec" in df_geo.columns:
            df_geo["longitud"] = df_geo["longitud_dec"]
            df_geo.drop(columns=["longitud_dec"], inplace=True)


    # 5. Exportar Dataset Gold Georreferenciado
    gold_path = base_dir / "gold_cultivos_valle_geo.csv"
    df_geo.to_csv(gold_path, index=False, encoding="utf-8")

    # Copia de respaldo para compatibilidad
    compat_path = base_dir / "gold_cultivos_municipios_geo.csv"
    df_geo.to_csv(compat_path, index=False, encoding="utf-8")

    bq_table_ref = get_bq_table_ref(cfg, "silver", "gold_spatial")
    status_bq = "SIMULATED"
    try:
        from google.cloud import bigquery
        bq_cfg = require_config_value(cfg, "bigquery")
        client = get_bigquery_client(
            cfg,
            require_config_value(bq_cfg, "project_id"),
            require_config_value(bq_cfg, "location"),
        )
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(gold_path, "rb") as sf:
            job = client.load_table_from_file(sf, bq_table_ref, job_config=job_config)
        job.result()
        status_bq = "SUCCESS"
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación BQ Gold Spatial] {exc}", flush=True)

    status = "SUCCESS" if status_bq == "SUCCESS" or not running_in_composer() else "SIMULATED"
    result = {
        "status": status,
        "municipios_count": len(df_muni),
        "geojson_file": str(geojson_path),
        "gold_geo_file": str(gold_path),
        "bigquery_table": bq_table_ref,
        "status_bq": status_bq,
        "total_rows_gold": len(df_geo),
    }

    print(
        f"✅ [SRC_TRANSFORM_SPATIAL] Proceso completado exitosamente.\n"
        f"   - Polígonos y centroides de {len(df_muni)} municipios generados.\n"
        f"   - GeoJSON exportado en: {geojson_path}\n"
        f"   - Dataset Gold Georreferenciado escrito en: {gold_path}\n"
        f"   - Tabla Gold en BigQuery: {bq_table_ref} ({status_bq})",
        flush=True,
    )
    return result


if __name__ == "__main__":
    run_transform_spatial()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    from src_common import get_airflow_dag_kwargs, run_with_airflow_alarm

    @dag(
        dag_id="src_transform_spatial",
        description="Etapa Transform Spatial Gold GIS (Polígonos Municipales, WKT/GeoJSON, Distancia Cavasa y Gold GIS)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "gis", "spatial", "gold", "poligono"],
        **get_airflow_dag_kwargs(),
    )
    def transform_spatial_dag():
        @task(task_id="run_transform_spatial")
        def execute_transform() -> Dict[str, Any]:
            return run_with_airflow_alarm(run_transform_spatial)

        execute_transform()

    dag = transform_spatial_dag()
    DAG = dag
except ImportError:
    pass

