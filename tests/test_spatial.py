"""Pruebas unitarias automatizadas para la etapa espacial (src_transform_spatial.py)."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
DAGS_DIR = PROJECT_ROOT / "dags"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from pyimport.src_transform_spatial import (
    haversine_distance_km,
    parse_lat_lon,
    generate_approx_polygon_wkt,
    generate_geojson_feature_collection,
    run_transform_spatial,
)


def test_haversine_distance_km():
    """Verifica que la distancia entre Cali y Buga se calcule con precisión razonable (~55-65 km)."""
    cali_lat, cali_lon = 3.42158, -76.5205
    buga_lat, buga_lon = 3.90000, -76.3000

    dist = haversine_distance_km(cali_lat, cali_lon, buga_lat, buga_lon)
    assert 40.0 < dist < 80.0, f"Distancia inesperada Cali-Buga: {dist} km"
    assert haversine_distance_km(cali_lat, cali_lon, cali_lat, cali_lon) == 0.0


def test_parse_lat_lon():
    """Verifica la conversión de enteros compactos y flotantes a coordenadas decimales WGS84."""
    assert parse_lat_lon(3.9, 0.0) == 3.9
    assert parse_lat_lon(-76.3, 0.0) == -76.3
    assert abs(parse_lat_lon(342158.0, 0.0) - 3.42158) < 0.1
    assert abs(parse_lat_lon(-765205.0, 0.0) - (-76.5205)) < 0.1


def test_generate_approx_polygon_wkt():
    """Verifica la sintaxis válida de polígonos WKT."""
    wkt = generate_approx_polygon_wkt(3.42158, -76.5205)
    assert wkt.startswith("POLYGON((")
    assert wkt.endswith("))")
    assert "," in wkt


def test_run_transform_spatial_integration():
    """Verifica la ejecución integral de la etapa espacial."""
    res = run_transform_spatial()
    assert res["status"] == "SUCCESS"
    assert res["municipios_count"] == 42

    geojson_file = Path(res["geojson_file"])
    assert geojson_file.exists()

    with open(geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 42

    gold_file = Path(res["gold_geo_file"])
    assert gold_file.exists()


def test_spatial_dag_loaded_without_errors():
    """Verifica la carga del DAG src_transform_spatial en Airflow DagBag."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))
    assert "src_transform_spatial" in dagbag.dags
    dag = dagbag.dags.get("src_transform_spatial")
    assert dag is not None
    assert dag.dag_id == "src_transform_spatial"
