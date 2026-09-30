"""Pruebas unitarias para el módulo de municipios (ingesta, carga y consolidación) y sus DAGs en Airflow."""

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import pytest

# Ensure src and dags directories are in python path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src" / "pyimport"
DAGS_DIR = PROJECT_ROOT / "dags" / "gdv_general_dbt_dag" / "dags_valledata"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from src_load_municipios import process_municipios_dataframe, run_load_municipios
from src_transform_consolidado import consolidar_bronze_a_silver


def test_process_municipios_dataframe() -> None:
    """Verifica la limpieza y estandarización del dataframe de municipios."""
    raw_data = {
        "CODIGO DE MUNICIPIO": [76001, 76020],
        "MUNICIPIO": ["Cali ", " Alcalá"],
        "LATITUD": ["342158", "467429"],
        "LONGITUD": ["-765205", "-757832"],
        "SUPERFICIE TIPO DE PISO CALIDO": ["194,0", "5"],
        "SUPERFICIE TIPO DE PISO MEDIO": ["254", "56"],
        "SUPERFICIE TIPO DE PISO FRIO": ["98", "0"],
        "SUPERFICIE TIPO DE PISO PARAMO": ["18", "0"],
        "ALTURA SOBRE NIVEL DEL MAR                           (metros)": ["995", "1290"],
        "TEMPERATURA  MEDIA                           øC": ["23", "22"],
    }
    df_raw = pd.DataFrame(raw_data)
    df_clean = process_municipios_dataframe(df_raw)

    assert len(df_clean) == 2
    assert df_clean.iloc[0]["codigo_municipio"] == 76001
    assert df_clean.iloc[0]["municipio"] == "Cali"
    assert df_clean.iloc[0]["latitud"] == 342158.0
    assert df_clean.iloc[0]["altura_snm"] == 995.0
    assert df_clean.iloc[0]["temperatura_media"] == 23.0
    assert df_clean.iloc[0]["superficie_piso_calido"] == 194.0

    assert df_clean.iloc[1]["codigo_municipio"] == 76020
    assert df_clean.iloc[1]["municipio"] == "Alcalá"
    assert list(df_clean.columns) == [
        "codigo_municipio",
        "municipio",
        "clean_mun",
        "latitud",
        "longitud",
        "superficie_piso_calido",
        "superficie_piso_medio",
        "superficie_piso_frio",
        "superficie_piso_paramo",
        "altura_snm",
        "temperatura_media",
    ]


def test_municipios_dags_loaded_without_errors() -> None:
    """Verifica que los nuevos DAGs de municipios se carguen sin errores."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado en este entorno")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))

    assert "src_ingest_municipios" in dagbag.dags
    assert "src_load_municipios" in dagbag.dags
