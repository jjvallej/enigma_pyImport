"""Pruebas unitarias para el módulo de cultivos y su DAG en Airflow."""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

# Ensure src and dags directories are in python path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
DAGS_DIR = PROJECT_ROOT / "dags"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from pyimport.cultivos import (
    CultivoRecord,
    parse_cultivos_permanentes,
    parse_cultivos_transitorios,
    write_consolidated_cultivos_csv,
)


def test_parse_and_consolidate_cultivos(tmp_path: Path) -> None:
    """Prueba el parseo y la consolidación en CSV de archivos de cultivo ficticios."""
    perm_csv = tmp_path / "permanentes.csv"
    trans_csv = tmp_path / "transitorios.csv"
    output_csv = tmp_path / "consolidado.csv"

    # Archivo simulado de permanentes
    perm_csv.write_text(
        "Tipo_cultivo;Año;Id_municipio;Municipio;Id_cultivo;Cultivo;Ciclo;Hectareas_sembradas;Hectareas_cosechadas;Produccion_toneladas;Rendimiento_toneladas/hectareas\n"
        "Frutales;2020;76001;Cali;2040202;Aguacate;Anual;10;8;48;6\n",
        encoding="latin-1",
    )

    # Archivo simulado de transitorios
    trans_csv.write_text(
        "Año;Id_municipio;Municipio;Id_cultivo;Cultivo;Ciclo;Hectareas_sembradas;Hectareas_cosechadas;Produccion_toneladas;Rendimiento_toneladas/hectareas\n"
        "2020;76100;Bolivar;1052000;Melón;Semestre 1;27;27;1139,4;42,2\n",
        encoding="latin-1",
    )

    perm_records = parse_cultivos_permanentes(perm_csv)
    assert len(perm_records) == 1
    assert perm_records[0].tipo_cultivo == "Frutales"
    assert perm_records[0].cultivo == "Aguacate"

    trans_records = parse_cultivos_transitorios(trans_csv)
    assert len(trans_records) == 1
    assert trans_records[0].tipo_cultivo == "Transitorios"
    assert trans_records[0].cultivo == "Melón"

    count = write_consolidated_cultivos_csv(perm_records + trans_records, output_csv)
    assert count == 2
    assert output_csv.exists()


def test_cultivos_dag_loaded_without_errors() -> None:
    """Verifica que el nuevo DAG cultivos_valle_import se cargue sin errores."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado en este entorno")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))

    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.dags.get("cultivos_valle_import")
    assert dag is not None, f"No se encontró el DAG 'cultivos_valle_import'. DAGs: {list(dagbag.dags.keys())}"
    assert dag.dag_id == "cultivos_valle_import"
    expected_tasks = {
        "preparar_entorno",
        "descargar_cultivos_permanentes",
        "descargar_cultivos_transitorios",
        "procesar_y_consolidar_cultivos",
    }
    assert set(dag.task_ids) == expected_tasks
    assert len(dag.tasks) == 4
