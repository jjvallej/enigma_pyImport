"""Pruebas unitarias para la consolidación maestra de los 3 datasets y su DAG."""

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

from pyimport.consolidate import consolidate_all_datasets


def test_consolidate_all_datasets(tmp_path: Path) -> None:
    """Verifica la integración y cruce de datos entre cultivos, precios SIPSA y clima ONI."""
    cultivos_csv = tmp_path / "cultivos_valle.csv"
    sipsa_csv = tmp_path / "sipsa_precios.csv"
    oni_csv = tmp_path / "oni_promedio_anual.csv"
    output_csv = tmp_path / "dataset_consolidado_valle.csv"

    # Muestra de cultivos
    cultivos_csv.write_text(
        "tipo_cultivo,anio,id_municipio,municipio,id_cultivo,cultivo,ciclo,hectareas_sembradas,hectareas_cosechadas,produccion_toneladas,rendimiento_toneladas_ha\n"
        "Frutales,2020,76001,Cali,2040202,Aguacate,Anual,10,8,48,6\n",
        encoding="utf-8",
    )

    # Muestra de precios SIPSA
    sipsa_csv.write_text(
        "anio,mes,alimento,valor\n"
        "2020,01,Aguacate papelillo,3000\n"
        "2020,02,Aguacate *,3200\n",
        encoding="utf-8",
    )

    # Muestra de clima ONI
    oni_csv.write_text(
        "anio,promedio_oni,num_periodos,fenomeno_predominante\n"
        "2020,-0.275,12,Neutro\n",
        encoding="utf-8",
    )

    result = consolidate_all_datasets(cultivos_csv, sipsa_csv, oni_csv, output_csv)

    assert result["total_rows"] == 1
    assert result["matched_price_rows"] == 1
    assert result["matched_oni_rows"] == 1
    assert output_csv.exists()


def test_consolidado_dag_loaded_without_errors() -> None:
    """Verifica que el nuevo DAG dataset_consolidado_master_import se cargue sin errores."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado en este entorno")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))

    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.dags.get("dataset_consolidado_master_import")
    assert dag is not None, f"No se encontró el DAG 'dataset_consolidado_master_import'. DAGs: {list(dagbag.dags.keys())}"
    assert dag.dag_id == "dataset_consolidado_master_import"
    expected_tasks = {
        "preparar_entorno",
        "cargar_y_mapear_precios_sipsa",
        "cargar_y_mapear_clima_oni",
        "generar_dataset_consolidado_master",
    }
    assert set(dag.task_ids) == expected_tasks
    assert len(dag.tasks) == 4
