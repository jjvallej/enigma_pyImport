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

from pyimport.consolidate import consolidar_bronze_a_silver


def test_consolidar_bronze_a_silver_execution(tmp_path: Path) -> None:
    """Verifica la ejecución de consolidar_bronze_a_silver con archivos de prueba."""
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    perm_csv = raw_dir / "cultivos_permanentes.csv"
    trans_csv = raw_dir / "cultivos_transitorios.csv"
    sipsa_csv = tmp_path / "sipsa_precios.csv"
    oni_csv = tmp_path / "oni_promedio_anual.csv"

    perm_csv.write_text(
        "Tipo_cultivo;Año;Id_municipio;Municipio;Id_cultivo;Cultivo;Ciclo;Hectareas_sembradas;Hectareas_cosechadas;Produccion_toneladas;Rendimiento_toneladas/hectareas\n"
        "Frutales;2020;76001;Cali;2040202;Aguacate;Anual;10;8;48;6\n",
        encoding="latin1",
    )

    trans_csv.write_text(
        "Año;Id_municipio;Municipio;Id_cultivo;Cultivo;Ciclo;Hectareas_sembradas;Hectareas_cosechadas;Produccion_toneladas;Rendimiento_toneladas/hectareas\n"
        "2020;76001;Cali;1010101;Maíz;A;15;12;30;2.5\n"
        "2020;76001;Cali;1010101;Maíz;B;10;8;20;2.5\n",
        encoding="latin1",
    )

    sipsa_csv.write_text(
        "anio,mes,alimento,valor\n2020,01,Aguacate,3000\n2020,01,Maiz,1500\n",
        encoding="utf-8",
    )

    oni_csv.write_text(
        "anio,promedio_oni,num_periodos,fenomeno_predominante\n2020,-0.275,12,Neutro\n",
        encoding="utf-8",
    )

    custom_cfg = {
        "paths": {
            "raw_dir": str(raw_dir),
            "output_dir": str(tmp_path),
            "sipsa_csv": str(sipsa_csv),
            "oni_csv": str(oni_csv),
            "dataset_consolidado": str(tmp_path / "dataset_consolidado_valle.csv"),
        },
        "cultivos": {
            "permanentes_filename": "cultivos_permanentes.csv",
            "transitorios_filename": "cultivos_transitorios.csv",
        },
        "bigquery": {
            "project_id": "test-project",
            "location": "US",
            "datasets": {"silver": "silver_test"},
            "tables": {"consolidado_silver": "silver_consolidado_test"},
        },
    }

    res = consolidar_bronze_a_silver(custom_cfg)

    assert res["status"] in ("SUCCESS", "SIMULATED")
    # 1 permanente + 2 transitorios (semestre A y B) = 3 filas
    assert res["total_rows"] == 3
    assert Path(res["output_csv"]).exists()
    content = Path(res["output_csv"]).read_text(encoding="utf-8")
    assert "semestre" in content.splitlines()[0]


def test_consolidado_dag_loaded_without_errors() -> None:
    """Verifica que el DAG consolidado_valle_import se cargue sin errores."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado en este entorno")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))
    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.dags.get("src_transform_consolidado") or dagbag.dags.get("src_transform_crops")
    assert dag is not None, "No se encontró DAG consolidado Silver."
    assert "run_transform" in " ".join(dag.task_ids) or any(
        "consolidado" in t or "crops" in t for t in dag.task_ids
    )

