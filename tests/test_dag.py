"""Pruebas para la integración del DAG de Apache Airflow."""

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


def test_dag_loaded_without_errors() -> None:
    """Verifica que el DAG sipsa_import se importe correctamente y no tenga errores de sintaxis o importación."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado en este entorno")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))

    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.dags.get("sipsa_import")
    assert dag is not None, f"No se encontró el DAG 'sipsa_import'. DAGs cargados: {list(dagbag.dags.keys())}"
    assert dag.dag_id == "sipsa_import"
    expected_tasks = {
        "preparar_entorno",
        "descargar_anexos_sipsa",
        "extraer_y_procesar_precios",
        "generar_csv_consolidado",
    }
    assert set(dag.task_ids) == expected_tasks
    assert len(dag.tasks) == 4
