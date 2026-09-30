"""Pruebas para la integración del DAG src_ingest_sipsa."""

from __future__ import annotations

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


def test_dag_loaded_without_errors() -> None:
    """Verifica que los DAGs src_* se importen correctamente."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado en este entorno")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))
    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.dags.get("src_ingest_sipsa")
    assert dag is not None, f"No se encontró el DAG 'src_ingest_sipsa'."
    assert dag.dag_id == "src_ingest_sipsa"
    assert "run_ingest_sipsa" in dag.task_ids

    dag_orch = dagbag.dags.get("src_orchestrator_master")
    assert dag_orch is not None, "No se encontró el DAG Orquestador Maestro 'src_orchestrator_master'."
    assert dag_orch.dag_id == "src_orchestrator_master"
    assert "trigger_ingest_crops" in dag_orch.task_ids
    assert "trigger_transform_spatial" in dag_orch.task_ids

    # DAG 1: Importación de Sentimiento (Diario, Pausado/Deshabilitado)
    dag_sent = dagbag.dags.get("src_importacion_sentimiento")
    assert dag_sent is not None, "No se encontró el DAG 'src_importacion_sentimiento'."
    assert dag_sent.dag_id == "src_importacion_sentimiento"
    assert dag_sent.is_paused_upon_creation is True
    assert "task_ingest_ckan_comentarios" in dag_sent.task_ids
    assert "task_load_ckan_comentarios" in dag_sent.task_ids
    assert "task_transform_ckan_comentarios" in dag_sent.task_ids

    # DAG 2: Importación de Cultivos (Anual, Orquestación Secuencial hasta Gold)
    dag_cult = dagbag.dags.get("src_importacion_cultivos")
    assert dag_cult is not None, "No se encontró el DAG 'src_importacion_cultivos'."
    assert dag_cult.dag_id == "src_importacion_cultivos"
    assert "trigger_ingest_crops" in dag_cult.task_ids
    assert "trigger_ingest_sipsa" in dag_cult.task_ids
    assert "trigger_ingest_oni" in dag_cult.task_ids
    assert "trigger_ingest_municipios" in dag_cult.task_ids
    assert "trigger_transform_consolidado" in dag_cult.task_ids
    assert "trigger_transform_spatial" in dag_cult.task_ids


