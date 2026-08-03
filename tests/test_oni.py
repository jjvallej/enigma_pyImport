"""Pruebas unitarias para el módulo ONI y su DAG en Airflow."""

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

from pyimport.oni import (
    parse_oni_html,
    process_and_generate_oni_csv,
    write_oni_annual_csv,
)


def test_parse_and_generate_oni(tmp_path: Path) -> None:
    """Prueba el parseo y cálculo de promedios anuales ONI desde HTML de muestra."""
    sample_html = tmp_path / "sample_oni.html"
    output_csv = tmp_path / "oni_promedio_anual.csv"

    # HTML simulado con formato NOAA ONI
    sample_html.write_text(
        """
        <table>
        <tr><td style="text-align:center;"><font face="verdana,arial" size="2"><strong>1950</strong></font></td>
            <td style="text-align:center;"><span style="color:blue"><strong><font face="verdana,arial" size="2">-1.5</font></strong></span></td>
            <td style="text-align:center;"><span style="color:blue"><strong><font face="verdana,arial" size="2">-1.3</font></strong></span></td>
            <td style="text-align:center;"><span style="color:blue"><strong><font face="verdana,arial" size="2">-1.2</font></strong></span></td>
        </tr>
        <tr><td style="text-align:center;"><font face="verdana,arial" size="2"><strong>1951</strong></font></td>
            <td style="text-align:center;"><span style="color:red"><strong><font face="verdana,arial" size="2">0.6</font></strong></span></td>
            <td style="text-align:center;"><span style="color:red"><strong><font face="verdana,arial" size="2">0.8</font></strong></span></td>
            <td style="text-align:center;"><span style="color:red"><strong><font face="verdana,arial" size="2">1.0</font></strong></span></td>
        </tr>
        </table>
        """,
        encoding="utf-8",
    )

    records = parse_oni_html(sample_html)
    assert len(records) == 2

    # 1950: (-1.5 + -1.3 + -1.2)/3 = -1.333
    assert records[0].anio == "1950"
    assert records[0].promedio_oni == "-1.333"
    assert records[0].fenomeno_predominante == "La Niña"

    # 1951: (0.6 + 0.8 + 1.0)/3 = 0.800
    assert records[1].anio == "1951"
    assert records[1].promedio_oni == "0.800"
    assert records[1].fenomeno_predominante == "El Niño"

    res = process_and_generate_oni_csv(sample_html, output_csv)
    assert res["years_processed"] == 2
    assert output_csv.exists()


def test_oni_dag_loaded_without_errors() -> None:
    """Verifica que el nuevo DAG oni_fenomeno_nino_import se cargue sin errores."""
    try:
        from airflow.models import DagBag
    except ImportError:
        pytest.skip("apache-airflow no está instalado en este entorno")

    dagbag = DagBag(dag_folder=str(DAGS_DIR))

    assert len(dagbag.import_errors) == 0, f"Errores al importar DAGs: {dagbag.import_errors}"

    dag = dagbag.dags.get("oni_fenomeno_nino_import")
    assert dag is not None, f"No se encontró el DAG 'oni_fenomeno_nino_import'. DAGs: {list(dagbag.dags.keys())}"
    assert dag.dag_id == "oni_fenomeno_nino_import"
    expected_tasks = {
        "preparar_entorno",
        "descargar_html_oni",
        "procesar_y_calcular_promedios",
    }
    assert set(dag.task_ids) == expected_tasks
    assert len(dag.tasks) == 3
