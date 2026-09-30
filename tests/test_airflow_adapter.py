from datetime import date
from pathlib import Path

from pyimport.airflow_adapter import resolve_import_config
from pyimport.config_loader import compute_execution_date_range


def test_resolve_import_config_uses_defaults_and_overrides() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    expected = compute_execution_date_range(date.today())

    config = resolve_import_config()
    # SIPSA usa earliest_period (anexos DANE); el fin sigue siendo el del rango de pipeline.
    assert config["start"] == "2012-01"
    assert config["end"] == expected["end_period"]
    assert config["download_dir"] == repo_root / "data" / "raw"
    assert config["output"] == repo_root / "data" / "sipsa_precios.csv"

    override = resolve_import_config({"start": "2024-01", "end": "2024-03"}, env={})
    assert override["start"] == "2024-01"
    assert override["end"] == "2024-03"


def test_execution_date_range_previous_year_end() -> None:
    rng = compute_execution_date_range(date(2026, 8, 17))
    assert rng["start_date"] == "2000-01-01"
    assert rng["end_date"] == "2025-12-31"
    assert rng["start_period"] == "2000-01"
    assert rng["end_period"] == "2025-12"
