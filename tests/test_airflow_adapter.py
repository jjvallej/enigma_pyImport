from pathlib import Path

from pyimport.airflow_adapter import resolve_import_config


def test_resolve_import_config_uses_defaults_and_overrides() -> None:
    repo_root = Path(__file__).resolve().parents[1]

    config = resolve_import_config()
    assert config["start"] == "2015-02"
    assert config["end"] == "2026-06"
    assert config["download_dir"] == repo_root / "data" / "raw"
    assert config["output"] == repo_root / "data" / "sipsa_precios.csv"

    override = resolve_import_config({"start": "2024-01", "end": "2024-03"}, env={})
    assert override["start"] == "2024-01"
    assert override["end"] == "2024-03"
