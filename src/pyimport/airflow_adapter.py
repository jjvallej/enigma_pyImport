"""Adaptador para ejecutar la importación SIPSA desde Apache Airflow."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

from pyimport.config_loader import load_config, require_config_value
from pyimport.sipsa import (
    FoodRow,
    extract_food_rows,
    import_sipsa,
    iter_periods,
    parse_period_arg,
    resolve_and_download,
    write_csv,
)


def _resolve_path(value: str | Path | None, *, default: Path, repo_root: Path) -> Path:
    if value is None:
        return default
    path = Path(value)
    if path.is_absolute():
        return path
    return repo_root / path


def resolve_import_config(
    overrides: Mapping[str, Any] | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Resuelve la configuración desde overrides, env o config.yaml."""
    repo_root = Path(__file__).resolve().parents[2]
    env_map = os.environ if env is None else env
    cfg = load_config()
    sipsa_cfg = require_config_value(cfg, "sipsa")
    paths_cfg = require_config_value(cfg, "paths")

    overrides = {} if overrides is None else overrides
    start = str(
        overrides.get("start")
        or env_map.get("SIPSA_START")
        or require_config_value(sipsa_cfg, "start_period")
    )
    end = str(
        overrides.get("end")
        or env_map.get("SIPSA_END")
        or require_config_value(sipsa_cfg, "end_period")
    )
    download_dir = _resolve_path(
        overrides.get("download_dir") or env_map.get("SIPSA_DOWNLOAD_DIR"),
        default=repo_root / require_config_value(paths_cfg, "raw_dir"),
        repo_root=repo_root,
    )
    output = _resolve_path(
        overrides.get("output") or env_map.get("SIPSA_OUTPUT"),
        default=repo_root / require_config_value(paths_cfg, "sipsa_csv"),
        repo_root=repo_root,
    )

    return {
        "start": start,
        "end": end,
        "download_dir": download_dir,
        "output": output,
    }


def download_attachments(
    start_period: str, end_period: str, download_dir: str | Path
) -> dict[str, list[str]]:
    """Paso 1: Descarga los anexos mensuales SIPSA."""
    start = parse_period_arg(start_period)
    end = parse_period_arg(end_period)
    dir_path = Path(download_dir)

    downloaded: list[str] = []
    missing: list[str] = []

    for period in iter_periods(start, end):
        label = f"{period.year}-{period.month_str}"
        try:
            path = resolve_and_download(period, dir_path)
            downloaded.append(str(path))
        except FileNotFoundError:
            missing.append(label)

    return {"downloaded": downloaded, "missing": missing}


def extract_and_process_prices(file_paths: list[str]) -> list[dict[str, str]]:
    """Paso 2: Extrae los precios de alimentos de los archivos Excel."""
    all_rows: list[dict[str, str]] = []
    for file_path in file_paths:
        path = Path(file_path)
        rows = extract_food_rows(path)
        for r in rows:
            all_rows.append(
                {
                    "year": r.year,
                    "month": r.month,
                    "alimento": r.alimento,
                    "valor": r.valor,
                }
            )
    return all_rows


def generate_consolidated_csv(
    rows_data: list[dict[str, str]], output_path: str | Path
) -> dict[str, object]:
    """Paso 3: Genera el archivo CSV consolidado."""
    rows = [
        FoodRow(
            year=r["year"],
            month=r["month"],
            alimento=r["alimento"],
            valor=r["valor"],
        )
        for r in rows_data
    ]
    written = write_csv(rows, Path(output_path))
    return {"rows": written, "output": str(output_path)}


def run_import(
    overrides: Mapping[str, Any] | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Ejecuta la importación SIPSA completa usando la configuración."""
    config = resolve_import_config(overrides, env=env)
    start = parse_period_arg(str(config["start"]))
    end = parse_period_arg(str(config["end"]))
    return import_sipsa(
        start,
        end,
        download_dir=config["download_dir"],
        output=config["output"],
    )
