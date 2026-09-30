"""Módulo para la descarga, procesamiento y consolidación de datos agrícolas del Valle del Cauca."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path
import urllib.request
from typing import Iterable

from pyimport.connections import get_connection_base_url
from pyimport.config_loader import build_url, get_connection_id, load_config, require_config_value


def get_cultivos_permanentes_url(conn_id: str | None = None, cfg: dict | None = None) -> str:
    """Retorna la URL de cultivos permanentes desde config + connections.gobernacion_valle."""
    config = cfg or load_config()
    if not conn_id:
        conn_id = get_connection_id(config, "gobernacion_valle")
    base_url = get_connection_base_url(
        conn_id,
        default_host=require_config_value(config, "cultivos", "base_url"),
    )
    return build_url(base_url, require_config_value(config, "cultivos", "permanentes_path"))


def get_cultivos_transitorios_url(conn_id: str | None = None, cfg: dict | None = None) -> str:
    """Retorna la URL de cultivos transitorios desde config + connections.gobernacion_valle."""
    config = cfg or load_config()
    if not conn_id:
        conn_id = get_connection_id(config, "gobernacion_valle")
    base_url = get_connection_base_url(
        conn_id,
        default_host=require_config_value(config, "cultivos", "base_url"),
    )
    return build_url(base_url, require_config_value(config, "cultivos", "transitorios_path"))


def download_cultivo_dataset(url: str, dest_path: Path) -> Path:
    """Descarga un dataset CSV desde la URL indicada."""
    if dest_path.exists():
        dest_path.unlink()
    req = urllib.request.Request(
        url, headers={"User-Agent": "pyimport-cultivos/0.1"}
    )
    with urllib.request.urlopen(req, timeout=60.0) as resp, dest_path.open("wb") as out:
        out.write(resp.read())
    return dest_path


@dataclass
class CultivoRecord:
    tipo_cultivo: str
    anio: str
    id_municipio: str
    municipio: str
    id_cultivo: str
    cultivo: str
    ciclo: str
    hectareas_sembradas: str
    hectareas_cosechadas: str
    produccion_toneladas: str
    rendimiento_toneladas_ha: str


def _norm_header(name: str) -> str:
    import re
    s = (
        str(name or "")
        .strip()
        .lower()
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    return re.sub(r"[^\w]", "", s)


def _row_get(row: dict[str, str], *candidates: str) -> str:
    import re
    normalized = {_norm_header(k): re.sub(r"[*]+", "", v or "").strip() for k, v in row.items()}
    for candidate in candidates:
        cand_norm = _norm_header(candidate)
        if cand_norm in normalized and normalized[cand_norm] != "":
            return normalized[cand_norm]
    for candidate in candidates:
        cand_norm = _norm_header(candidate)
        if cand_norm in normalized:
            return normalized[cand_norm]
    return ""


def parse_cultivos_permanentes(path: Path) -> list[CultivoRecord]:
    """Lee y parsea permanentes por cabecera. tipo_cultivo queda 'Permanente'."""
    records: list[CultivoRecord] = []
    with path.open("r", encoding="latin-1", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            if not row:
                continue
            cultivo = _row_get(row, "Cultivo")
            if not cultivo:
                continue
            ciclo = _row_get(row, "Ciclo") or "Anual"
            anio = _row_get(row, "Año", "Anio", "Ano", "Ao", "A_o")
            records.append(
                CultivoRecord(
                    tipo_cultivo="Permanente",
                    anio=anio,
                    id_municipio=_row_get(row, "Id_municipio"),
                    municipio=_row_get(row, "Municipio"),
                    id_cultivo=_row_get(row, "Id_cultivo"),
                    cultivo=cultivo,
                    ciclo=ciclo,
                    hectareas_sembradas=_row_get(row, "Hectareas_sembradas"),
                    hectareas_cosechadas=_row_get(row, "Hectareas_cosechadas"),
                    produccion_toneladas=_row_get(row, "Produccion_toneladas"),
                    rendimiento_toneladas_ha=_row_get(
                        row, "Rendimiento_toneladas/hectareas", "Rendimiento_toneladas_ha"
                    ),
                )
            )
    return records


def parse_cultivos_transitorios(path: Path) -> list[CultivoRecord]:
    """Lee y parsea transitorios por cabecera. tipo_cultivo queda 'Transitorio'."""
    records: list[CultivoRecord] = []
    with path.open("r", encoding="latin-1", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            if not row:
                continue
            cultivo = _row_get(row, "Cultivo")
            if not cultivo:
                continue
            ciclo = _row_get(row, "Ciclo") or "Anual"
            anio = _row_get(row, "Año", "Anio", "Ano", "Ao", "A_o")
            records.append(
                CultivoRecord(
                    tipo_cultivo="Transitorio",
                    anio=anio,
                    id_municipio=_row_get(row, "Id_municipio"),
                    municipio=_row_get(row, "Municipio"),
                    id_cultivo=_row_get(row, "Id_cultivo"),
                    cultivo=cultivo,
                    ciclo=ciclo,
                    hectareas_sembradas=_row_get(row, "Hectareas_sembradas"),
                    hectareas_cosechadas=_row_get(row, "Hectareas_cosechadas"),
                    produccion_toneladas=_row_get(row, "Produccion_toneladas"),
                    rendimiento_toneladas_ha=_row_get(
                        row, "Rendimiento_toneladas/hectareas", "Rendimiento_toneladas_ha"
                    ),
                )
            )
    return records


def write_consolidated_cultivos_csv(
    records: Iterable[CultivoRecord], output_path: Path
) -> int:
    """Escribe la lista de CultivoRecord consolidados en el archivo CSV de salida."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "tipo_cultivo",
        "anio",
        "id_municipio",
        "municipio",
        "id_cultivo",
        "cultivo",
        "ciclo",
        "hectareas_sembradas",
        "hectareas_cosechadas",
        "produccion_toneladas",
        "rendimiento_toneladas_ha",
    ]
    count = 0
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(asdict(rec))
            count += 1
    return count


def import_and_consolidate_cultivos(
    permanentes_path: Path, transitorios_path: Path, output_path: Path
) -> dict[str, object]:
    """Parsea ambos archivos y genera el CSV consolidado."""
    perm_records = parse_cultivos_permanentes(permanentes_path)
    trans_records = parse_cultivos_transitorios(transitorios_path)
    all_records = perm_records + trans_records
    written = write_consolidated_cultivos_csv(all_records, output_path)
    return {
        "permanentes_rows": len(perm_records),
        "transitorios_rows": len(trans_records),
        "total_rows": written,
        "output": str(output_path),
    }
