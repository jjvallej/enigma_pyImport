"""Módulo para la descarga, procesamiento y consolidación de datos agrícolas del Valle del Cauca."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path
import urllib.request
from typing import Iterable

from pyimport.connections import CONN_GOBERNACION_VALLE, get_connection_base_url

PERMANENTES_PATH = (
    "/dataset/55a3d384-fec1-4267-8541-7d62a3fc9223/resource/"
    "7c578f9f-094d-4e6e-b1db-2f5e3bde32c9/download/cultivos_permanentes.csv"
)

TRANSITORIOS_PATH = (
    "/dataset/16a0cede-1b2f-4db8-8a42-ca10d0223cee/resource/"
    "f0ed7211-5ab5-4fa0-885d-e343cc906f2c/download/cultivos_transitorios.csv"
)


def get_cultivos_permanentes_url(conn_id: str = CONN_GOBERNACION_VALLE) -> str:
    """Retorna la URL de cultivos permanentes usando la Conexión de Airflow."""
    base_url = get_connection_base_url(conn_id)
    return f"{base_url}{PERMANENTES_PATH}"


def get_cultivos_transitorios_url(conn_id: str = CONN_GOBERNACION_VALLE) -> str:
    """Retorna la URL de cultivos transitorios usando la Conexión de Airflow."""
    base_url = get_connection_base_url(conn_id)
    return f"{base_url}{TRANSITORIOS_PATH}"


CULTIVOS_PERMANENTES_URL = get_cultivos_permanentes_url()
CULTIVOS_TRANSITORIOS_URL = get_cultivos_transitorios_url()


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


def download_cultivo_dataset(url: str, dest_path: Path) -> Path:
    """Descarga un dataset CSV desde la URL indicada."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(
        url, headers={"User-Agent": "pyimport-cultivos/0.1"}
    )
    with urllib.request.urlopen(req, timeout=60.0) as resp, dest_path.open("wb") as out:
        out.write(resp.read())
    return dest_path


def parse_cultivos_permanentes(path: Path) -> list[CultivoRecord]:
    """Lee y parsea el archivo de cultivos permanentes."""
    records: list[CultivoRecord] = []
    with path.open("r", encoding="latin-1", errors="replace") as handle:
        reader = csv.reader(handle, delimiter=";")
        next(reader, None)  # Ignorar cabecera
        for row in reader:
            if not row or len(row) < 11:
                continue
            records.append(
                CultivoRecord(
                    tipo_cultivo=row[0].strip(),
                    anio=row[1].strip(),
                    id_municipio=row[2].strip(),
                    municipio=row[3].strip(),
                    id_cultivo=row[4].strip(),
                    cultivo=row[5].strip(),
                    ciclo=row[6].strip(),
                    hectareas_sembradas=row[7].strip(),
                    hectareas_cosechadas=row[8].strip(),
                    produccion_toneladas=row[9].strip(),
                    rendimiento_toneladas_ha=row[10].strip(),
                )
            )
    return records


def parse_cultivos_transitorios(path: Path) -> list[CultivoRecord]:
    """Lee y parsea el archivo de cultivos transitorios (asignando tipo_cultivo='Transitorios')."""
    records: list[CultivoRecord] = []
    with path.open("r", encoding="latin-1", errors="replace") as handle:
        reader = csv.reader(handle, delimiter=";")
        next(reader, None)  # Ignorar cabecera
        for row in reader:
            if not row or len(row) < 10:
                continue
            records.append(
                CultivoRecord(
                    tipo_cultivo="Transitorios",
                    anio=row[0].strip(),
                    id_municipio=row[1].strip(),
                    municipio=row[2].strip(),
                    id_cultivo=row[3].strip(),
                    cultivo=row[4].strip(),
                    ciclo=row[5].strip(),
                    hectareas_sembradas=row[6].strip(),
                    hectareas_cosechadas=row[7].strip(),
                    produccion_toneladas=row[8].strip(),
                    rendimiento_toneladas_ha=row[9].strip(),
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
