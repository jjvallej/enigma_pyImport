"""Módulo para la descarga y procesamiento del Índice Climático ONI (NOAA)."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import csv

from pathlib import Path
import re
from typing import Iterable
import urllib.request


@dataclass(frozen=True)
class AnnualONIRecord:
    anio: str
    promedio_oni: str
    num_periodos: int
    fenomeno_predominante: str


def get_oni_url(connection_id: str | None = None, cfg: dict | None = None) -> str:
    """Resuelve la URL ONI desde config (oni.url / oni.path) y connections.noaa_oni."""
    from pyimport.config_loader import build_url, get_connection_id, load_config, require_config_value
    from pyimport.connections import get_connection_base_url

    config = cfg or load_config()
    if not connection_id:
        connection_id = get_connection_id(config, "noaa_oni")
    base_url = get_connection_base_url(
        connection_id,
        default_host=require_config_value(config, "oni", "base_url"),
    )
    path = require_config_value(config, "oni", "path")
    return build_url(base_url, path)


def download_oni_html(url: str | None = None, dest_path: Path | None = None) -> Path:
    """Descarga el archivo HTML desde la URL de la NOAA CPC hacia la ruta especificada."""
    if url is None:
        url = get_oni_url()
    if dest_path is None:
        raise ValueError("dest_path es requerido")

    if dest_path.exists():
        dest_path.unlink()
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
        },
    )
    with urllib.request.urlopen(req, timeout=60.0) as resp, dest_path.open(
        "w", encoding="utf-8"
    ) as out:
        out.write(resp.read().decode("utf-8", errors="ignore"))
    return dest_path


def parse_oni_html(html_path: Path) -> list[AnnualONIRecord]:
    """Parsea el HTML de ONI v5 y calcula el promedio de temperatura anual para cada año."""
    html_content = html_path.read_text(encoding="utf-8", errors="ignore")

    year_rows = re.findall(
        r"<tr>\s*<td[^>]*>(?:<[^>]+>)*(\d{4})(?:<[^>]+>)*</td>(.*?)</tr>",
        html_content,
        re.DOTALL,
    )

    records: list[AnnualONIRecord] = []
    for year, cells_content in year_rows:
        values = [float(v) for v in re.findall(r"(-?\d+\.\d+)", cells_content)]
        if not values:
            continue
        avg_temp = sum(values) / len(values)

        if avg_temp >= 0.5:
            fenomeno = "El Niño"
        elif avg_temp <= -0.5:
            fenomeno = "La Niña"
        else:
            fenomeno = "Neutro"

        records.append(
            AnnualONIRecord(
                anio=year,
                promedio_oni=f"{avg_temp:.3f}",
                num_periodos=len(values),
                fenomeno_predominante=fenomeno,
            )
        )

    return records


def write_oni_annual_csv(
    records: Iterable[AnnualONIRecord], output_path: Path
) -> int:
    """Escribe la lista de promedios anuales ONI en el archivo CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "anio",
        "promedio_oni",
        "num_periodos",
        "fenomeno_predominante",
    ]
    count = 0
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(asdict(rec))
            count += 1
    return count


def process_and_generate_oni_csv(
    html_path: Path, output_path: Path
) -> dict[str, object]:
    """Parsea el HTML descargado y genera el CSV consolidado por año."""
    records = parse_oni_html(html_path)
    written = write_oni_annual_csv(records, output_path)
    return {
        "years_processed": written,
        "output": str(output_path),
    }
