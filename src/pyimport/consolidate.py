"""Módulo para consolidar los 3 conjuntos de datos (Cultivos Valle, Precios SIPSA y Fenómeno El Niño ONI)."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
import re
from pathlib import Path
import unicodedata
from typing import Iterable, Mapping

ALIAS_MAP = {
    "cebolla bulbo": "cebolla cabezona",
    "cebolla larga": "cebolla junca",
    "caña de azúcar": "azúcar",
    "caña panelera": "panela",
    "plátano": "platano",
}


@dataclass
class MasterConsolidatedRecord:
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
    precio_promedio_anual_sipsa: str
    promedio_oni: str
    fenomeno_predominante: str


def _clean_text(text: str) -> str:
    """Remueve caracteres especiales, acentos y convierte a minúsculas para comparaciones flexibles."""
    text = re.sub(r"[*#]", "", text)
    text = unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("utf-8")
    return text.lower().strip()


def build_sipsa_annual_prices(sipsa_csv_path: Path) -> dict[tuple[str, str], float]:
    """Lee el CSV de precios SIPSA y calcula el promedio anual por (año, producto_limpio)."""
    sipsa_by_year_product: dict[tuple[str, str], list[float]] = {}
    with sipsa_csv_path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for r in reader:
            val_str = r.get("valor", "").replace(",", ".").strip()
            try:
                val = float(val_str)
            except ValueError:
                continue
            year = r.get("anio", "").strip()
            alimento_clean = _clean_text(r.get("alimento", ""))
            if year and alimento_clean:
                sipsa_by_year_product.setdefault((year, alimento_clean), []).append(val)

    # Promedio anual por producto exacto limpiado
    return {k: sum(v) / len(v) for k, v in sipsa_by_year_product.items()}


def find_sipsa_price_for_crop(
    crop_name: str, year: str, sipsa_prices: dict[tuple[str, str], float]
) -> float | None:
    """Busca el precio anual en SIPSA para un cultivo comparando por coincidencia exacta, alias o raíz de palabra."""
    crop_clean = _clean_text(crop_name)
    if not crop_clean:
        return None

    # Coincidencia exacta o mediante alias
    crop_alias = ALIAS_MAP.get(crop_name.lower(), crop_clean)

    # Filtrar productos disponibles para el año indicado
    foods_in_year = {
        food: price
        for (yr, food), price in sipsa_prices.items()
        if yr == year
    }
    if not foods_in_year:
        return None

    # 1. Coincidencia exacta
    if crop_clean in foods_in_year:
        return foods_in_year[crop_clean]
    if crop_alias in foods_in_year:
        return foods_in_year[crop_alias]

    # 2. Coincidencia por raíz principal / subsecuencia
    crop_first_word = crop_clean.split()[0] if crop_clean else ""
    matched_prices: list[float] = []

    if len(crop_first_word) >= 3:
        for food_name, price in foods_in_year.items():
            food_words = food_name.split()
            if (
                crop_first_word in food_words
                or food_name.startswith(crop_first_word)
                or crop_alias in food_name
            ):
                matched_prices.append(price)

    if matched_prices:
        return sum(matched_prices) / len(matched_prices)
    return None


def load_oni_data(oni_csv_path: Path) -> dict[str, dict[str, str]]:
    """Carga los promedios anuales y fenómenos climáticos por año."""
    oni_data: dict[str, dict[str, str]] = {}
    with oni_csv_path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for r in reader:
            year = r.get("anio", "").strip()
            if year:
                oni_data[year] = {
                    "promedio_oni": r.get("promedio_oni", "").strip(),
                    "fenomeno_predominante": r.get("fenomeno_predominante", "").strip(),
                }
    return oni_data


def consolidate_all_datasets(
    cultivos_csv_path: Path,
    sipsa_csv_path: Path,
    oni_csv_path: Path,
    output_csv_path: Path,
) -> dict[str, object]:
    """Genera el dataset consolidado maestro uniendo cultivos, precios SIPSA y fenómeno ONI."""
    sipsa_prices = build_sipsa_annual_prices(sipsa_csv_path)
    oni_data = load_oni_data(oni_csv_path)

    master_records: list[MasterConsolidatedRecord] = []
    matched_price_count = 0
    matched_oni_count = 0

    with cultivos_csv_path.open("r", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for r in reader:
            year = r.get("anio", "").strip()
            crop = r.get("cultivo", "").strip()

            price_val = find_sipsa_price_for_crop(crop, year, sipsa_prices)
            if price_val is not None:
                price_str = f"{price_val:.2f}"
                matched_price_count += 1
            else:
                price_str = ""

            oni_info = oni_data.get(year, {})
            promedio_oni = oni_info.get("promedio_oni", "")
            fenomeno = oni_info.get("fenomeno_predominante", "")
            if promedio_oni:
                matched_oni_count += 1

            record = MasterConsolidatedRecord(
                tipo_cultivo=r.get("tipo_cultivo", "").strip(),
                anio=year,
                id_municipio=r.get("id_municipio", "").strip(),
                municipio=r.get("municipio", "").strip(),
                id_cultivo=r.get("id_cultivo", "").strip(),
                cultivo=crop,
                ciclo=r.get("ciclo", "").strip(),
                hectareas_sembradas=r.get("hectareas_sembradas", "").strip(),
                hectareas_cosechadas=r.get("hectareas_cosechadas", "").strip(),
                produccion_toneladas=r.get("produccion_toneladas", "").strip(),
                rendimiento_toneladas_ha=r.get("rendimiento_toneladas_ha", "").strip(),
                precio_promedio_anual_sipsa=price_str,
                promedio_oni=promedio_oni,
                fenomeno_predominante=fenomeno,
            )
            master_records.append(record)

    # Escribir archivo CSV consolidado maestro
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
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
        "precio_promedio_anual_sipsa",
        "promedio_oni",
        "fenomeno_predominante",
    ]

    count = 0
    with output_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for rec in master_records:
            writer.writerow(asdict(rec))
            count += 1

    return {
        "total_rows": count,
        "matched_price_rows": matched_price_count,
        "matched_oni_rows": matched_oni_count,
        "output": str(output_csv_path),
    }
