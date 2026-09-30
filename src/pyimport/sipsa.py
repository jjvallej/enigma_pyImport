"""Descarga e importa anexos mensuales SIPSA (DANE) a un CSV unificado."""

from __future__ import annotations

import csv
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator

MONTH_ABBR = {
    "ene": "01",
    "feb": "02",
    "mar": "03",
    "abr": "04",
    "may": "05",
    "jun": "06",
    "jul": "07",
    "ago": "08",
    "sep": "09",
    "oct": "10",
    "nov": "11",
    "dic": "12",
}

MONTH_NUM_TO_ABBR = {v: k for k, v in MONTH_ABBR.items()}
MONTH_TOKEN = "|".join(MONTH_ABBR)

from pyimport.connections import get_connection_base_url
from pyimport.config_loader import get_connection_id, load_config, require_config_value


def get_sipsa_path_templates(cfg: dict | None = None) -> tuple[str, ...]:
    """Lee sipsa.path_templates desde config.yaml."""
    config = cfg or load_config()
    templates = require_config_value(config, "sipsa", "path_templates")
    return tuple(str(t) for t in templates)


def get_sipsa_url_templates(conn_id: str | None = None, cfg: dict | None = None) -> tuple[str, ...]:
    """Retorna plantillas de URL SIPSA (base de conexión + paths de config)."""
    config = cfg or load_config()
    if not conn_id:
        conn_id = get_connection_id(config, "sipsa_dane")
    base_url = get_connection_base_url(
        conn_id,
        default_host=require_config_value(config, "sipsa", "base_url"),
    )
    return tuple(f"{base_url.rstrip('/')}{tmpl}" for tmpl in get_sipsa_path_templates(config))


FILENAME_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"^anex_mensual_(?P<mes>{MONTH_TOKEN})_(?P<anio>\d{{4}})\.(?:xls|xlsx)$",
        re.IGNORECASE,
    ),
    re.compile(
        rf"^anexo_mensual_SIPSA_mayoristas_(?P<mes>{MONTH_TOKEN})_(?P<anio>\d{{4}})\.xlsx$",
        re.IGNORECASE,
    ),
    re.compile(
        rf"^anex-SIPSAMensual-(?P<mes>{MONTH_TOKEN})(?P<anio>\d{{4}})\.xlsx$",
        re.IGNORECASE,
    ),
)

# Filas de categoría / notas que no son alimentos.
SKIP_NAMES = {
    "hortalizas y verduras",
    "frutas frescas",
    "tubérculos y plátanos",
    "tuberculos y platanos",
    "granos, cárnicos y procesados",
    "granos, carnicos y procesados",
    "producto",
    "precio $/kg",
    "fuente: dane",
}

SKIP_PREFIXES = (
    "anexo ",
    "variación",
    "variacion",
    "comportamiento",
    "sistema de información",
    "sistema de informacion",
    "var%",
    "n.d.",
    "fuente:",
    "*variedad",
    "-:",
)


@dataclass(frozen=True)
class Period:
    year: int
    month: int  # 1-12

    @property
    def month_abbr(self) -> str:
        return MONTH_NUM_TO_ABBR[f"{self.month:02d}"]

    @property
    def month_str(self) -> str:
        return f"{self.month:02d}"

    @property
    def year_str(self) -> str:
        return str(self.year)


@dataclass(frozen=True)
class FoodRow:
    year: str
    month: str
    alimento: str
    valor: str


def parse_period_from_filename(filename: str) -> Period:
    name = Path(filename).name
    for pattern in FILENAME_PATTERNS:
        match = pattern.match(name)
        if match:
            return Period(
                year=int(match.group("anio")),
                month=int(MONTH_ABBR[match.group("mes").lower()]),
            )
    raise ValueError(f"Nombre de archivo no reconocido: {filename}")


def candidate_urls(period: Period, conn_id: str | None = None) -> list[str]:
    templates = get_sipsa_url_templates(conn_id)
    return [
        template.format(mes=period.month_abbr, anio=period.year)
        for template in templates
    ]


def candidate_filenames(period: Period) -> list[str]:
    return [Path(url).name for url in candidate_urls(period)]


def build_url(period: Period, template_index: int = 0) -> str:
    return candidate_urls(period)[template_index]


def iter_periods(start: Period, end: Period) -> Iterator[Period]:
    if (start.year, start.month) > (end.year, end.month):
        raise ValueError("El periodo inicial no puede ser posterior al final")
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield Period(year=year, month=month)
        month += 1
        if month > 12:
            month = 1
            year += 1


def parse_period_arg(value: str) -> Period:
    """Acepta YYYY-MM o YYYY-M."""
    parts = value.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"Periodo inválido '{value}'. Use YYYY-MM")
    year, month = int(parts[0]), int(parts[1])
    if month < 1 or month > 12:
        raise ValueError(f"Mes inválido en '{value}'")
    return Period(year=year, month=month)


def _request(url: str, *, method: str = "GET", timeout: float = 30.0):
    req = urllib.request.Request(
        url,
        method=method,
        headers={"User-Agent": "pyimport/0.1"},
    )
    return urllib.request.urlopen(req, timeout=timeout)


def url_exists(url: str, timeout: float = 20.0) -> bool:
    """Comprueba existencia con HEAD; si el servidor no lo soporta, usa GET parcial."""
    try:
        with _request(url, method="HEAD", timeout=timeout) as response:
            return 200 <= response.status < 300
    except urllib.error.HTTPError as exc:
        if exc.code == 405:
            try:
                with _request(url, method="GET", timeout=timeout) as response:
                    return 200 <= response.status < 300
            except urllib.error.HTTPError as get_exc:
                return get_exc.code != 404
            except Exception:  # noqa: BLE001
                return False
        return False
    except Exception:  # noqa: BLE001
        return False


def download_file(url: str, dest: Path, timeout: float = 60.0) -> None:
    if dest.exists():
        dest.unlink()
    with _request(url, method="GET", timeout=timeout) as response, dest.open("wb") as out:
        out.write(response.read())


def find_local_file(period: Period, download_dir: Path) -> Path | None:
    for name in candidate_filenames(period):
        local = download_dir / name
        if local.exists() and local.stat().st_size > 0:
            return local
    return None


def resolve_and_download(period: Period, download_dir: Path) -> Path:
    """Prueba los esquemas de URL conocidos y recrea el archivo destino."""
    last_error: Exception | None = None
    for url in candidate_urls(period):
        if not url_exists(url):
            continue
        local = download_dir / Path(url).name
        try:
            download_file(url, local)
            return local
        except urllib.error.HTTPError as exc:
            last_error = exc
            if local.exists():
                local.unlink(missing_ok=True)
            if exc.code != 404:
                raise
        except Exception as exc:  # noqa: BLE001 - se reintenta con otro esquema
            last_error = exc
            if local.exists():
                local.unlink(missing_ok=True)

    raise FileNotFoundError(
        f"No se encontró anexo para {period.year}-{period.month_str}: {last_error}"
    )


def _normalize_cell(value: object) -> str:
    import re
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        val_str = str(int(value))
    else:
        val_str = str(value)
    return re.sub(r"[*]+", "", val_str).strip()


def _is_food_name(name: str) -> bool:
    if not name:
        return False
    lower = name.lower()
    if lower in SKIP_NAMES:
        return False
    if any(lower.startswith(prefix) for prefix in SKIP_PREFIXES):
        return False
    if lower in {"precio", "var %", "var%"}:
        return False
    return True


def _is_usable_value(value: str) -> bool:
    return bool(value)


def _sheet_looks_like_price_table(rows: list[tuple[str, str]]) -> bool:
    """Detecta la tabla de precios mensuales (col A alimento, col J precio Cali)."""
    for alimento, valor in rows[:40]:
        if alimento.lower() == "precio $/kg" and valor.lower() == "cali":
            return True
    # Fallback: varias filas alimento + valor numérico / n.d.
    hits = 0
    for alimento, valor in rows:
        if not _is_food_name(alimento) or not valor:
            continue
        lower = valor.lower()
        if lower in {"n.d.", "-", "precio", "var %"}:
            hits += 1
            continue
        try:
            float(valor.replace(",", "."))
            hits += 1
        except ValueError:
            continue
        if hits >= 5:
            return True
    return hits >= 5


def _pick_sheet_name(sheet_names: list[str]) -> str:
    preferred = {"anexo 1", "1"}
    for name in sheet_names:
        if name.strip().lower() in preferred:
            return name
    # Evitar índice; preferir primera hoja de datos.
    for name in sheet_names:
        if name.strip().lower() not in {"indice", "índice", "index"}:
            return name
    return sheet_names[-1]


def _read_all_sheets_xls(path: Path) -> list[tuple[str, list[tuple[str, str]]]]:
    import xlrd

    workbook = xlrd.open_workbook(path)
    result: list[tuple[str, list[tuple[str, str]]]] = []
    for sheet_name in workbook.sheet_names():
        sheet = workbook.sheet_by_name(sheet_name)
        rows: list[tuple[str, str]] = []
        for r in range(sheet.nrows):
            alimento = _normalize_cell(sheet.cell_value(r, 0) if sheet.ncols > 0 else "")
            valor = _normalize_cell(sheet.cell_value(r, 9) if sheet.ncols > 9 else "")
            rows.append((alimento, valor))
        result.append((sheet_name, rows))
    return result


def _read_all_sheets_xlsx(path: Path) -> list[tuple[str, list[tuple[str, str]]]]:
    import openpyxl

    workbook = openpyxl.load_workbook(path, data_only=True, read_only=True)
    result: list[tuple[str, list[tuple[str, str]]]] = []
    for sheet_name in workbook.sheetnames:
        sheet = workbook[sheet_name]
        rows: list[tuple[str, str]] = []
        for row in sheet.iter_rows(min_col=1, max_col=10, values_only=True):
            alimento = _normalize_cell(row[0] if row else "")
            valor = _normalize_cell(row[9] if row and len(row) > 9 else "")
            rows.append((alimento, valor))
        result.append((sheet_name, rows))
    workbook.close()
    return result


def _select_price_rows(
    sheets: list[tuple[str, list[tuple[str, str]]]],
) -> list[tuple[str, str]]:
    if not sheets:
        raise ValueError("El archivo no tiene hojas")

    # 1) Preferir Anexo 1 / hoja "1" si parece tabla de precios.
    preferred_name = _pick_sheet_name([name for name, _ in sheets])
    for name, rows in sheets:
        if name == preferred_name and _sheet_looks_like_price_table(rows):
            return rows

    # 2) Cualquier hoja que parezca tabla de precios (no la de variaciones).
    for name, rows in sheets:
        if name.strip().lower() in {"indice", "índice", "index"}:
            continue
        if _sheet_looks_like_price_table(rows):
            return rows

    # 3) Fallback a la hoja preferida.
    for name, rows in sheets:
        if name == preferred_name:
            return rows
    return sheets[0][1]


def extract_food_rows(path: Path, period: Period | None = None) -> list[FoodRow]:
    """Extrae alimento (col A) y valor (col J) de la tabla de precios mensuales."""
    if period is None:
        period = parse_period_from_filename(path.name)

    suffix = path.suffix.lower()
    readers: dict[str, Callable[[Path], list[tuple[str, list[tuple[str, str]]]]]] = {
        ".xls": _read_all_sheets_xls,
        ".xlsx": _read_all_sheets_xlsx,
    }
    if suffix not in readers:
        raise ValueError(f"Extensión no soportada: {path}")

    raw_rows = _select_price_rows(readers[suffix](path))

    records: list[FoodRow] = []
    for alimento, valor in raw_rows:
        if not _is_food_name(alimento):
            continue
        if not _is_usable_value(valor):
            continue
        records.append(
            FoodRow(
                year=period.year_str,
                month=period.month_str,
                alimento=alimento,
                valor=valor,
            )
        )
    return records


def write_csv(rows: Iterable[FoodRow], output: Path) -> int:
    output.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["anio", "mes", "alimento", "valor"])
        for row in rows:
            writer.writerow([row.year, row.month, row.alimento, row.valor])
            count += 1
    return count


def import_sipsa(
    start: Period,
    end: Period,
    *,
    download_dir: Path,
    output: Path,
) -> dict[str, object]:
    """Descarga el rango de anexos y genera un CSV consolidado."""
    all_rows: list[FoodRow] = []
    downloaded: list[str] = []
    missing: list[str] = []

    for period in iter_periods(start, end):
        label = f"{period.year}-{period.month_str}"
        try:
            path = resolve_and_download(period, download_dir)
        except FileNotFoundError:
            missing.append(label)
            continue
        downloaded.append(path.name)
        all_rows.extend(extract_food_rows(path, period))

    written = write_csv(all_rows, output)
    return {
        "files": downloaded,
        "missing": missing,
        "rows": written,
        "output": str(output),
    }
