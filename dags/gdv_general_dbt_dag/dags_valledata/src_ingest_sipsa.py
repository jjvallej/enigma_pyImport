"""ETAPA INGEST SIPSA (src_ingest_sipsa.py)
Descarga los anexos mensuales Excel de precios SIPSA desde el portal DANE
hacia el bucket de GCS / almacenamiento local raw parametrizado en config.yaml.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Dict, Iterator, Sequence
import urllib.error
import urllib.request

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    get_composer_params,
    get_connection_base_url,
    get_connection_id,
    get_raw_root,
    load_config,
    require_config_value,
    storage_exists,
    storage_join,
    write_bytes,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)

MONTH_ABBR = {
    "ene": "01", "feb": "02", "mar": "03", "abr": "04", "may": "05", "jun": "06",
    "jul": "07", "ago": "08", "sep": "09", "oct": "10", "nov": "11", "dic": "12",
}
MONTH_NUM_TO_ABBR = {v: k for k, v in MONTH_ABBR.items()}

# Timeouts cortos: DANE suele colgar en URLs inexistentes.
PROBE_TIMEOUT = 8.0
DOWNLOAD_TIMEOUT = 60.0


@dataclass(frozen=True)
class Period:
    year: int
    month: int

    @property
    def month_abbr(self) -> str:
        return MONTH_NUM_TO_ABBR[f"{self.month:02d}"]

    @property
    def month_str(self) -> str:
        return f"{self.month:02d}"


def parse_period_arg(value: str) -> Period:
    parts = value.strip().split("-")
    if len(parts) != 2:
        raise ValueError(f"Periodo inválido '{value}'. Use YYYY-MM")
    year, month = int(parts[0]), int(parts[1])
    if month < 1 or month > 12:
        raise ValueError(f"Mes inválido en '{value}'")
    return Period(year=year, month=month)


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


def candidate_urls(
    period: Period,
    base_url: str,
    path_templates: Sequence[str],
) -> list[str]:
    return [
        f"{base_url.rstrip('/')}{tmpl.format(mes=period.month_abbr, anio=period.year)}"
        for tmpl in path_templates
    ]


def _request(url: str, method: str = "GET", timeout: float = 30.0):
    req = urllib.request.Request(url, method=method, headers={"User-Agent": "pyimport/0.1"})
    return urllib.request.urlopen(req, timeout=timeout)


def _looks_like_excel(data: bytes, url: str) -> bool:
    """Filtra soft-404 HTML del DANE que responden 200."""
    if not data or len(data) < 8:
        return False
    lower = url.lower()
    if lower.endswith(".xls") and data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return True
    if lower.endswith(".xlsx") and data[:2] == b"PK":
        return True
    # Algunos anexos .xls reales vienen como xlsx renombrado
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" or data[:2] == b"PK":
        return True
    return False


def fetch_excel_bytes(url: str, timeout: float = PROBE_TIMEOUT) -> bytes | None:
    """Un solo GET. Devuelve bytes si es Excel válido; None si no existe / timeout / HTML."""
    try:
        with _request(url, method="GET", timeout=timeout) as response:
            if not (200 <= response.status < 300):
                return None
            data = response.read()
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError):
        return None
    except Exception:
        return None
    if not _looks_like_excel(data, url):
        return None
    return data


def resolve_and_download(
    period: Period,
    download_root: str,
    base_url: str,
    path_templates: Sequence[str],
    cfg: Dict[str, Any] | None = None,
    preferred_template_idx: int = 0,
) -> tuple[str, int]:
    """Descarga el anexo del periodo. Retorna (uri, índice de template usado)."""
    urls = candidate_urls(period, base_url=base_url, path_templates=path_templates)
    if not urls:
        raise FileNotFoundError(f"No se encontró anexo SIPSA para {period.year}-{period.month_str}")

    # Prioriza el template que funcionó en el periodo anterior.
    order = list(range(len(urls)))
    if 0 <= preferred_template_idx < len(urls):
        order = [preferred_template_idx] + [i for i in order if i != preferred_template_idx]
    ordered_urls = [(idx, urls[idx]) for idx in order]

    # Prueba en paralelo: un mes ausente cuesta ~PROBE_TIMEOUT, no N*timeout.
    with ThreadPoolExecutor(max_workers=min(4, len(ordered_urls))) as pool:
        futures = {
            pool.submit(fetch_excel_bytes, url, PROBE_TIMEOUT): (idx, url)
            for idx, url in ordered_urls
        }
        for fut in as_completed(futures):
            idx, url = futures[fut]
            data = fut.result()
            if data is None:
                continue
            filename = Path(url).name
            target = storage_join(download_root, filename)
            # Re-descarga con timeout largo solo si el probe trajo el archivo completo
            # (ya lo tenemos en memoria).
            write_bytes(target, data, cfg=cfg)
            return target, idx

    raise FileNotFoundError(f"No se encontró anexo SIPSA para {period.year}-{period.month_str}")


def download_attachments(
    start_period: str,
    end_period: str,
    download_root: str,
    base_url: str,
    path_templates: Sequence[str],
    cfg: Dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    start = parse_period_arg(start_period)
    end = parse_period_arg(end_period)
    periods = list(iter_periods(start, end))
    total = len(periods)

    downloaded: list[str] = []
    skipped: list[str] = []
    missing: list[str] = []
    preferred_idx = 0

    print(
        f"⏳ [SRC_INGEST_SIPSA] {total} periodos a revisar "
        f"({start_period} → {end_period}), timeout={PROBE_TIMEOUT}s por URL",
        flush=True,
    )

    for i, period in enumerate(periods, start=1):
        label = f"{period.year}-{period.month_str}"
        # Si ya hay algún candidato en storage, no re-descargar.
        already = None
        for url in candidate_urls(period, base_url=base_url, path_templates=path_templates):
            candidate = storage_join(download_root, Path(url).name)
            if storage_exists(candidate, cfg=cfg):
                already = candidate
                break
        if already:
            skipped.append(already)
            if i == 1 or i == total or i % 12 == 0:
                print(f"⏭️  [{i}/{total}] {label}: ya existe → {already}", flush=True)
            continue

        try:
            path, preferred_idx = resolve_and_download(
                period,
                download_root,
                base_url=base_url,
                path_templates=path_templates,
                cfg=cfg,
                preferred_template_idx=preferred_idx,
            )
            downloaded.append(path)
            print(f"✅ [{i}/{total}] {label}: descargado → {path}", flush=True)
        except FileNotFoundError:
            missing.append(label)
            if i == 1 or i == total or i % 12 == 0:
                print(f"⚠️  [{i}/{total}] {label}: sin anexo en DANE", flush=True)

    print(
        f"📊 [SRC_INGEST_SIPSA] descargados={len(downloaded)} "
        f"omitidos={len(skipped)} ausentes={len(missing)}",
        flush=True,
    )
    return {"downloaded": downloaded, "skipped": skipped, "missing": missing}


def run_ingest_sipsa(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    sipsa_cfg = require_config_value(cfg, "sipsa")
    sipsa_dane = get_connection_id(cfg, "sipsa_dane")
    composer = get_composer_params(cfg)
    gcs_bucket = require_config_value(cfg, "gcs_bucket")

    start = require_config_value(sipsa_cfg, "start_period")
    end = require_config_value(sipsa_cfg, "end_period")
    path_templates = require_config_value(sipsa_cfg, "path_templates")
    config_base = require_config_value(sipsa_cfg, "base_url")
    raw_root = get_raw_root(cfg)

    base_url = get_connection_base_url(sipsa_dane, default_host=config_base) or config_base

    print(
        f"🌐 [SRC_INGEST_SIPSA] Target: {gcs_bucket} | raw_root={raw_root} | Base URL: {base_url} | "
        f"Composer={composer['environment']} ({composer['location']}) | conn={sipsa_dane}",
        flush=True,
    )
    print(f"📥 Descargando anexos SIPSA ({start} a {end}) en {raw_root}...", flush=True)
    res = download_attachments(
        start_period=start,
        end_period=end,
        download_root=raw_root,
        base_url=base_url,
        path_templates=path_templates,
        cfg=cfg,
    )
    print(
        f"✅ [SRC_INGEST_SIPSA] Completada: {len(res.get('downloaded', []))} archivos nuevos, "
        f"{len(res.get('skipped', []))} ya existentes",
        flush=True,
    )
    return res


if __name__ == "__main__":
    run_ingest_sipsa()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    @dag(
        dag_id="src_ingest_sipsa",
        description="Etapa Ingest Precios SIPSA (Descarga Anexos DANE)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "sipsa", "ingest", "connection:sipsa_dane"],
        **get_airflow_dag_kwargs(),
    )
    def ingest_sipsa_dag():
        @task(task_id="run_ingest_sipsa")
        def execute_ingest() -> dict[str, object]:
            return run_with_airflow_alarm(run_ingest_sipsa)

        execute_ingest()

    dag = ingest_sipsa_dag()
except ImportError:
    pass
