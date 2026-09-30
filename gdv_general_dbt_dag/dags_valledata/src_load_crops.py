"""ETAPA LOAD CULTIVOS (src_load_crops.py)
Lee los archivos de cultivos descargados en el bucket GCS / directorio raw
y los carga en el dataset y tabla de BigQuery Bronze (bronze_agricola -> bronze_cultivos_valle).
"""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path
import re
import sys
from typing import Any, Dict, Iterable

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    clean_str,
    normalize_municipio,
    normalize_cultivo,
    get_bigquery_client,
    get_composer_params,
    get_connection_id,
    get_raw_root,
    load_config,
    materialize_local,
    require_config_value,
    storage_exists,
    storage_join,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)

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
    return (
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


def _row_get(row: dict[str, str], *candidates: str) -> str:
    """Obtiene un valor por nombre de columna (tolerante a mayúsculas/tildes y sin asteriscos)."""
    normalized = {_norm_header(k): re.sub(r"[*]+", "", v or "").strip() for k, v in row.items()}
    for candidate in candidates:
        value = normalized.get(_norm_header(candidate))
        if value is not None and value != "":
            return value
        # permitir match exacto vacío vs ausente
        if _norm_header(candidate) in normalized:
            return normalized[_norm_header(candidate)]
    return ""


def parse_cultivos_permanentes(path: Path) -> list[CultivoRecord]:
    """Lee permanentes por cabecera. tipo_cultivo queda 'Permanente'; cultivo = nombre (col Cultivo)."""
    records: list[CultivoRecord] = []
    with path.open("r", encoding="latin-1", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            if not row:
                continue
            cultivo = _row_get(row, "Cultivo")
            if not cultivo:
                continue
            records.append(
                CultivoRecord(
                    tipo_cultivo="Permanente",
                    anio=_row_get(row, "Año", "Anio"),
                    id_municipio=_row_get(row, "Id_municipio"),
                    municipio=_row_get(row, "Municipio"),
                    id_cultivo=_row_get(row, "Id_cultivo"),
                    cultivo=cultivo,
                    ciclo=_row_get(row, "Ciclo"),
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
    """Lee transitorios por cabecera. tipo_cultivo='Transitorio'; cultivo = nombre (col Cultivo)."""
    records: list[CultivoRecord] = []
    with path.open("r", encoding="latin-1", errors="replace") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            if not row:
                continue
            cultivo = _row_get(row, "Cultivo")
            if not cultivo:
                continue
            records.append(
                CultivoRecord(
                    tipo_cultivo="Transitorio",
                    anio=_row_get(row, "Año", "Anio"),
                    id_municipio=_row_get(row, "Id_municipio"),
                    municipio=_row_get(row, "Municipio"),
                    id_cultivo=_row_get(row, "Id_cultivo"),
                    cultivo=cultivo,
                    ciclo=_row_get(row, "Ciclo"),
                    hectareas_sembradas=_row_get(row, "Hectareas_sembradas"),
                    hectareas_cosechadas=_row_get(row, "Hectareas_cosechadas"),
                    produccion_toneladas=_row_get(row, "Produccion_toneladas"),
                    rendimiento_toneladas_ha=_row_get(
                        row, "Rendimiento_toneladas/hectareas", "Rendimiento_toneladas_ha"
                    ),
                )
            )
    return records


def write_consolidated_cultivos_csv(records: Iterable[CultivoRecord], output_path: Path) -> int:
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


def run_load_crops(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    paths_cfg = require_config_value(cfg, "paths")
    bq_cfg = require_config_value(cfg, "bigquery")
    google_cloud_default = get_connection_id(cfg, "google_cloud_default")
    composer = get_composer_params(cfg)

    raw_root = get_raw_root(cfg)
    perm_uri = storage_join(raw_root, require_config_value(cfg, "cultivos", "permanentes_filename"))
    trans_uri = storage_join(raw_root, require_config_value(cfg, "cultivos", "transitorios_filename"))
    output_file = Path(require_config_value(paths_cfg, "cultivos_csv"))

    if not storage_exists(perm_uri, cfg=cfg) or not storage_exists(trans_uri, cfg=cfg):
        raise FileNotFoundError("Debe ejecutar primero src_ingest_crops para descargar los CSVs de cultivos.")

    perm_file = materialize_local(perm_uri, cfg=cfg)
    trans_file = materialize_local(trans_uri, cfg=cfg)

    project_id = require_config_value(bq_cfg, "project_id")
    dataset_bronze = require_config_value(bq_cfg, "datasets", "bronze")
    table_bronze = require_config_value(bq_cfg, "tables", "cultivos_bronze")
    gcp_conn_id = google_cloud_default
    composer_env = composer["environment"]
    composer_loc = composer["location"]

    print(
        f"📦 [SRC_LOAD_CROPS] Cargando cultivos a BigQuery Bronze -> {project_id}.{dataset_bronze}.{table_bronze} | "
        f"Composer={composer_env} ({composer_loc}) | conn={gcp_conn_id}"
    )

    res = import_and_consolidate_cultivos(
        permanentes_path=perm_file,
        transitorios_path=trans_file,
        output_path=output_file,
    )

    try:
        from google.cloud import bigquery
        client = get_bigquery_client(cfg, project_id, require_config_value(bq_cfg, "location"))
        table_ref = f"{project_id}.{dataset_bronze}.{table_bronze}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(output_file, "rb") as sf:
            job = client.load_table_from_file(sf, table_ref, job_config=job_config)
        job.result()
        res["bigquery_table"] = table_ref
        res["status"] = "SUCCESS"
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación Load BQ] {exc}")
        res["bigquery_table"] = f"{project_id}.{dataset_bronze}.{table_bronze}"
        res["status"] = "SIMULATED"

    print(f"✅ [SRC_LOAD_CROPS] Carga Bronze completada. Total filas: {res['total_rows']}")
    return res


if __name__ == "__main__":
    run_load_crops()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    from src_common import get_airflow_dag_kwargs, run_with_airflow_alarm

    @dag(
        dag_id="src_load_crops",
        description="Etapa Load Cultivos Valle (Carga BigQuery Bronze)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "cultivos", "load", "bronze"],
        **get_airflow_dag_kwargs(),
    )
    def load_crops_dag():
        @task(task_id="run_load_crops")
        def execute_load() -> dict[str, object]:
            return run_with_airflow_alarm(run_load_crops)

        execute_load()

    dag_single = load_crops_dag()
except ImportError:
    pass
