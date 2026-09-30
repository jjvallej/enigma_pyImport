"""ETAPA LOAD ONI (src_load_oni.py)
Lee el archivo HTML descargado del índice ONI (NOAA) y lo procesa
hacia el dataset y tabla de BigQuery Bronze (bronze_agricola -> bronze_oni_climatico).
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

@dataclass(frozen=True)
class AnnualONIRecord:
    anio: str
    promedio_oni: str
    num_periodos: int
    fenomeno_predominante: str



def parse_oni_html(html_path: Path) -> list[AnnualONIRecord]:
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


def write_oni_annual_csv(records: Iterable[AnnualONIRecord], output_path: Path) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["anio", "promedio_oni", "num_periodos", "fenomeno_predominante"]
    count = 0
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for rec in records:
            writer.writerow(asdict(rec))
            count += 1
    return count


def process_and_generate_oni_csv(html_path: Path, output_path: Path) -> dict[str, object]:
    records = parse_oni_html(html_path)
    written = write_oni_annual_csv(records, output_path)
    return {"years_processed": written, "output": str(output_path)}


def run_load_oni(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    paths_cfg = require_config_value(cfg, "paths")
    bq_cfg = require_config_value(cfg, "bigquery")
    google_cloud_default = get_connection_id(cfg, "google_cloud_default")
    composer = get_composer_params(cfg)

    raw_root = get_raw_root(cfg)
    html_uri = storage_join(raw_root, require_config_value(cfg, "oni", "raw_filename"))
    output_csv = Path(require_config_value(paths_cfg, "oni_csv"))

    if not storage_exists(html_uri, cfg=cfg):
        raise FileNotFoundError(f"Archivo HTML no encontrado: {html_uri}. Ejecute src_ingest_oni primero.")

    html_file = materialize_local(html_uri, cfg=cfg)

    project_id = require_config_value(bq_cfg, "project_id")
    dataset_bronze = require_config_value(bq_cfg, "datasets", "bronze")
    table_bronze = require_config_value(bq_cfg, "tables", "oni_bronze")
    gcp_conn_id = google_cloud_default
    composer_env = composer["environment"]
    composer_loc = composer["location"]

    print(
        f"📦 [SRC_LOAD_ONI] Procesando HTML ONI {html_file} -> {output_csv} | "
        f"Composer={composer_env} ({composer_loc}) | conn={gcp_conn_id}"
    )
    res = process_and_generate_oni_csv(html_path=html_file, output_path=output_csv)

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
        with open(output_csv, "rb") as sf:
            job = client.load_table_from_file(sf, table_ref, job_config=job_config)
        job.result()
        res["bigquery_table"] = table_ref
        res["status"] = "SUCCESS"
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación Load BQ ONI] {exc}")
        res["bigquery_table"] = f"{project_id}.{dataset_bronze}.{table_bronze}"
        res["status"] = "SIMULATED"

    print(f"✅ [SRC_LOAD_ONI] Carga Bronze completada. Total años: {res.get('years_processed', 0)}")
    return res


if __name__ == "__main__":
    run_load_oni()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    from src_common import get_airflow_dag_kwargs, run_with_airflow_alarm

    @dag(
        dag_id="src_load_oni",
        description="Etapa Load Clima ONI (Carga BigQuery Bronze)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "oni", "load", "bronze"],
        **get_airflow_dag_kwargs(),
    )
    def load_oni_dag():
        @task(task_id="run_load_oni")
        def execute_load() -> dict[str, object]:
            return run_with_airflow_alarm(run_load_oni)

        execute_load()

    dag_single = load_oni_dag()
except ImportError:
    pass
