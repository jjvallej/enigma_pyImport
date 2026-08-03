from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

# Ensure src/ and dags/ directories are on sys.path for Airflow DAG loader
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
for p in (str(DAGS_DIR), str(SRC_DIR), str(PROJECT_ROOT)):
    if p not in sys.path:
        sys.path.insert(0, p)

from airflow.decorators import dag, task

from pyimport.airflow_adapter import (
    download_attachments,
    extract_and_process_prices,
    generate_consolidated_csv,
)
from pyimport.connections import register_connections_in_airflow


@dag(
    dag_id="sipsa_import",
    description="Descarga, extrae y consolida anexos mensuales SIPSA (DANE) mediante la conexión sipsa_dane",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyimport", "sipsa", "connection:sipsa_dane"],
    params={
        "start": "2015-02",
        "end": "2026-06",
    },
)
def sipsa_import_pipeline():

    @task(task_id="preparar_entorno")
    def preparar_entorno(**context: object) -> dict[str, str]:
        """1. Resuelve configuración de rutas, asegura conexiones en Airflow UI y prepara carpetas."""
        register_connections_in_airflow()
        params = context.get("params") or {}
        base_dir = PROJECT_ROOT / "data"
        download_dir = params.get("download_dir") or str(base_dir / "raw")
        output = params.get("output") or str(base_dir / "sipsa_precios.csv")

        Path(download_dir).mkdir(parents=True, exist_ok=True)
        Path(output).parent.mkdir(parents=True, exist_ok=True)

        return {
            "start": str(params.get("start", "2015-02")),
            "end": str(params.get("end", "2026-06")),
            "download_dir": download_dir,
            "output": output,
        }

    @task(task_id="descargar_anexos_sipsa")
    def descargar_anexos(config: dict[str, str]) -> dict[str, list[str]]:
        """2. Descarga los anexos mensuales Excel de la página del DANE."""
        return download_attachments(
            start_period=config["start"],
            end_period=config["end"],
            download_dir=config["download_dir"],
        )

    @task(task_id="extraer_y_procesar_precios")
    def extraer_precios(download_info: dict[str, list[str]]) -> list[dict[str, str]]:
        """3. Lee los archivos Excel y extrae las filas de alimentos y precios."""
        files = download_info.get("downloaded", [])
        return extract_and_process_prices(files)

    @task(task_id="generar_csv_consolidado")
    def generar_csv(
        rows_data: list[dict[str, str]], config: dict[str, str]
    ) -> dict[str, object]:
        """4. Consolida y escribe los registros en el archivo CSV final."""
        result = generate_consolidated_csv(rows_data, config["output"])
        print(f"✅ Proceso completado. Filas escritas: {result['rows']}")
        print(f"📄 Archivo CSV generado en: {result['output']}")
        return result

    # Conexión del flujo de tareas en Airflow
    config = preparar_entorno()
    download_info = descargar_anexos(config)
    rows = extraer_precios(download_info)
    generar_csv(rows, config)


dag = sipsa_import_pipeline()

