"""DAG de Apache Airflow para descargar y compilar la información de El Niño/La Niña (ONI NOAA) por año."""

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

from pyimport.connections import register_connections_in_airflow
from pyimport.oni import (
    NOAA_ONI_URL,
    download_oni_html,
    process_and_generate_oni_csv,
)


@dag(
    dag_id="oni_fenomeno_nino_import",
    description="Descarga datos de El Niño/La Niña (NOAA ONI v5) usando la conexión noaa_oni y compila un CSV con el promedio por año",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyimport", "oni", "noaa", "nino", "nina", "connection:noaa_oni"],
)
def oni_valle_pipeline():

    @task(task_id="preparar_entorno")
    def preparar_entorno(**context: object) -> dict[str, str]:
        """1. Prepara las carpetas de trabajo, asegura conexiones en Airflow UI y define rutas."""
        register_connections_in_airflow()
        params = context.get("params") or {}
        base_dir = PROJECT_ROOT / "data"
        raw_dir = base_dir / "raw"
        output_file = params.get("output") or str(base_dir / "oni_promedio_anual.csv")

        raw_dir.mkdir(parents=True, exist_ok=True)
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        return {
            "html_file": str(raw_dir / "oni_v5.html"),
            "output_file": str(output_file),
        }

    @task(task_id="descargar_html_oni")
    def descargar_oni(config: dict[str, str]) -> str:
        """2. Descarga la página HTML con la tabla del índice ONI de la NOAA."""
        dest = Path(config["html_file"])
        download_oni_html(NOAA_ONI_URL, dest)
        return str(dest)

    @task(task_id="procesar_y_calcular_promedios")
    def calcular_promedios(html_file: str, config: dict[str, str]) -> dict[str, object]:
        """3 y 4. Parsea el HTML, calcula el promedio anual de anomalía de temperatura y genera el CSV."""
        result = process_and_generate_oni_csv(
            html_path=Path(html_file),
            output_path=Path(config["output_file"]),
        )
        print("✅ Procesamiento completado.")
        print(f"   - Años procesados: {result['years_processed']}")
        print(f"📄 CSV generado en: {result['output']}")
        return result

    # Definición de dependencias y flujo de tareas
    config = preparar_entorno()
    html_file = descargar_oni(config)
    calcular_promedios(html_file, config)


dag = oni_valle_pipeline()
