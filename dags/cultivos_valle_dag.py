"""DAG de Apache Airflow para la descarga y consolidación de cultivos agrícolas del Valle del Cauca."""

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
from pyimport.cultivos import (
    CULTIVOS_PERMANENTES_URL,
    CULTIVOS_TRANSITORIOS_URL,
    download_cultivo_dataset,
    import_and_consolidate_cultivos,
)


@dag(
    dag_id="cultivos_valle_import",
    description="Descarga y consolida cultivos permanentes y transitorios del Valle del Cauca mediante la conexión gobernacion_valle",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyimport", "cultivos", "valle", "connection:gobernacion_valle"],
)
def cultivos_valle_pipeline():

    @task(task_id="preparar_entorno")
    def preparar_entorno(**context: object) -> dict[str, str]:
        """1. Prepara las carpetas de trabajo, asegura conexiones en Airflow UI y define rutas."""
        register_connections_in_airflow()
        params = context.get("params") or {}
        base_dir = PROJECT_ROOT / "data"
        raw_dir = base_dir / "raw"
        output_file = params.get("output") or str(base_dir / "cultivos_valle.csv")

        raw_dir.mkdir(parents=True, exist_ok=True)
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        return {
            "permanentes_file": str(raw_dir / "cultivos_permanentes.csv"),
            "transitorios_file": str(raw_dir / "cultivos_transitorios.csv"),
            "output_file": str(output_file),
        }

    @task(task_id="descargar_cultivos_permanentes")
    def descargar_permanentes(config: dict[str, str]) -> str:
        """2. Descarga el CSV de cultivos permanentes del Valle del Cauca."""
        dest = Path(config["permanentes_file"])
        download_cultivo_dataset(CULTIVOS_PERMANENTES_URL, dest)
        return str(dest)

    @task(task_id="descargar_cultivos_transitorios")
    def descargar_transitorios(config: dict[str, str]) -> str:
        """3. Descarga el CSV de cultivos transitorios del Valle del Cauca."""
        dest = Path(config["transitorios_file"])
        download_cultivo_dataset(CULTIVOS_TRANSITORIOS_URL, dest)
        return str(dest)

    @task(task_id="procesar_y_consolidar_cultivos")
    def consolidar_cultivos(
        perm_path: str, trans_path: str, config: dict[str, str]
    ) -> dict[str, object]:
        """4. Lee, normaliza y consolida ambos datasets en cultivos_valle.csv."""
        result = import_and_consolidate_cultivos(
            permanentes_path=Path(perm_path),
            transitorios_path=Path(trans_path),
            output_path=Path(config["output_file"]),
        )
        print(f"✅ Consolidación completada.")
        print(f"   - Filas permanentes: {result['permanentes_rows']}")
        print(f"   - Filas transitorios: {result['transitorios_rows']}")
        print(f"   - Total filas escritas: {result['total_rows']}")
        print(f"📄 CSV consolidado en: {result['output']}")
        return result

    # Definición de dependencias y flujo de tareas
    config = preparar_entorno()
    perm_file = descargar_permanentes(config)
    trans_file = descargar_transitorios(config)
    consolidar_cultivos(perm_file, trans_file, config)


dag = cultivos_valle_pipeline()
