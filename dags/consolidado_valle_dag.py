"""DAG de Apache Airflow para la consolidación maestra de los 3 conjuntos de datos (Cultivos, Precios SIPSA y Fenómeno El Niño)."""

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

from pyimport.consolidate import (
    build_sipsa_annual_prices,
    consolidate_all_datasets,
    load_oni_data,
)


@dag(
    dag_id="dataset_consolidado_master_import",
    description="Consolida los datos de Cultivos del Valle, Precios SIPSA (Cali) y Fenómeno El Niño (NOAA ONI)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pyimport", "consolidado", "valle", "sipsa", "oni"],
)
def consolidado_master_pipeline():

    @task(task_id="preparar_entorno")
    def preparar_entorno(**context: object) -> dict[str, str]:
        """1. Define las rutas de entrada de los 3 archivos y la ruta del CSV consolidado maestro de salida."""
        params = context.get("params") or {}
        base_dir = PROJECT_ROOT / "data"

        cultivos_file = params.get("cultivos_file") or str(base_dir / "cultivos_valle.csv")
        sipsa_file = params.get("sipsa_file") or str(base_dir / "sipsa_precios.csv")
        oni_file = params.get("oni_file") or str(base_dir / "oni_promedio_anual.csv")
        output_file = params.get("output") or str(base_dir / "dataset_consolidado_valle.csv")

        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        return {
            "cultivos_file": cultivos_file,
            "sipsa_file": sipsa_file,
            "oni_file": oni_file,
            "output_file": output_file,
        }

    @task(task_id="cargar_y_mapear_precios_sipsa")
    def cargar_precios(config: dict[str, str]) -> int:
        """2. Carga y precalcula los promedios anuales de precios en SIPSA."""
        prices = build_sipsa_annual_prices(Path(config["sipsa_file"]))
        print(f"✅ Promedios de precios SIPSA cargados para {len(prices)} combinaciones (año, producto).")
        return len(prices)

    @task(task_id="cargar_y_mapear_clima_oni")
    def cargar_clima(config: dict[str, str]) -> int:
        """3. Carga los registros del fenómeno El Niño por año."""
        oni_data = load_oni_data(Path(config["oni_file"]))
        print(f"✅ Indicadores de clima ONI cargados para {len(oni_data)} años.")
        return len(oni_data)

    @task(task_id="generar_dataset_consolidado_master")
    def generar_master(
        prices_count: int, oni_count: int, config: dict[str, str]
    ) -> dict[str, object]:
        """4. Cruza cultivos_valle con los precios promedio SIPSA y los datos de clima ONI por año."""
        result = consolidate_all_datasets(
            cultivos_csv_path=Path(config["cultivos_file"]),
            sipsa_csv_path=Path(config["sipsa_file"]),
            oni_csv_path=Path(config["oni_file"]),
            output_csv_path=Path(config["output_file"]),
        )
        print("✅ Consolidación maestra completada con éxito.")
        print(f"   - Total de registros principales (Cultivos Valle): {result['total_rows']}")
        print(f"   - Registros vinculados con precio SIPSA: {result['matched_price_rows']}")
        print(f"   - Registros vinculados con indicador ONI: {result['matched_oni_rows']}")
        print(f"📄 Archivo consolidado maestro escrito en: {result['output']}")
        return result

    # Definición de dependencias
    config = preparar_entorno()
    p_count = cargar_precios(config)
    o_count = cargar_clima(config)
    generar_master(p_count, o_count, config)


dag = consolidado_master_pipeline()
