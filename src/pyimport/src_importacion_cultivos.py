"""ETAPA IMPORTACIÓN CULTIVOS Y ORQUESTACIÓN GOLD (src_importacion_cultivos.py)

DAG Orquestador Anual para la Importación de Datos de Cultivos y la ejecución
secuencial de los DAGs dependientes hasta alcanzar la Capa Gold.

Frecuencia: Anual (`@yearly` / `0 0 1 1 *`).

Orden Estricto de Ejecución:
  1. Importación Datos de Cultivos: Ingesta (`src_ingest_crops`), Carga Bronze (`src_load_crops`) y Transformación Silver (`src_transform_crops`).
  2. Índice de Precios (SIPSA): Ingesta (`src_ingest_sipsa`) y Carga Bronze (`src_load_sipsa`).
  3. ONI (Clima El Niño/La Niña): Ingesta (`src_ingest_oni`) y Carga Bronze (`src_load_oni`).
  4. Distribución Geográfica (Municipios): Ingesta (`src_ingest_municipios`) y Carga Bronze (`src_load_municipios`).
  5. Consolidado Silver: Transformación Silver unificada (`src_transform_consolidado`).
  6. Capa Gold Espacial: Transformación GIS Espacial Gold (`src_transform_spatial`).
"""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Dict

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    get_airflow_dag_kwargs,
    get_composer_params,
    load_config,
    run_with_airflow_alarm,
)


def run_importacion_cultivos(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Ejecuta secuencialmente la importación de cultivos y el pipeline Medallón hasta la Capa Gold."""
    from src_ingest_crops import run_ingest_crops
    from src_load_crops import run_load_crops
    from src_transform_crops import run_transform_crops
    from src_ingest_sipsa import run_ingest_sipsa
    from src_load_sipsa import run_load_sipsa
    from src_ingest_oni import run_ingest_oni
    from src_load_oni import run_load_oni
    from src_ingest_municipios import run_ingest_municipios
    from src_load_municipios import run_load_municipios
    from src_transform_consolidado import consolidar_bronze_a_silver
    from src_transform_spatial import run_transform_spatial

    cfg = config or load_config()
    composer = get_composer_params(cfg)
    print(f"🚀 [SRC_IMPORTACION_CULTIVOS] Iniciando Orquestador Anual Cultivos -> Gold (Composer={composer.get('environment', '-')})")

    # 1. Datos de Cultivos (Ingest -> Load Bronze -> Transform Silver)
    print("1️⃣ [PASO 1/6 - CULTIVOS] Ingesta, Carga Bronze y Transformación Silver de Cultivos...")
    res_ingest_crops = run_ingest_crops(cfg)
    res_load_crops = run_load_crops(cfg)
    res_trans_crops = run_transform_crops(cfg)

    # 2. Índice de Precios (SIPSA DANE)
    print("2️⃣ [PASO 2/6 - ÍNDICE DE PRECIOS] Ingesta y Carga Bronze de SIPSA...")
    res_ingest_sipsa = run_ingest_sipsa(cfg)
    res_load_sipsa = run_load_sipsa(cfg)

    # 3. ONI (Clima El Niño / La Niña)
    print("3️⃣ [PASO 3/6 - ONI] Ingesta y Carga Bronze de ONI Clima...")
    res_ingest_oni = run_ingest_oni(cfg)
    res_load_oni = run_load_oni(cfg)

    # 4. Distribución Geográfica (Municipios Valle)
    print("4️⃣ [PASO 4/6 - DISTRIBUCIÓN GEOGRÁFICA] Ingesta y Carga Bronze de Municipios...")
    res_ingest_mun = run_ingest_municipios(cfg)
    res_load_mun = run_load_municipios(cfg)

    # 5. Consolidación Silver
    print("5️⃣ [PASO 5/6 - CONSOLIDADO SILVER] Generación de Silver Consolidado...")
    res_consolidado = consolidar_bronze_a_silver(cfg)

    # 6. Capa Gold (GIS Espacial)
    print("6️⃣ [PASO 6/6 - CAPA GOLD] Generación de Gold GIS Espacial...")
    res_spatial = run_transform_spatial(cfg)

    print("🎉 [SRC_IMPORTACION_CULTIVOS] Pipeline Orquestador Anual finalizado exitosamente hasta Capa Gold.")
    return {
        "status": "SUCCESS",
        "cultivos": {
            "ingest": res_ingest_crops,
            "load": res_load_crops,
            "transform": res_trans_crops,
        },
        "sipsa_precios": {
            "ingest": res_ingest_sipsa,
            "load": res_load_sipsa,
        },
        "oni_clima": {
            "ingest": res_ingest_oni,
            "load": res_load_oni,
        },
        "distribucion_geografica": {
            "ingest": res_ingest_mun,
            "load": res_load_mun,
        },
        "consolidado_silver": res_consolidado,
        "capa_gold": res_spatial,
    }


if __name__ == "__main__":
    run_importacion_cultivos()

# Definición del DAG de Airflow (Compatibilidad Composer 2 y 3)
try:
    try:
        from airflow.sdk import dag as _af_dag, task as _af_task
    except ImportError:
        from airflow.decorators import dag as _af_dag, task as _af_task
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.operators.empty import EmptyOperator
except ImportError:
    _af_dag = _af_task = None

if _af_dag is not None:
    from datetime import datetime

    @_af_dag(
        dag_id="src_importacion_cultivos",
        description="Importación anual de cultivos y orquestación secuencial (Cultivos -> Precios -> ONI -> Geografía -> Gold)",
        start_date=datetime(2000, 1, 1),
        schedule="@yearly",
        catchup=False,
        tags=["valledata", "cultivos", "sipsa", "oni", "municipios", "gold", "anual"],
        **get_airflow_dag_kwargs(),
    )
    def importacion_cultivos_dag():
        start_pipeline = EmptyOperator(task_id="start_importacion_cultivos")
        end_pipeline = EmptyOperator(task_id="end_importacion_cultivos")

        # 1. Datos de Cultivos (Ingest -> Load -> Transform)
        trig_ingest_crops = TriggerDagRunOperator(
            task_id="trigger_ingest_crops",
            trigger_dag_id="src_ingest_crops",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_load_crops = TriggerDagRunOperator(
            task_id="trigger_load_crops",
            trigger_dag_id="src_load_crops",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_transform_crops = TriggerDagRunOperator(
            task_id="trigger_transform_crops",
            trigger_dag_id="src_transform_crops",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 2. Índice de Precios (SIPSA)
        trig_ingest_sipsa = TriggerDagRunOperator(
            task_id="trigger_ingest_sipsa",
            trigger_dag_id="src_ingest_sipsa",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_load_sipsa = TriggerDagRunOperator(
            task_id="trigger_load_sipsa",
            trigger_dag_id="src_load_sipsa",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 3. ONI (Índice Climatológico El Niño / La Niña)
        trig_ingest_oni = TriggerDagRunOperator(
            task_id="trigger_ingest_oni",
            trigger_dag_id="src_ingest_oni",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_load_oni = TriggerDagRunOperator(
            task_id="trigger_load_oni",
            trigger_dag_id="src_load_oni",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 4. Distribución Geográfica (Municipios)
        trig_ingest_mun = TriggerDagRunOperator(
            task_id="trigger_ingest_municipios",
            trigger_dag_id="src_ingest_municipios",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_load_mun = TriggerDagRunOperator(
            task_id="trigger_load_municipios",
            trigger_dag_id="src_load_municipios",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 5. Consolidado Silver
        trig_consolidado = TriggerDagRunOperator(
            task_id="trigger_transform_consolidado",
            trigger_dag_id="src_transform_consolidado",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 6. Capa Gold Espacial
        trig_gold_spatial = TriggerDagRunOperator(
            task_id="trigger_transform_spatial",
            trigger_dag_id="src_transform_spatial",
            wait_for_completion=True,
            poke_interval=10,
        )

        # Flujo de ejecución secuencial ordenado:
        # Cultivos -> Indice de Precios -> ONI -> Distribución Geográfica -> Consolidado Silver -> Capa Gold
        (
            start_pipeline
            >> trig_ingest_crops >> trig_load_crops >> trig_transform_crops
            >> trig_ingest_sipsa >> trig_load_sipsa
            >> trig_ingest_oni >> trig_load_oni
            >> trig_ingest_mun >> trig_load_mun
            >> trig_consolidado
            >> trig_gold_spatial
            >> end_pipeline
        )

    dag = importacion_cultivos_dag()
    DAG = dag
