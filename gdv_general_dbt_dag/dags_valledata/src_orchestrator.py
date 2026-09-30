"""ETAPA MASTER ORCHESTRATOR (src_orchestrator.py)

DAG Orquestador Maestro que coordina la ejecución completa del pipeline de ValleData:
  1. Ingesta (Municipios, Cultivos, SIPSA, ONI Clima)
  2. Carga / Bronze (Municipios, Cultivos, SIPSA, ONI Clima)
  3. Transformación Cultivos / Silver
  4. Consolidación Silver (silver_agri_consolidado)
  5. Transformación GIS Espacial / Gold (gold_cultivos_valle_geo)

Nota: Los DAGs de comentarios CKAN (src_ingest_ckan_comentarios, src_load_ckan_comentarios,
src_transform_ckan_comentarios) se mantienen comentados como plantilla para ser activados
cuando la API de CKAN esté disponible.
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


def run_orchestrator(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Ejecuta secuencialmente las etapas Medallón (Ingest -> Bronze -> Silver -> Gold)."""
    from src_ingest_crops import run_ingest_crops
    from src_ingest_municipios import run_ingest_municipios
    from src_ingest_oni import run_ingest_oni
    from src_ingest_sipsa import run_ingest_sipsa
    from src_load_crops import run_load_crops
    from src_load_municipios import run_load_municipios
    from src_load_oni import run_load_oni
    from src_load_sipsa import run_load_sipsa
    from src_transform_consolidado import consolidar_bronze_a_silver
    from src_transform_crops import run_transform_crops
    from src_transform_spatial import run_transform_spatial

    cfg = config or load_config()
    composer = get_composer_params(cfg)
    print(f"🚀 [SRC_ORCHESTRATOR] Iniciando Orquestador Maestro Medallón en Composer={composer.get('environment', '-')}")

    # 1. Ingesta (Raw)
    print("1️⃣ [PASO 1/4 - INGESTA RAW] Ingesta de fuentes (Municipios, Cultivos, SIPSA, ONI)...")
    res_ingest_mun = run_ingest_municipios(cfg)
    res_ingest_crops = run_ingest_crops(cfg)
    res_ingest_sipsa = run_ingest_sipsa(cfg)
    res_ingest_oni = run_ingest_oni(cfg)

    # 2. Carga / Bronze (Raw -> Bronze)
    print("2️⃣ [PASO 2/4 - CARGA BRONZE] Carga de tablas Bronze a BigQuery...")
    res_load_mun = run_load_municipios(cfg)
    res_load_crops = run_load_crops(cfg)
    res_load_sipsa = run_load_sipsa(cfg)
    res_load_oni = run_load_oni(cfg)

    # 3. Transformación Silver (Bronze -> Silver)
    print("3️⃣ [PASO 3/4 - TRANSFORMACIÓN SILVER] Leyendo Bronze BigQuery -> Consolidando Silver...")
    res_trans_crops = run_transform_crops(cfg)
    res_consolidado = consolidar_bronze_a_silver(cfg)

    # 4. Transformación GIS Espacial / Gold (Silver -> Gold)
    print("4️⃣ [PASO 4/4 - TRANSFORMACIÓN GOLD] Leyendo Silver BigQuery -> Generando Gold GIS...")
    res_spatial = run_transform_spatial(cfg)

    print("🎉 [SRC_ORCHESTRATOR] Pipeline Orquestador Medallón ejecutado exitosamente.")
    return {
        "status": "SUCCESS",
        "ingest": {
            "municipios": res_ingest_mun,
            "crops": res_ingest_crops,
            "sipsa": res_ingest_sipsa,
            "oni": res_ingest_oni,
        },
        "load": {
            "municipios": res_load_mun,
            "crops": res_load_crops,
            "sipsa": res_load_sipsa,
            "oni": res_load_oni,
        },
        "transform": {
            "crops": res_trans_crops,
            "consolidado": res_consolidado,
            "spatial": res_spatial,
        },
    }


if __name__ == "__main__":
    run_orchestrator()

# Definición Airflow DAG (Compatible con Composer 2 y 3)
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
        dag_id="src_orchestrator_master",
        description="Orquestador Maestro Medallón ValleData: coordina Ingesta -> Bronze -> Silver -> Gold",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "master", "orchestrator", "medallion", "pipeline"],
        **get_airflow_dag_kwargs(),
    )
    def master_orchestrator_dag():
        # Inicio y fin del pipeline
        start_pipeline = EmptyOperator(task_id="start_pipeline")
        end_pipeline = EmptyOperator(task_id="end_pipeline")

        # 1. Triggers de Ingesta (Paralelo)
        trig_ingest_mun = TriggerDagRunOperator(
            task_id="trigger_ingest_municipios",
            trigger_dag_id="src_ingest_municipios",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_ingest_crops = TriggerDagRunOperator(
            task_id="trigger_ingest_crops",
            trigger_dag_id="src_ingest_crops",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_ingest_sipsa = TriggerDagRunOperator(
            task_id="trigger_ingest_sipsa",
            trigger_dag_id="src_ingest_sipsa",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_ingest_oni = TriggerDagRunOperator(
            task_id="trigger_ingest_oni",
            trigger_dag_id="src_ingest_oni",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 2. Triggers de Carga / Bronze
        trig_load_mun = TriggerDagRunOperator(
            task_id="trigger_load_municipios",
            trigger_dag_id="src_load_municipios",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_load_crops = TriggerDagRunOperator(
            task_id="trigger_load_crops",
            trigger_dag_id="src_load_crops",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_load_sipsa = TriggerDagRunOperator(
            task_id="trigger_load_sipsa",
            trigger_dag_id="src_load_sipsa",
            wait_for_completion=True,
            poke_interval=10,
        )
        trig_load_oni = TriggerDagRunOperator(
            task_id="trigger_load_oni",
            trigger_dag_id="src_load_oni",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 3. Trigger Transformación Silver (Consolidado Silver)
        trig_trans_crops = TriggerDagRunOperator(
            task_id="trigger_transform_crops",
            trigger_dag_id="src_transform_crops",
            wait_for_completion=True,
            poke_interval=10,
        )

        trig_trans_consolidado = TriggerDagRunOperator(
            task_id="trigger_transform_consolidado",
            trigger_dag_id="src_transform_consolidado",
            wait_for_completion=True,
            poke_interval=10,
        )

        # 4. Trigger GIS Espacial Gold (Lee de Silver Consolidado)
        trig_trans_spatial = TriggerDagRunOperator(
            task_id="trigger_transform_spatial",
            trigger_dag_id="src_transform_spatial",
            wait_for_completion=True,
            poke_interval=10,
        )

        # Flujo de dependencias Medallón:
        # Capa 1: Ingesta en paralelo
        start_pipeline >> [trig_ingest_mun, trig_ingest_crops, trig_ingest_sipsa, trig_ingest_oni]

        # Capa 1 >> Capa 2: Ingesta activa Carga Bronze
        trig_ingest_mun >> trig_load_mun
        trig_ingest_crops >> trig_load_crops
        trig_ingest_sipsa >> trig_load_sipsa
        trig_ingest_oni >> trig_load_oni

        # Capa 2 >> Capa 3: Carga Bronze completa activa Transformación Silver
        [trig_load_mun, trig_load_crops, trig_load_sipsa, trig_load_oni] >> trig_trans_crops >> trig_trans_consolidado

        # Capa 3 >> Capa 4: Transformación Silver completa activa Transformación Gold GIS
        trig_trans_consolidado >> trig_trans_spatial >> end_pipeline

    dag = master_orchestrator_dag()
    DAG = dag
