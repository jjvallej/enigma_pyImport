"""ETAPA IMPORTACIÓN SENTIMIENTO (src_importacion_sentimiento.py)

DAG de Importación y Procesamiento de Datos de Sentimiento (CKAN Comentarios).
- Frecuencia: Diaria (`@daily` / `0 0 * * *`).
- Estado Inicial: Pausado / Deshabilitado (`is_paused_upon_creation=True`) por defecto
  mientras se resuelve la conectividad de red con las instancias PostgreSQL CKAN.

Flujo de ejecución:
  1. Ingesta CKAN Comentarios (`src_ingest_ckan_comentarios.py`)
  2. Carga Bronze CKAN Comentarios (`src_load_ckan_comentarios.py`)
  3. Transformación Silver CKAN Comentarios (`src_transform_ckan_comentarios.py`)
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


def run_importacion_sentimiento(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Ejecuta secuencialmente la ingesta, carga y transformación de datos de sentimiento."""
    from src_ingest_ckan_comentarios import run_ingest_ckan_comentarios
    from src_load_ckan_comentarios import run_load_ckan_comentarios
    from src_transform_ckan_comentarios import run_transform_ckan_comentarios

    cfg = config or load_config()
    composer = get_composer_params(cfg)
    print(f"🚀 [SRC_IMPORTACION_SENTIMIENTO] Iniciando Pipeline de Sentimiento (Composer={composer.get('environment', '-')})")

    # 1. Ingesta Raw
    print("1️⃣ [PASO 1/3 - INGESTA CKAN] Ingesta de comentarios desde PostgreSQL CKAN...")
    res_ingest = run_ingest_ckan_comentarios(cfg)

    # 2. Carga Bronze
    print("2️⃣ [PASO 2/3 - CARGA BRONZE] Carga de tabla bronze_comentarios...")
    res_load = run_load_ckan_comentarios(cfg)

    # 3. Transformación Silver
    print("3️⃣ [PASO 3/3 - TRANSFORMACIÓN SILVER] Transformación NLP y clasificación de sentimiento...")
    res_transform = run_transform_ckan_comentarios(cfg)

    print("🎉 [SRC_IMPORTACION_SENTIMIENTO] Pipeline de Sentimiento finalizado.")
    return {
        "status": "SUCCESS",
        "ingest": res_ingest,
        "load": res_load,
        "transform": res_transform,
    }


if __name__ == "__main__":
    run_importacion_sentimiento()

# Definición del DAG de Airflow (Compatibilidad Composer 2 y 3)
try:
    try:
        from airflow.sdk import dag as _af_dag, task as _af_task
    except ImportError:
        from airflow.decorators import dag as _af_dag, task as _af_task
    from airflow.operators.empty import EmptyOperator
except ImportError:
    _af_dag = _af_task = None

if _af_dag is not None:
    from datetime import datetime

    @_af_dag(
        dag_id="src_importacion_sentimiento",
        description="Importación diaria de sentimiento CKAN (Deshabilitado por problemas de conexión)",
        start_date=datetime(2000, 1, 1),
        schedule="@daily",
        is_paused_upon_creation=True,
        catchup=False,
        tags=["valledata", "sentimiento", "ckan", "diario", "deshabilitado"],
        **get_airflow_dag_kwargs(),
    )
    def importacion_sentimiento_dag():
        start_pipeline = EmptyOperator(task_id="start_sentimiento_pipeline")
        end_pipeline = EmptyOperator(task_id="end_sentimiento_pipeline")

        @_af_task(task_id="task_ingest_ckan_comentarios")
        def task_ingest() -> Dict[str, Any]:
            return run_with_airflow_alarm(run_ingest_ckan_comentarios)

        @_af_task(task_id="task_load_ckan_comentarios")
        def task_load() -> Dict[str, Any]:
            return run_with_airflow_alarm(run_load_ckan_comentarios)

        @_af_task(task_id="task_transform_ckan_comentarios")
        def task_transform() -> Dict[str, Any]:
            return run_with_airflow_alarm(run_transform_ckan_comentarios)

        t_ingest = task_ingest()
        t_load = task_load()
        t_transform = task_transform()

        start_pipeline >> t_ingest >> t_load >> t_transform >> end_pipeline

    dag = importacion_sentimiento_dag()
    DAG = dag
