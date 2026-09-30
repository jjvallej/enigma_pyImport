from __future__ import annotations

from pathlib import Path
import sys

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)


def consolidar_bronze_a_silver(config=None):
    from src_transform_consolidado import consolidar_bronze_a_silver as _cbs
    return _cbs(config)


def run_transform_consolidado(config=None):
    from src_transform_consolidado import run_transform_consolidado as _rtc
    return _rtc(config)


def run_transform_crops(config=None):
    return run_transform_consolidado(config)


__all__ = [
    "consolidar_bronze_a_silver",
    "run_transform_consolidado",
    "run_transform_crops",
]

if __name__ == "__main__":
    run_transform_consolidado()

try:
    from datetime import datetime
    from airflow.decorators import dag, task

    from src_common import get_airflow_dag_kwargs, run_with_airflow_alarm

    @dag(
        dag_id="src_transform_crops",
        description="Alias del consolidado Silver único (crops+SIPSA+ONI)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "consolidado", "transform", "silver"],
        **get_airflow_dag_kwargs(),
    )
    def transform_crops_dag():
        @task(task_id="run_transform_crops")
        def execute_transform() -> dict[str, object]:
            return run_with_airflow_alarm(run_transform_crops)

        execute_transform()

    dag = transform_crops_dag()
except ImportError:
    pass

