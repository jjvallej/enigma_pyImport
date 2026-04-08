"""
DAG de ingesta: consulta la API SAP (zimportdata) con params ini/fin (YYYYMM),
guarda la respuesta como JSONL en GCS (data_staging/dpt_planeacion_municipal/sap_api_evaplan).
Al finalizar dispara el DAG de load.

modo_ejecucion en config: ``auto`` usa periodo desde SCRIPT_OVERRIDE_* o config.yaml (y ds/mes actual
según reglas). ``manual`` exige fecha_inicio y fecha_fin en el trigger (formulario o JSON) cuando el
run es manual; el scheduler sin formulario sigue usando script/config.
Prioridad de resolución: conf/params > SCRIPT_OVERRIDE_* > config.yaml.
"""
from datetime import timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils import timezone
from airflow.utils.types import DagRunType

import os
import sys


def add_project_root_to_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.basename(current_dir).startswith("dags_"):
        project_root = os.path.dirname(current_dir)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        return
    while current_dir != "/":
        modules_dir = os.path.join(current_dir, "modules")
        if os.path.exists(modules_dir):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            return
        current_dir = os.path.dirname(current_dir)
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)


add_project_root_to_path()

from modules.config import CONF, DEFAULT_BUCKET_NAME  # noqa: E402
from modules.sap_api_evaplan.sap_api_evaplan_period_resolve import (  # noqa: E402
    assert_manual_period_for_airflow_trigger,
    build_period_conf_from_context,
)
from modules.sap_api_evaplan.sap_api_evaplan_ingest import run_ingest, ensure_gcs_folder  # noqa: E402

CFG = CONF.sap_api_evaplan
_MODO_EJECUCION = str(getattr(CFG, "modo_ejecucion", "auto") or "auto").strip().lower()
_MANUAL_MODE = _MODO_EJECUCION == "manual"

if _MANUAL_MODE:
    DAG_PARAMS = {
        "fecha_inicio": Param(
            "",
            type="string",
            title="Fecha inicio (YYYYMM)",
            description="Obligatorio al disparar a mano si modo_ejecucion=manual. Formato YYYYMM (ej. 202401).",
        ),
        "fecha_fin": Param(
            "",
            type="string",
            title="Fecha fin (YYYYMM)",
            description="Obligatorio al disparar a mano si modo_ejecucion=manual. Formato YYYYMM.",
        ),
    }
else:
    # En modo auto no exponemos inputs en el formulario de Trigger DAG.
    DAG_PARAMS = {}

# Opcional: forzar periodo desde código (None = solo config.yaml + dag_run.conf al disparar manualmente)
#SCRIPT_OVERRIDE_INI = None
#SCRIPT_OVERRIDE_FIN = None
SCRIPT_OVERRIDE_INI = 202501
SCRIPT_OVERRIDE_FIN = 202512


def _do_ingest(**context):
    ds_nodash = context.get("ds_nodash") or ""
    dag_conf = build_period_conf_from_context(context)

    dag_run = context.get("dag_run")
    airflow_run_is_manual = bool(
        dag_run is not None and getattr(dag_run, "run_type", None) == DagRunType.MANUAL
    )
    assert_manual_period_for_airflow_trigger(
        CFG,
        dag_conf,
        airflow_run_is_manual=airflow_run_is_manual,
    )

    modo = getattr(CFG, "modo_ejecucion", "auto")
    print(
        f"[SAP_EVAPLAN_INGEST] modo_ejecucion={modo!r} run_type="
        f"{getattr(dag_run, 'run_type', None)!r} airflow_manual={airflow_run_is_manual} "
        f"periodo_conf={dag_conf} script_override=({SCRIPT_OVERRIDE_INI!r}, {SCRIPT_OVERRIDE_FIN!r})"
    )

    gcs_uri = run_ingest(
        ds_nodash=ds_nodash,
        dag_conf=dag_conf,
        script_ini=SCRIPT_OVERRIDE_INI,
        script_fin=SCRIPT_OVERRIDE_FIN,
        bucket_name=DEFAULT_BUCKET_NAME,
    )
    print(f"[OK] Exportado a {gcs_uri}")
    return gcs_uri


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="src_sap_api_evaplan_ingest_dag",
    default_args=DEFAULT_ARGS,
    description="Ingesta API SAP (zimportdata) a GCS - planeación municipal",
    schedule=(
        getattr(CFG, "ingest_schedule_interval", None)
        or getattr(CFG, "schedule_interval", None)
    ),
    start_date=timezone.datetime(2025, 1, 1),
    catchup=False,
    tags=["planeacion_municipal", "sap_api_evaplan", "api", "gcs"],
    params=DAG_PARAMS,
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="bronze") as bronze:
        ensure_folder = PythonOperator(
            task_id="ensure_gcs_folder",
            python_callable=ensure_gcs_folder,
            op_kwargs={"bucket_name": DEFAULT_BUCKET_NAME},
        )

        export_to_gcs = PythonOperator(
            task_id="export_sap_api_to_gcs",
            python_callable=_do_ingest,
        )

        ensure_folder >> export_to_gcs

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_sap_api_evaplan",
        trigger_dag_id="src_sap_api_evaplan_load_dag",
        wait_for_completion=False,
    )

    start >> bronze >> trigger_load_dag >> end
