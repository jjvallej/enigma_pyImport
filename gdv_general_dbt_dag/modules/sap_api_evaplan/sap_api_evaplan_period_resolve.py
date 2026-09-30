"""
Resolución de periodo ini/fin (YYYYMM) para la API SAP Evaplan.

Prioridad (por campo, de mayor a menor):
- Valores en dag_run.conf y en params del DAG (formulario o JSON al disparar manualmente)
- Parámetros opcionales del script (script_ini / script_fin)
- config.yaml

Si ``modo_ejecucion`` es ``manual`` y el run es manual, deben venir fecha_inicio y fecha_fin en el
trigger (no basta el script); en scheduler se ignoran esas exigencias y se usa script/config.

Reglas adicionales:
- fecha_inicio: si sigue vacío, se usa YYYYMM derivado de ds_nodash (primeros 6 caracteres).
- fecha_fin: si sigue vacío tras config, se usa el mes calendario actual (America/Bogota) en YYYYMM.

Claves aceptadas en conf: fecha_inicio, fecha_fin, y alias ini, fin.
En config: fecha_inicio / fecha_fin, con compatibilidad hacia atrás con ini / fin.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

_YYYYMM = re.compile(r"^\d{6}$")
_COLOMBIA_TZ = ZoneInfo("America/Bogota")


def _as_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _validate_yyyymm(label: str, value: str) -> str:
    if not _YYYYMM.match(value):
        raise ValueError(
            f"{label} debe ser YYYYMM (6 dígitos); recibido: {value!r}"
        )
    return value


def _conf_get(conf: Optional[Mapping[str, Any]], *keys: str) -> Optional[str]:
    if not conf:
        return None
    for k in keys:
        if k in conf and conf[k] is not None:
            return _as_str(conf[k])
    return None


def _config_fecha_inicio(cfg: Any) -> Optional[str]:
    return _as_str(
        getattr(cfg, "fecha_inicio", None) or getattr(cfg, "ini", None)
    )


def _config_fecha_fin(cfg: Any) -> Optional[str]:
    return _as_str(getattr(cfg, "fecha_fin", None) or getattr(cfg, "fin", None))


def _current_yyyymm_colombia() -> str:
    return datetime.now(tz=_COLOMBIA_TZ).strftime("%Y%m")


def build_period_conf_from_context(context: Mapping[str, Any]) -> dict:
    """
    Combina ``dag_run.conf`` (JSON opcional al disparar) con ``params`` del DAG
    (formulario de Trigger DAG con campos fecha_inicio / fecha_fin).

    Solo los params con valor no vacío se fusionan; un campo vacío no elimina
    claves ya presentes en ``conf``.
    """
    dag_run = context.get("dag_run")
    raw = getattr(dag_run, "conf", None) if dag_run else None
    merged: dict = dict(raw) if isinstance(raw, dict) else {}
    params = context.get("params")
    if not isinstance(params, dict):
        return merged
    for key in ("fecha_inicio", "fecha_fin", "ini", "fin"):
        if key not in params:
            continue
        val = _as_str(params.get(key))
        if val is not None:
            merged[key] = val
    return merged


def assert_manual_period_for_airflow_trigger(
    cfg: Any,
    dag_conf: Mapping[str, Any],
    *,
    airflow_run_is_manual: bool,
) -> None:
    """
    Si ``modo_ejecucion`` es ``manual`` y el run es un disparo manual en Airflow,
    exige ``fecha_inicio`` y ``fecha_fin`` (o ``ini`` / ``fin``) en ``dag_conf``.

    En ejecución programada (scheduler) no aplica: ahí se usan script/config sin pedir formulario.
    """
    modo = (str(getattr(cfg, "modo_ejecucion", "auto") or "auto").strip().lower())
    if modo != "manual":
        return
    if not airflow_run_is_manual:
        return
    ini = _conf_get(dag_conf, "fecha_inicio", "ini")
    fin = _conf_get(dag_conf, "fecha_fin", "fin")
    if not ini or not fin:
        raise ValueError(
            "modo_ejecucion=manual: en un disparo manual debe indicar fecha_inicio y fecha_fin "
            "(formato YYYYMM) en el formulario de Trigger DAG o en el JSON de configuración."
        )


def resolve_sap_api_evaplan_periods(
    cfg: Any,
    ds_nodash: str,
    dag_conf: Optional[Mapping[str, Any]] = None,
    script_ini: Optional[str] = None,
    script_fin: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Devuelve (fecha_inicio, fecha_fin) en formato YYYYMM validado.

    dag_conf: típicamente context['dag_run'].conf en Airflow (dict o vacío).
    script_ini / script_fin: overrides opcionales definidos en el DAG (ej. constantes).
    """
    conf = dag_conf or {}

    # --- fecha_inicio
    raw_ini = (
        _conf_get(conf, "fecha_inicio", "ini")
        or _as_str(script_ini)
        or _config_fecha_inicio(cfg)
    )
    if raw_ini is None:
        if ds_nodash and len(ds_nodash) >= 6:
            raw_ini = ds_nodash[:6]
        else:
            raise ValueError(
                "No se pudo resolver fecha_inicio: falta en config/conf/script y "
                "ds_nodash no tiene al menos 6 caracteres (YYYYMM)."
            )
    fecha_inicio = _validate_yyyymm("fecha_inicio", raw_ini)

    # --- fecha_fin
    raw_fin = (
        _conf_get(conf, "fecha_fin", "fin")
        or _as_str(script_fin)
        or _config_fecha_fin(cfg)
    )
    if raw_fin is None:
        raw_fin = _current_yyyymm_colombia()
    fecha_fin = _validate_yyyymm("fecha_fin", raw_fin)

    if fecha_inicio > fecha_fin:
        raise ValueError(
            f"fecha_inicio ({fecha_inicio}) no puede ser posterior a fecha_fin ({fecha_fin})."
        )

    return fecha_inicio, fecha_fin
