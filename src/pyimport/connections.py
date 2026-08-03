"""Módulo para la resolución y registro de conexiones en Apache Airflow."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

CONN_SIPSA_DANE = "sipsa_dane"
CONN_NOAA_ONI = "noaa_oni"
CONN_GOBERNACION_VALLE = "gobernacion_valle"

DEFAULT_HOSTS = {
    CONN_SIPSA_DANE: "https://www.dane.gov.co",
    CONN_NOAA_ONI: "https://www.cpc.ncep.noaa.gov",
    CONN_GOBERNACION_VALLE: "https://datosabiertos.valledelcauca.gov.co",
}

DESCRIPTIONS = {
    CONN_SIPSA_DANE: "Conexión HTTP al portal DANE SIPSA para descarga de anexos de precios",
    CONN_NOAA_ONI: "Conexión HTTP al portal NOAA CPC para consulta del índice ONI",
    CONN_GOBERNACION_VALLE: "Conexión HTTP al portal de Datos Abiertos de la Gobernación del Valle del Cauca",
}


def register_connections_in_airflow() -> dict[str, str]:
    """Crea o asegura que las conexiones HTTP existan en la base de datos de Airflow para que aparezcan en Admin -> Connections de la UI."""
    results: dict[str, str] = {}
    try:
        from airflow.models import Connection
        from airflow.utils.session import create_session

        connections_data = [
            {
                "conn_id": conn_id,
                "conn_type": "http",
                "host": DEFAULT_HOSTS[conn_id],
                "description": DESCRIPTIONS[conn_id],
            }
            for conn_id in (CONN_SIPSA_DANE, CONN_NOAA_ONI, CONN_GOBERNACION_VALLE)
        ]

        with create_session() as session:
            for cdata in connections_data:
                cid = cdata["conn_id"]
                existing = (
                    session.query(Connection)
                    .filter(Connection.conn_id == cid)
                    .first()
                )
                if not existing:
                    new_conn = Connection(
                        conn_id=cid,
                        conn_type=cdata["conn_type"],
                        host=cdata["host"],
                        description=cdata["description"],
                    )
                    session.add(new_conn)
                    results[cid] = "creada"
                else:
                    results[cid] = "existente"
            session.commit()
    except Exception as exc:
        logger.debug("No se pudieron registrar las conexiones en Airflow DB: %s", exc)

    return results


def get_connection_base_url(conn_id: str, default_host: str | None = None) -> str:
    """Obtiene la URL base desde una Conexión HTTP de Airflow si existe, o usa el host por defecto.

    Parámetros:
        conn_id: Identificador de la conexión en Airflow (ej. 'sipsa_dane').
        default_host: Host por defecto si la conexión no está definida en Airflow.

    Retorna:
        La URL base normalizada (ej. 'https://www.dane.gov.co') sin barra final.
    """
    if default_host is None:
        default_host = DEFAULT_HOSTS.get(conn_id, "")

    try:
        try:
            from airflow.sdk.bases.hook import BaseHook
        except ImportError:
            from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(conn_id)
        if conn and conn.host:
            host = conn.host.strip()
            if not host.startswith(("http://", "https://")):
                schema = (conn.schema or "https").strip()
                host = f"{schema}://{host}"
            return host.rstrip("/")
    except Exception:
        # Intenta auto-registrar las conexiones si Airflow está activo pero no existen aún
        register_connections_in_airflow()

    return default_host.strip().rstrip("/")


# Intentar auto-registro al importar en entorno Airflow
register_connections_in_airflow()
