"""Pruebas unitarias para el módulo de gestión de conexiones Airflow (connections.py)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyimport.connections import (
    CONN_GOBERNACION_VALLE,
    CONN_NOAA_ONI,
    CONN_SIPSA_DANE,
    DEFAULT_HOSTS,
    get_connection_base_url,
)
from pyimport.cultivos import (
    get_cultivos_permanentes_url,
    get_cultivos_transitorios_url,
)
from pyimport.oni import get_oni_url
from pyimport.sipsa import get_sipsa_url_templates


def test_default_connection_base_urls() -> None:
    """Verifica que sin Airflow DB configurada, se retornen los hosts por defecto."""
    assert get_connection_base_url(CONN_SIPSA_DANE) == "https://www.dane.gov.co"
    assert get_connection_base_url(CONN_NOAA_ONI) == "https://www.cpc.ncep.noaa.gov"
    assert (
        get_connection_base_url(CONN_GOBERNACION_VALLE)
        == "https://datosabiertos.valledelcauca.gov.co"
    )


def test_airflow_connection_override() -> None:
    """Verifica la resolución dinámica cuando Airflow BaseHook retorna una Conexión configurada."""
    mock_conn = MagicMock()
    mock_conn.host = "https://sipsa-mirror.internal.org/"
    mock_conn.schema = "https"

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=mock_conn):
        url = get_connection_base_url(CONN_SIPSA_DANE)
        assert url == "https://sipsa-mirror.internal.org"

        templates = get_sipsa_url_templates(CONN_SIPSA_DANE)
        assert templates[0].startswith("https://sipsa-mirror.internal.org/files/")


def test_oni_url_resolution_with_connection() -> None:
    """Verifica que get_oni_url utilice el host de la conexión Airflow."""
    mock_conn = MagicMock()
    mock_conn.host = "noaa-mirror.local"
    mock_conn.schema = "http"

    with patch("pyimport.connections.get_connection_base_url", return_value="http://noaa-mirror.local"):
        oni_url = get_oni_url(CONN_NOAA_ONI)
        assert "noaa-mirror.local" in oni_url


def test_cultivos_url_resolution_with_connection() -> None:
    """Verifica que las URLs de cultivos usen la conexión Airflow configurada."""
    mock_conn = MagicMock()
    mock_conn.host = "https://datos.valle.gov.co"
    mock_conn.schema = "https"

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=mock_conn):
        perm_url = get_cultivos_permanentes_url(CONN_GOBERNACION_VALLE)
        trans_url = get_cultivos_transitorios_url(CONN_GOBERNACION_VALLE)

        assert perm_url.startswith("https://datos.valle.gov.co/dataset/")
        assert trans_url.startswith("https://datos.valle.gov.co/dataset/")


def test_register_connections_in_airflow() -> None:
    """Verifica que register_connections_in_airflow sintonice adecuadamente sin lanzar excepciones."""
    from pyimport.connections import register_connections_in_airflow

    res = register_connections_in_airflow()
    assert isinstance(res, dict)
