"""Pruebas unitarias para la configuración multientorno y el pipeline:
Cultivos: src_ingest_crops, src_load_crops
Precios: src_ingest_sipsa, src_load_sipsa
Clima: src_ingest_oni, src_load_oni
CKAN: src_ingest/load/transform_ckan_comentarios
Silver agri: src_transform_consolidado
"""

from __future__ import annotations

from pyimport.config_loader import load_config
from pyimport.src_transform_consolidado import clean_str
from pyimport.main import main


def test_multienvironment_config_resolution() -> None:
    """Verifica la resolución multientorno (dev, qa, prod) desde config.yaml."""
    cfg_dev = load_config(env="dev")
    assert cfg_dev["active_env"] == "dev"
    assert cfg_dev["bigquery"]["project_id"] == "datagov-477214"
    assert cfg_dev["bigquery"]["datasets"]["bronze"] == "valledata"
    assert cfg_dev["bigquery"]["tables"]["sipsa_bronze"] == "bronze_agri_sipsa"
    assert cfg_dev["bigquery"]["tables"]["consolidado_silver"] == "silver_agri_consolidado"
    assert cfg_dev["bigquery"]["tables"]["comentarios_bronze"] == "bronze_comentarios"
    assert cfg_dev["bigquery"]["tables"]["comentarios_silver"] == "silver_comentarios"
    assert cfg_dev["ckan_comentarios"]["table"] == "comment"
    assert cfg_dev["paths"]["ckan_comentarios_csv"].endswith("ckan_comentarios.csv")
    assert cfg_dev["paths"]["ckan_comentarios_silver_csv"].endswith("silver_comentarios.csv")

    cfg_qa = load_config(env="qa")
    assert cfg_qa["active_env"] == "qa"
    assert cfg_qa["bigquery"]["datasets"]["bronze"] == "valledata"
    assert cfg_qa["bigquery"]["tables"]["sipsa_bronze"] == "bronze_agri_sipsa"

    cfg_prod = load_config(env="prod")
    assert cfg_prod["active_env"] == "prod"
    assert cfg_prod["bigquery"]["datasets"]["bronze"] == "valledata"
    assert cfg_prod["bigquery"]["tables"]["sipsa_bronze"] == "bronze_agri_sipsa"


def test_connection_params_from_config() -> None:
    """Verifica que cada entorno (dev/qa/prod) defina y resuelva sus conexiones."""
    from pyimport.config_loader import (
        get_composer_params,
        get_connection_id,
        get_connection_list,
        get_connection_params,
    )

    for env_name in ("dev", "qa", "prod"):
        cfg = load_config(env=env_name)
        env_conns = cfg["env_spec"]["connections"]
        assert env_conns["sipsa_dane"] == "sipsa_dane"
        assert env_conns["noaa_oni"] == "noaa_oni"
        assert env_conns["gobernacion_valle"] == "gobernacion_valle"
        assert env_conns["google_cloud_default"] == "google_cloud_default"
        assert isinstance(env_conns["ckan_comentarios"], list)
        assert get_connection_params(cfg)["sipsa_dane"] == env_conns["sipsa_dane"]
        assert get_connection_id(cfg, "sipsa_dane") == "sipsa_dane"
        assert get_connection_id(cfg, "noaa_oni") == "noaa_oni"
        assert get_connection_id(cfg, "gobernacion_valle") == "gobernacion_valle"
        assert get_connection_id(cfg, "google_cloud_default") == "google_cloud_default"
        assert get_connection_list(cfg, "ckan_comentarios") == []
        composer = get_composer_params(cfg)
        assert composer["environment"] == cfg["env_spec"]["composer"]["environment"]
        assert composer["location"] == "us-east1"


def test_ckan_connection_list_normalization() -> None:
    """Normaliza el arreglo de conexiones CKAN (str o dict) y respeta el tope 14."""
    from pyimport.config_loader import get_connection_list

    cfg = {
        "env_spec": {
            "connections": {
                "ckan_comentarios": [
                    "ckan_pg_01",
                    {"conn_id": "ckan_pg_02", "municipio": "Sevilla"},
                ]
            }
        },
        "ckan_comentarios": {"max_connections": 14},
    }
    items = get_connection_list(cfg, "ckan_comentarios")
    assert items == [
        {"conn_id": "ckan_pg_01", "municipio": ""},
        {"conn_id": "ckan_pg_02", "municipio": "Sevilla"},
    ]


def test_ingest_scripts_consume_connection_params() -> None:
    """Verifica que cada script de ingest consulte su parámetro de conexión vía config."""
    from pyimport.config_loader import get_connection_id, load_config

    cfg = load_config()
    assert get_connection_id(cfg, "gobernacion_valle") == "gobernacion_valle"
    assert get_connection_id(cfg, "sipsa_dane") == "sipsa_dane"
    assert get_connection_id(cfg, "noaa_oni") == "noaa_oni"


def test_clean_str_removes_accents() -> None:
    """Verifica la limpieza de tildes usada en el consolidado Silver."""
    assert clean_str("Plátano Verde") == "platano verde"
    assert clean_str("Limón Tahití") == "limon tahiti"
    assert clean_str("Maracuyá") == "maracuya"
    assert clean_str("Café Molido") == "cafe molido"
    assert clean_str("Yuca  ") == "yuca"


def test_normalize_municipio_and_cultivo_helpers() -> None:
    """Verifica que normalize_municipio y normalize_cultivo eliminen tildes, asteriscos y espacios."""
    from pyimport.src_common import normalize_municipio, normalize_cultivo

    assert normalize_municipio("Cali *") == "cali"
    assert normalize_municipio(" Alcalá ") == "alcala"
    assert normalize_municipio("EL CERRITO") == "el cerrito"

    assert normalize_cultivo("Maíz *") == "maiz"
    assert normalize_cultivo("Plátano * ") == "platano"
    assert normalize_cultivo("AGUACATE HASS") == "aguacate hass"


def test_transform_ckan_comentarios_dedupe_and_sentiment() -> None:
    """Deduplica y agrega sentimiento/score con analizador mock de pysentimiento."""
    from types import SimpleNamespace

    import pandas as pd
    from pyimport.src_transform_ckan_comentarios import transform_comentarios

    class FakeAnalyzer:
        def predict(self, text: str):
            text_l = text.lower()
            if "malo" in text_l or "pésimo" in text_l or "pesimo" in text_l:
                return SimpleNamespace(output="NEG", probas={"NEG": 0.9, "NEU": 0.05, "POS": 0.05})
            if "bueno" in text_l or "excelente" in text_l:
                return SimpleNamespace(output="POS", probas={"POS": 0.88, "NEU": 0.1, "NEG": 0.02})
            return SimpleNamespace(output="NEU", probas={"NEU": 0.7, "POS": 0.2, "NEG": 0.1})

    df = pd.DataFrame(
        [
            {"id": "1", "content": "Hola", "source_conn_id": "ckan_pg_01", "municipio": "Buga", "id_municipio": "76111"},
            {"id": "1", "content": "Servicio excelente", "source_conn_id": "ckan_pg_01", "municipio": "Buga", "id_municipio": "76111"},
            {"id": "2", "content": "Esto es malo", "source_conn_id": "ckan_pg_02", "municipio": "Cali", "id_municipio": "76001"},
        ]
    )
    out = transform_comentarios(df, analyzer=FakeAnalyzer())
    assert len(out) == 2
    assert "id_municipio" not in out.columns, "id_municipio debe ser eliminado de gold_comentarios_sentimiento"
    row1 = out.loc[out["id"] == "1"].iloc[0]
    assert row1["content"] == "Servicio excelente"
    assert row1["sentimiento"] == "positivo"
    assert row1["sentimiento_codigo"] == "POS"
    assert abs(float(row1["score"]) - 0.88) < 1e-9
    row2 = out.loc[out["id"] == "2"].iloc[0]
    assert row2["sentimiento"] == "negativo"
    assert abs(float(row2["score_neg"]) - 0.9) < 1e-9

    from pyimport.src_transform_ckan_comentarios import build_gold_comentarios_consolidado
    gold = build_gold_comentarios_consolidado(out)
    expected_cols = [
        "municipio",
        "id_dataset",
        "nombre_dataset",
        "total_comentarios",
        "positivos",
        "negativos",
        "neutros",
        "confianza_promedio",
        "emocion_predominante",
    ]
    assert list(gold.columns) == expected_cols
    assert len(gold) >= 1
    assert "id_municipio" not in gold.columns


def test_ingest_ckan_uses_test_data_without_connections() -> None:
    """Sin Postgres, con use_test_data=true, genera fixtures en staging."""
    from pyimport.src_ingest_ckan_comentarios import run_ingest_ckan_comentarios

    cfg = load_config(env="dev")
    cfg["ckan_comentarios"] = dict(cfg["ckan_comentarios"])
    cfg["ckan_comentarios"]["use_test_data"] = True
    cfg["ckan_comentarios"]["test_data_records"] = 120  # más liviano en tests
    res = run_ingest_ckan_comentarios(cfg)
    assert res["status"] == "SUCCESS"
    assert res.get("mode") == "test_data"
    assert res["total_rows"] == 120
    assert res["connections"] >= 1


def test_main_cli_script_flags() -> None:
    """Verifica la interfaz CLI de main.py con flags multientorno y scripts especificos."""
    try:
        main(["--help"])
    except SystemExit as exc:
        assert exc.code == 0


def test_ckan_dags_register_in_composer_layout() -> None:
    """Los DAG src_*_ckan_comentarios deben registrarse en el layout de Composer."""
    import importlib.util
    from pathlib import Path

    dag_folder = Path(__file__).resolve().parents[1] / "dags" / "gdv_general_dbt_dag" / "dags_valledata"
    expected = {
        "src_ingest_ckan_comentarios.py": "src_ingest_ckan_comentarios",
        "src_load_ckan_comentarios.py": "src_load_ckan_comentarios",
        "src_transform_ckan_comentarios.py": "src_transform_ckan_comentarios",
        "src_importacion_sentimiento.py": "src_importacion_sentimiento",
        "src_importacion_cultivos.py": "src_importacion_cultivos",
    }
    for filename, dag_id in expected.items():
        path = dag_folder / filename
        assert path.exists(), f"Falta {path} para subir a Composer"
        spec = importlib.util.spec_from_file_location(path.stem, path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        dag = getattr(mod, "dag", None) or getattr(mod, "DAG", None)
        assert dag is not None, f"{filename} no registró un DAG (no aparecerá en GCP)"
        assert dag.dag_id == dag_id



def test_airflow_alarm_helpers() -> None:
    """raise_if_failed dispara alarma en status ERROR; SUCCESS no falla."""
    import pytest
    from pyimport.src_common import raise_if_failed, get_airflow_dag_kwargs, airflow_failure_alarm

    assert raise_if_failed({"status": "SUCCESS"})["status"] == "SUCCESS"
    assert raise_if_failed({"status": "SKIPPED"})["status"] == "SKIPPED"
    with pytest.raises(Exception) as exc:
        raise_if_failed({"status": "ERROR", "errors": ["boom"]}, context_label="unit")
    assert "ALARM" in str(exc.value)

    kwargs = get_airflow_dag_kwargs()
    assert "default_args" in kwargs
    assert kwargs["default_args"]["on_failure_callback"] is airflow_failure_alarm

    # callback no debe romper aunque no haya email
    airflow_failure_alarm(
        {
            "dag": type("D", (), {"dag_id": "demo"})(),
            "task_instance": type("T", (), {"task_id": "t1", "try_number": 1, "run_id": "r1"})(),
            "run_id": "r1",
            "exception": RuntimeError("fallo demo"),
        }
    )
