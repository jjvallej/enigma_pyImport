"""
Transform SAP API Evaplan por capas:
- Bronze: histórico completo (sin cambios).
- Silver: solo última ejecución, con textos en MAYÚSCULA.
- Gold: copia de Silver (sin transformaciones adicionales).
"""
from typing import Any, List

from google.cloud import bigquery

from modules.config import CONF, PROJECT_ID, DATASET_ID_GOLD, DATASET_ID_SILVER
from modules.gcp_utils import get_bq_client


def _get_cfg() -> Any:
    return getattr(CONF, "sap_api_evaplan", None)


def _bronze_table() -> str:
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")
    from modules.config import DATASET_ID_BRONZE

    dataset = getattr(cfg, "target_dataset", None) or DATASET_ID_BRONZE
    return f"`{PROJECT_ID}.{dataset}.{cfg.target_table}`"


def _silver_table() -> str:
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")
    dataset = getattr(cfg, "silver_dataset", None) or DATASET_ID_SILVER
    table = getattr(cfg, "silver_table", None) or "sap_api_evaplan_transformed_data"
    return f"`{PROJECT_ID}.{dataset}.{table}`"


def _gold_table() -> str:
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")
    dataset = getattr(cfg, "gold_dataset", None) or DATASET_ID_GOLD
    table = getattr(cfg, "gold_table", "sap_api_evaplan_final_data")
    return f"`{PROJECT_ID}.{dataset}.{table}`"


def _bronze_has_data(client: bigquery.Client) -> bool:
    bronze = _bronze_table()
    query = f"SELECT COUNT(1) AS n FROM {bronze}"
    rows = list(client.query(query).result())
    return bool(rows and rows[0].n and int(rows[0].n) > 0)


def _build_silver_select_list(client: bigquery.Client) -> str:
    """
    Construye el SELECT para Silver:
    - STRING -> UPPER()
    - resto de tipos -> valor original
    """
    bronze_fqn = _bronze_table().strip("`")
    table = client.get_table(bronze_fqn)
    expressions: List[str] = []
    for field in table.schema:
        name = field.name
        if field.field_type.upper() == "STRING":
            expressions.append(f"UPPER(`{name}`) AS `{name}`")
        else:
            expressions.append(f"`{name}`")
    return ",\n        ".join(expressions)


def run_transform() -> None:
    """
    Ejecuta el flujo Bronze -> Silver -> Gold:
    1) Silver: reemplaza completamente con la última ejecución de Bronze, aplicando UPPER en STRING.
    2) Gold: reemplaza completamente con el contenido actual de Silver.
    """
    client = get_bq_client()
    if not _bronze_has_data(client):
        print("[WARN] Bronze vacío; no hay nada que llevar a Gold.")
        return

    bronze = _bronze_table()
    silver = _silver_table()
    gold = _gold_table()
    select_list = _build_silver_select_list(client)

    # Silver: última ejecución de Bronze + transformación de texto a mayúsculas.
    silver_sql = f"""
    CREATE OR REPLACE TABLE {silver} AS
    SELECT
        {select_list}
    FROM {bronze}
    WHERE run_ts = (SELECT MAX(run_ts) FROM {bronze})
    """
    client.query(silver_sql).result()
    print(f"[OK] Silver actualizado desde Bronze: {silver}")

    # Gold: copia directa de Silver (sin transformaciones adicionales).
    gold_sql = f"""
    CREATE OR REPLACE TABLE {gold} AS
    SELECT * FROM {silver}
    """
    client.query(gold_sql).result()
    print(f"[OK] Gold actualizado desde Silver: {gold}")
