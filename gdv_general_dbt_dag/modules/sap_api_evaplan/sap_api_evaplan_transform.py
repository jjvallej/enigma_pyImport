"""
Transform SAP API Evaplan: actualiza Gold con la última ejecución.
- Bronze mantiene histórico (varias ejecuciones por día, con fecha_lectura y run_ts).
- Gold (sap_api_evaplan_final_data): se reemplaza solo el bloque (periodo_ini, periodo_fin)
  de la ejecución recién cargada; el resto de periodos se mantiene (ej. run 01-12 luego run 03-07
  deja en Gold 01-02 y 08-12 del primer run y 03-07 del segundo).
"""
from datetime import datetime
from typing import Any, Optional, Tuple, Union

from google.cloud import bigquery

from modules.config import CONF, PROJECT_ID, DATASET_ID_GOLD
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


def _gold_table() -> str:
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")
    dataset = getattr(cfg, "gold_dataset", None) or DATASET_ID_GOLD
    table = getattr(cfg, "gold_table", "sap_api_evaplan_final_data")
    return f"`{PROJECT_ID}.{dataset}.{table}`"


def _gold_dataset() -> str:
    cfg = _get_cfg()
    if not cfg:
        raise ValueError("No existe la sección 'sap_api_evaplan' en config.yaml")
    return getattr(cfg, "gold_dataset", None) or DATASET_ID_GOLD


def get_latest_run_info() -> Optional[Tuple[Union[datetime, str], str, str]]:
    """
    Obtiene (run_ts, periodo_ini, periodo_fin) de la última ejecución en Bronze.
    run_ts se devuelve tal cual (datetime si columna TIMESTAMP, str si STRING) para comparar bien en el INSERT.
    Returns None si Bronze está vacío.
    """
    client = get_bq_client()
    bronze = _bronze_table()
    query = f"""
    SELECT run_ts, periodo_ini, periodo_fin
    FROM {bronze}
    WHERE run_ts = (SELECT MAX(run_ts) FROM {bronze})
    LIMIT 1
    """
    job = client.query(query)
    rows = list(job.result())
    if not rows:
        return None
    row = rows[0]
    return (row.run_ts, str(row.periodo_ini), str(row.periodo_fin))


def ensure_gold_table(client: bigquery.Client) -> None:
    """Crea la tabla Gold si no existe (misma estructura que Bronze)."""
    gold_fqn = _gold_table().strip("`")
    try:
        client.get_table(gold_fqn)
        return
    except Exception:
        pass
    bronze = _bronze_table()
    create_sql = f"""
    CREATE TABLE {_gold_table()} AS
    SELECT * FROM {bronze} WHERE 1=0
    """
    client.query(create_sql).result()
    print(f"[OK] Tabla Gold creada: {gold_fqn}")


def run_transform() -> None:
    """
    Actualiza Gold con la última ejecución en Bronze:
    1. Obtiene la última run (max run_ts) y su (periodo_ini, periodo_fin).
    2. Borra de Gold solo las filas de ese bloque (periodo_ini, periodo_fin).
    3. Inserta en Gold todas las filas de Bronze de esa run.
    Así se mantienen en Gold los bloques de periodos no tocados por esta ejecución.
    """
    client = get_bq_client()
    info = get_latest_run_info()
    if not info:
        print("[WARN] Bronze vacío; no hay nada que llevar a Gold.")
        return

    run_ts, periodo_ini, periodo_fin = info
    bronze = _bronze_table()
    gold = _gold_table()

    ensure_gold_table(client)

    # Borrar de Gold solo el bloque (periodo_ini, periodo_fin) que vamos a reemplazar.
    # Las columnas en BQ pueden ser INT64 (autodetect); comparamos como STRING para evitar 400.
    delete_sql = f"""
    DELETE FROM {gold}
    WHERE CAST(periodo_ini AS STRING) = @periodo_ini AND CAST(periodo_fin AS STRING) = @periodo_fin
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("periodo_ini", "STRING", periodo_ini),
            bigquery.ScalarQueryParameter("periodo_fin", "STRING", periodo_fin),
        ]
    )
    client.query(delete_sql, job_config=job_config).result()
    print(f"[OK] Borrado en Gold bloque periodo_ini={periodo_ini}, periodo_fin={periodo_fin}")

    # Insertar en Gold todas las filas de la última run.
    # Bronze puede tener run_ts como TIMESTAMP o STRING; usamos el tipo correcto para que coincida.
    if isinstance(run_ts, datetime):
        insert_sql = f"""
        INSERT INTO {gold}
        SELECT * FROM {bronze}
        WHERE run_ts = @run_ts
        """
        job_config2 = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_ts", "TIMESTAMP", run_ts),
            ]
        )
    else:
        insert_sql = f"""
        INSERT INTO {gold}
        SELECT * FROM {bronze}
        WHERE run_ts = @run_ts
        """
        job_config2 = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("run_ts", "STRING", str(run_ts)),
            ]
        )
    query_job = client.query(insert_sql, job_config=job_config2)
    query_job.result()
    num_rows = query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else "?"
    print(f"[OK] Insertados en Gold registros de run_ts={run_ts} (filas: {num_rows})")
