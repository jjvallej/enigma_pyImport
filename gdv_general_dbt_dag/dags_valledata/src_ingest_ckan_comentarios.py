"""ETAPA INGEST CKAN COMENTARIOS (src_ingest_ckan_comentarios.py)

Extrae la tabla Postgres `comment` de cada instancia CKAN configurada y la
escribe en staging (GCS en Composer / data/raw en local).

Itera el arreglo environments.<env>.connections.ckan_comentarios (hasta 14).
Cada ítem es una conexión Airflow tipo postgres hacia un host/ruta distinto.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import csv
import io
import re
import sys
from typing import Any, Dict, List

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    get_composer_params,
    get_connection_list,
    get_postgres_connection,
    get_raw_root,
    load_config,
    require_config_value,
    safe_sql_ident,
    storage_join,
    write_text,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)


def _safe_filename_token(conn_id: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_\-]+", "_", conn_id.strip())
    return token or "conn"


def _fetch_comment_rows(
    conn_id: str,
    schema: str,
    table: str,
) -> tuple[list[str], list[tuple[Any, ...]]]:
    schema_id = safe_sql_ident(schema)
    table_id = safe_sql_ident(table)
    sql = f'SELECT * FROM "{schema_id}"."{table_id}"'
    conn = get_postgres_connection(conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return columns, rows


def _rows_to_csv(
    columns: list[str],
    rows: list[tuple[Any, ...]],
    *,
    source_conn_id: str,
    municipio: str,
) -> str:
    fieldnames = list(columns) + ["source_conn_id", "municipio", "ingested_at"]
    ingested_at = datetime.now(timezone.utc).isoformat()
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        record = {col: row[i] for i, col in enumerate(columns)}
        for key, value in list(record.items()):
            if value is None:
                record[key] = ""
            elif not isinstance(value, (str, int, float, bool)):
                record[key] = str(value)
        record["source_conn_id"] = source_conn_id
        record["municipio"] = municipio
        record["ingested_at"] = ingested_at
        writer.writerow(record)
    return buf.getvalue()


def ingest_one_connection(
    cfg: Dict[str, Any],
    item: Dict[str, str],
    *,
    schema: str,
    table: str,
    raw_subdir: str,
    filename_template: str,
) -> Dict[str, Any]:
    conn_id = item["conn_id"]
    municipio = item.get("municipio") or ""
    token = _safe_filename_token(conn_id)
    filename = filename_template.format(conn_id=token)
    dest = storage_join(get_raw_root(cfg), raw_subdir, filename)

    print(f"📥 [CKAN] Extrayendo {schema}.{table} | conn={conn_id} | municipio={municipio or '-'}")
    columns, rows = _fetch_comment_rows(conn_id, schema, table)
    csv_text = _rows_to_csv(columns, rows, source_conn_id=conn_id, municipio=municipio)
    write_text(dest, csv_text, cfg=cfg)
    print(f"✅ [CKAN] {len(rows)} filas -> {dest}")
    return {
        "conn_id": conn_id,
        "municipio": municipio,
        "rows": len(rows),
        "dest": dest,
        "status": "SUCCESS",
    }


_FIXTURE_POSITIVOS = (
    "Excelente servicio, muy recomendado.",
    "La información está clara y actualizada, gracias.",
    "Me ayudó mucho este dataset para mi trabajo.",
    "Muy buena calidad de los datos abiertos.",
    "El portal es fácil de usar y rápido.",
    "Gran trabajo del municipio publicando estos datos.",
    "Todo funcionó perfecto, experiencia positiva.",
    "Los metadatos están completos y bien documentados.",
    "Me encantó la visualización, muy útil.",
    "Respuesta oportuna y datos confiables.",
)
_FIXTURE_NEGATIVOS = (
    "La información está desactualizada y no sirve.",
    "Hay muchos errores en los registros.",
    "El archivo no descarga, pésimo servicio.",
    "Datos incompletos, falta la mitad de los campos.",
    "Muy lento el portal, una experiencia horrible.",
    "No recomiendo este dataset, está mal estructurado.",
    "La documentación es confusa y contradictoria.",
    "Encontré duplicados y valores nulos por todas partes.",
    "El enlace está roto desde hace semanas.",
    "Pésima calidad, no se puede usar para análisis.",
)
_FIXTURE_NEUTROS = (
    "¿Cada cuánto se actualiza este recurso?",
    "Necesito el diccionario de datos, ¿dónde está?",
    "Consulta sobre el significado de la columna codigo.",
    "¿Hay versión histórica de este dataset?",
    "Solicito el mismo archivo en formato CSV.",
    "Buenas tardes, ¿quién es el responsable del dataset?",
    "Quiero saber si incluye datos del 2024.",
    "¿Se puede filtrar por municipio?",
    "Información recibida, revisaré y comento después.",
    "¿Existe API para consultar estos datos?",
)
_FIXTURE_MUNICIPIOS = (
    ("ckan_pg_alcala", "alcala"),
    ("ckan_pg_buga", "buga"),
    ("ckan_pg_cali", "cali"),
    ("ckan_pg_palmira", "palmira"),
    ("ckan_pg_tulua", "tulua"),
    ("ckan_pg_yumbo", "yumbo"),
    ("ckan_pg_sevilla", "sevilla"),
    ("ckan_pg_cartago", "cartago"),
)
_FIXTURE_SUBJECTS = (
    "Calidad del dataset",
    "Actualización de datos",
    "Problema de descarga",
    "Consulta técnica",
    "Sugerencia de mejora",
    "Error en registros",
    "Documentación",
    "Acceso al recurso",
)


def generate_comment_records(n: int = 1000, seed: int = 42) -> list[dict[str, str]]:
    """Genera comentarios de prueba (mismo formato que la tabla CKAN `comment`)."""
    import random
    import uuid
    from datetime import timedelta

    rng = random.Random(seed)
    start = datetime(2022, 1, 1, tzinfo=timezone.utc)
    rows: list[dict[str, str]] = []
    
    # Crear pool de 4 thread_ids (datasets) por municipio
    thread_pool: dict[str, list[str]] = {}
    for _, m in _FIXTURE_MUNICIPIOS:
        thread_pool[m] = [
            str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{m}.dataset.{ds_idx}"))
            for ds_idx in range(1, 5)
        ]

    for i in range(1, n + 1):
        conn_id, municipio = _FIXTURE_MUNICIPIOS[i % len(_FIXTURE_MUNICIPIOS)]
        thread_id = rng.choice(thread_pool[municipio])
        roll = rng.random()
        if roll < 0.38:
            label, base = "POS", rng.choice(_FIXTURE_POSITIVOS)
        elif roll < 0.70:
            label, base = "NEG", rng.choice(_FIXTURE_NEGATIVOS)
        else:
            label, base = "NEU", rng.choice(_FIXTURE_NEUTROS)
        created = start + timedelta(days=rng.randint(0, 1200), hours=rng.randint(0, 23))
        rows.append(
            {
                "id": str(uuid.UUID(int=rng.getrandbits(128))),
                "thread_id": thread_id,
                "content": f"{base} (ref {i})",
                "subject": rng.choice(_FIXTURE_SUBJECTS),
                "author_id": f"user_{rng.randint(1, 200):03d}",
                "state": "approved",
                "created_at": created.isoformat(),
                "modified_at": (created + timedelta(hours=rng.randint(0, 72))).isoformat(),
                "source_conn_id": conn_id,
                "municipio": municipio,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "fixture_sentiment_hint": label,
            }
        )
    return rows


def write_comment_fixtures(rows: list[dict[str, str]], out_dir: Path) -> list[Path]:
    """Escribe CSVs locales `comment_<conn_id>.csv` (CLI / tests)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("comment_*.csv"):
        old.unlink()
    by_conn: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_conn.setdefault(row["source_conn_id"], []).append(row)
    fieldnames = [
        "id",
        "thread_id",
        "content",
        "subject",
        "author_id",
        "state",
        "created_at",
        "modified_at",
        "source_conn_id",
        "municipio",
        "ingested_at",
        "fixture_sentiment_hint",
    ]
    written: list[Path] = []
    for conn_id, items in by_conn.items():
        path = out_dir / f"comment_{conn_id}.csv"
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(items)
        written.append(path)
    return written


def _csv_text_for_conn(items: list[dict[str, str]]) -> str:
    fieldnames = [
        "id",
        "thread_id",
        "content",
        "subject",
        "author_id",
        "state",
        "created_at",
        "modified_at",
        "source_conn_id",
        "municipio",
        "ingested_at",
        "fixture_sentiment_hint",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(items)
    return buf.getvalue()


def _ingest_from_test_fixtures(cfg: Dict[str, Any], domain: Dict[str, Any]) -> Dict[str, Any]:
    """Genera fixtures embebidos (sin archivo extra) y los publica a staging."""
    n_records = int(domain.get("test_data_records") or 1000)
    seed = int(domain.get("test_data_seed") or 42)
    raw_subdir = str(require_config_value(domain, "raw_subdir"))

    rows = generate_comment_records(n_records, seed=seed)
    by_conn: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_conn.setdefault(row["source_conn_id"], []).append(row)

    results: List[Dict[str, Any]] = []
    for conn_id, items in by_conn.items():
        filename = f"comment_{_safe_filename_token(conn_id)}.csv"
        dest = storage_join(get_raw_root(cfg), raw_subdir, filename)
        write_text(dest, _csv_text_for_conn(items), cfg=cfg)
        municipio = items[0].get("municipio") if items else ""
        results.append(
            {
                "conn_id": conn_id,
                "municipio": municipio or "",
                "rows": len(items),
                "dest": dest,
                "status": "SUCCESS",
            }
        )

    total_rows = sum(int(r["rows"]) for r in results)
    print(
        f"🧪 [SRC_INGEST_CKAN_COMENTARIOS] Modo test_data | "
        f"registros={total_rows} | archivos={len(results)}"
    )
    return {
        "status": "SUCCESS",
        "mode": "test_data",
        "connections": len(results),
        "total_rows": total_rows,
        "errors": [],
        "results": results,
    }


def run_ingest_ckan_comentarios(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    domain = require_config_value(cfg, "ckan_comentarios")
    connections = get_connection_list(cfg, "ckan_comentarios")
    composer = get_composer_params(cfg)
    gcs_bucket = require_config_value(cfg, "gcs_bucket")

    schema = str(require_config_value(domain, "schema"))
    table = str(require_config_value(domain, "table"))
    raw_subdir = str(require_config_value(domain, "raw_subdir"))
    filename_template = str(
        domain.get("raw_filename_template") or "comment_{conn_id}.csv"
    )
    use_test_data = bool(domain.get("use_test_data"))

    print(
        f"🌐 [SRC_INGEST_CKAN_COMENTARIOS] Target={gcs_bucket} | "
        f"tabla={schema}.{table} | conexiones={len(connections)} | "
        f"use_test_data={use_test_data} | "
        f"Composer={composer['environment']} ({composer['location']})"
    )

    if not connections:
        if use_test_data:
            return _ingest_from_test_fixtures(cfg, domain)
        print(
            "⏭️ [SRC_INGEST_CKAN_COMENTARIOS] SKIPPED: "
            "environments.<env>.connections.ckan_comentarios está vacío. "
            "Agregue conexiones postgres o active ckan_comentarios.use_test_data: true."
        )
        return {"status": "SKIPPED", "connections": 0, "results": []}

    results: List[Dict[str, Any]] = []
    errors: List[str] = []
    for item in connections:
        try:
            results.append(
                ingest_one_connection(
                    cfg,
                    item,
                    schema=schema,
                    table=table,
                    raw_subdir=raw_subdir,
                    filename_template=filename_template,
                )
            )
        except Exception as exc:
            msg = f"{item.get('conn_id')}: {exc}"
            errors.append(msg)
            print(f"❌ [CKAN] {msg}")
            results.append(
                {
                    "conn_id": item.get("conn_id"),
                    "municipio": item.get("municipio") or "",
                    "rows": 0,
                    "dest": None,
                    "status": "ERROR",
                    "error": str(exc),
                }
            )

    total_rows = sum(int(r.get("rows") or 0) for r in results)
    status = "SUCCESS" if not errors else ("PARTIAL" if total_rows else "ERROR")
    print(
        f"✅ [SRC_INGEST_CKAN_COMENTARIOS] Fin | status={status} | "
        f"conexiones={len(connections)} | filas={total_rows} | errores={len(errors)}"
    )
    return {
        "status": status,
        "connections": len(connections),
        "total_rows": total_rows,
        "errors": errors,
        "results": results,
    }


if __name__ == "__main__":
    run_ingest_ckan_comentarios()

# Airflow DAG (Composer 3: airflow.sdk; Composer 2: airflow.decorators).
# Si Airflow no está instalado (CLI local), el módulo sigue siendo importable.
try:
    try:
        from airflow.sdk import dag as _af_dag, task as _af_task
    except ImportError:
        from airflow.decorators import dag as _af_dag, task as _af_task
except ImportError:
    _af_dag = _af_task = None

if _af_dag is not None:
    @_af_dag(
        dag_id="src_ingest_ckan_comentarios",
        description="Ingest comentarios CKAN (tabla comment) desde N Postgres",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "ckan", "comentarios", "ingest"],
        **get_airflow_dag_kwargs(),
    )
    def ingest_ckan_comentarios_dag():
        @_af_task(task_id="run_ingest_ckan_comentarios")
        def execute_ingest() -> Dict[str, Any]:
            return run_with_airflow_alarm(run_ingest_ckan_comentarios)

        execute_ingest()

    dag = ingest_ckan_comentarios_dag()
    DAG = dag  # noqa: N816 — token de descubrimiento de Airflow
