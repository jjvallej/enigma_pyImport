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
    "{us:Excellent service, highly recommended.} {es:Excelente servicio, muy recomendado.}",
    "{us:The information is clear and updated, thanks.} {es:La información está clara y actualizada, gracias.}",
    "{us:This dataset helped me a lot with my work.} {es:Me ayudó mucho este dataset para mi trabajo.}",
    "{us:Very good quality open data.} {es:Muy buena calidad de los datos abiertos.}",
    "{us:The portal is easy to use and fast.} {es:El portal es fácil de usar y rápido.}",
    "{us:Great job from the municipality publishing this data.} {es:Gran trabajo del municipio publicando estos datos.}",
    "{us:Everything worked perfectly, positive experience.} {es:Todo funcionó perfecto, experiencia positiva.}",
    "{us:Metadata is complete and well documented.} {es:Los metadatos están completos y bien documentados.}",
    "{us:Loved the visualization, very useful.} {es:Me encantó la visualización, muy útil.}",
    "{us:Timely response and reliable data.} {es:Respuesta oportuna y datos confiables.}",
)
_FIXTURE_NEGATIVOS = (
    "{us:The information is outdated and useless.} {es:La información está desactualizada y no sirve.}",
    "{us:There are many errors in the records.} {es:Hay muchos errores en los registros.}",
    "{us:The file does not download, terrible service.} {es:El archivo no descarga, pésimo servicio.}",
    "{us:Incomplete data, half of the fields are missing.} {es:Datos incompletos, falta la mitad de los campos.}",
    "{us:Portal is very slow, horrible experience.} {es:Muy lento el portal, una experiencia horrible.}",
    "{us:I do not recommend this dataset, poorly structured.} {es:No recomiendo este dataset, está mal estructurado.}",
    "{us:Documentation is confusing and contradictory.} {es:La documentación es confusa y contradictoria.}",
    "{us:I found duplicates and null values everywhere.} {es:Encontré duplicados y valores nulos por todas partes.}",
    "{us:The link has been broken for weeks.} {es:El enlace está roto desde hace semanas.}",
    "{us:Terrible quality, cannot be used for analysis.} {es:Pésima calidad, no se puede usar para análisis.}",
)
_FIXTURE_NEUTROS = (
    "{us:How often is this resource updated?} {es:¿Cada cuánto se actualiza este recurso?}",
    "{us:I need the data dictionary, where is it?} {es:Necesito el diccionario de datos, ¿dónde está?}",
    "{us:Inquiry about the meaning of column code.} {es:Consulta sobre el significado de la columna código.}",
    "{us:Is there a historical version of this dataset?} {es:¿Hay versión histórica de este dataset?}",
    "{us:I request the same file in CSV format.} {es:Solicito el mismo archivo en formato CSV.}",
    "{us:Good afternoon, who is responsible for the dataset?} {es:Buenas tardes, ¿quién es el responsable del dataset?}",
    "{us:I want to know if it includes 2024 data.} {es:Quiero saber si incluye datos del 2024.}",
    "{us:Can it be filtered by municipality?} {es:¿Se puede filtrar por municipio?}",
    "{us:Information received, will review and comment later.} {es:Información recibida, revisaré y comento después.}",
    "{us:Is there an API to query this data?} {es:¿Existe API para consultar estos datos?}",
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
_FIXTURE_DATASETS = (
    ("ds_cultivos_valle_001", "Dataset Cultivos Valle del Cauca"),
    ("ds_precios_sipsa_002", "Dataset Precios Mayoristas SIPSA"),
    ("ds_rendimiento_agri_003", "Dataset Rendimiento Agrícola Municipal"),
    ("ds_participacion_004", "Dataset Participación Ciudadana y Comentarios"),
)


def generate_comment_records(n: int = 1000, seed: int = 42) -> list[dict[str, str]]:
    """Genera comentarios de prueba con el esquema id, Id_dataset, nombre_dataset, comment, created ({us:...} {es:...})."""
    import random
    import uuid
    from datetime import timedelta

    rng = random.Random(seed)
    start = datetime(2022, 1, 1, tzinfo=timezone.utc)
    rows: list[dict[str, str]] = []

    for i in range(1, n + 1):
        conn_id, municipio = _FIXTURE_MUNICIPIOS[i % len(_FIXTURE_MUNICIPIOS)]
        ds_id, ds_name = _FIXTURE_DATASETS[i % len(_FIXTURE_DATASETS)]
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
                "Id_dataset": ds_id,
                "nombre_dataset": ds_name,
                "comment": base,
                "created": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "source_conn_id": conn_id,
                "municipio": municipio,
                "ingested_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "fixture_sentiment_hint": label,
            }
        )
    return rows


import json


def write_comment_fixtures(rows: list[dict[str, str]], out_dir: Path) -> list[Path]:
    """Escribe un único archivo JSON local `comentarios_staging.json` (CLI / tests)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("comment_*.csv"):
        old.unlink(missing_ok=True)
    for old in out_dir.glob("comment_*.json"):
        old.unlink(missing_ok=True)
    path = out_dir / "comentarios_staging.json"
    json_text = json.dumps(rows, indent=2, ensure_ascii=False)
    path.write_text(json_text, encoding="utf-8")
    return [path]


def _json_text_for_rows(items: list[dict[str, str]]) -> str:
    return json.dumps(items, indent=2, ensure_ascii=False)


def _ingest_from_test_fixtures(cfg: Dict[str, Any], domain: Dict[str, Any]) -> Dict[str, Any]:
    """Genera datos JSON de prueba (simulando respuesta de API) y publica `comentarios_staging.json` a staging."""
    n_records = int(domain.get("test_data_records") or 1000)
    seed = int(domain.get("test_data_seed") or 42)
    raw_subdir = str(require_config_value(domain, "raw_subdir"))

    rows = generate_comment_records(n_records, seed=seed)
    filename = "comentarios_staging.json"
    dest = storage_join(get_raw_root(cfg), raw_subdir, filename)
    write_text(dest, _json_text_for_rows(rows), cfg=cfg)

    print(
        f"🧪 [SRC_INGEST_CKAN_COMENTARIOS] Modo API JSON test_data | "
        f"registros={len(rows)} | archivo_json_staging={dest}"
    )
    return {
        "status": "SUCCESS",
        "mode": "test_data",
        "connections": 1,
        "total_rows": len(rows),
        "errors": [],
        "results": [
            {
                "conn_id": "api_json_fixtures",
                "municipio": "todos",
                "rows": len(rows),
                "dest": dest,
                "status": "SUCCESS",
            }
        ],
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
        domain.get("raw_filename_template") or "comment_{conn_id}.json"
    )
    use_test_data = bool(domain.get("use_test_data"))

    print(
        f"🌐 [SRC_INGEST_CKAN_COMENTARIOS] Ingesta API JSON -> Target={gcs_bucket} | "
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
            res = ingest_one_connection(
                cfg,
                item,
                schema=schema,
                table=table,
                raw_subdir=raw_subdir,
                filename_template=filename_template,
            )
            results.append(res)
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

    # Consolidar en un solo archivo JSON de staging para load
    total_rows = sum(int(r.get("rows") or 0) for r in results if r.get("status") == "SUCCESS")
    if total_rows > 0:
        single_staging_dest = storage_join(get_raw_root(cfg), raw_subdir, "comentarios_staging.json")
        all_frames = []
        for r in results:
            if r.get("dest") and r.get("status") == "SUCCESS":
                try:
                    local_p = materialize_local(r["dest"], cfg=cfg)
                    all_frames.append(pd.read_csv(local_p, dtype=str, keep_default_na=False))
                except Exception:
                    pass
        if all_frames:
            combined_df = pd.concat(all_frames, ignore_index=True, sort=False)
            records = combined_df.to_dict(orient="records")
            write_text(single_staging_dest, json.dumps(records, indent=2, ensure_ascii=False), cfg=cfg)
            print(f"📦 [INGEST API JSON] Consolidados {len(records)} registros JSON en staging: {single_staging_dest}")

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
