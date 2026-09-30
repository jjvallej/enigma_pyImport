"""Genera archivo SQL para BigQuery:
1. DDL para tabla valledata.silver_comentarios
2. DML con 1.000 INSERT INTO para valledata.silver_comentarios desde data/silver_comentarios.csv
3. DDL para vista valledata.gold_comentarios_sentimiento (sin id_municipio)
"""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_CSV = PROJECT_ROOT / "data" / "silver_comentarios.csv"
OUTPUT_SQL = PROJECT_ROOT / "scripts" / "bigquery_comentarios_silver_and_gold.sql"

PROJECT_ID = "datagov-477214"
DATASET_ID = "valledata"
TABLE_SILVER = f"`{PROJECT_ID}.{DATASET_ID}.silver_comentarios`"
VIEW_GOLD = f"`{PROJECT_ID}.{DATASET_ID}.gold_comentarios_sentimiento`"


def _sql_str(val: str | None) -> str:
    if val is None or pd.isna(val) or val == "":
        return "NULL"
    # Escapar comillas simples para SQL
    clean = str(val).replace("'", "''")
    return f"'{clean}'"


def _sql_num(val: float | int | None) -> str:
    if val is None or pd.isna(val) or val == "":
        return "NULL"
    return str(float(val))


def generate_sql() -> Path:
    if not SILVER_CSV.exists():
        raise FileNotFoundError(f"No existe {SILVER_CSV}. Ejecute el pipeline de transform primero.")

    df = pd.read_csv(SILVER_CSV, dtype=str, keep_default_na=False)

    lines: list[str] = [
        f"-- ==============================================================================",
        f"-- CONSULTAS BIGQUERY: TABLA SILVER_COMENTARIOS Y VISTA GOLD_COMENTARIOS_SENTIMIENTO",
        f"-- Proyecto: {PROJECT_ID} | Dataset: {DATASET_ID}",
        f"-- Total de registros generados: {len(df)}",
        f"-- ==============================================================================",
        "",
        f"-- 1. CREACIÓN DE LA TABLA SILVER_COMENTARIOS (REEMPLAZA CUALQUIER ESQUEMA PREVIO INCOMPATIBLE)",
        f"CREATE OR REPLACE TABLE {TABLE_SILVER} (",
        "    id STRING,",
        "    thread_id STRING,",
        "    content STRING,",
        "    subject STRING,",
        "    author_id STRING,",
        "    state STRING,",
        "    created_at TIMESTAMP,",
        "    modified_at TIMESTAMP,",
        "    source_conn_id STRING,",
        "    municipio STRING,",
        "    ingested_at TIMESTAMP,",
        "    fixture_sentiment_hint STRING,",
        "    source_file STRING,",
        "    sentimiento STRING,",
        "    sentimiento_codigo STRING,",
        "    score FLOAT64,",
        "    score_pos FLOAT64,",
        "    score_neg FLOAT64,",
        "    score_neu FLOAT64",
        ");",
        "",
        f"-- 2. INSERCIÓN DE DATOS (DML INSERT INTO)",
        f"INSERT INTO {TABLE_SILVER} (",
        "    id, thread_id, content, subject, author_id, state, created_at, modified_at,",
        "    source_conn_id, municipio, ingested_at, fixture_sentiment_hint, source_file,",
        "    sentimiento, sentimiento_codigo, score, score_pos, score_neg, score_neu",
        ") VALUES",
    ]

    value_rows: list[str] = []
    for _, row in df.iterrows():
        r_id = _sql_str(row.get("id"))
        r_thread = _sql_str(row.get("thread_id"))
        r_content = _sql_str(row.get("content"))
        r_subject = _sql_str(row.get("subject"))
        r_author = _sql_str(row.get("author_id"))
        r_state = _sql_str(row.get("state"))
        r_created = f"TIMESTAMP({_sql_str(row.get('created_at'))})" if row.get("created_at") else "NULL"
        r_modified = f"TIMESTAMP({_sql_str(row.get('modified_at'))})" if row.get("modified_at") else "NULL"
        r_conn = _sql_str(row.get("source_conn_id"))
        r_muni = _sql_str(row.get("municipio"))
        r_ingest = f"TIMESTAMP({_sql_str(row.get('ingested_at'))})" if row.get("ingested_at") else "NULL"
        r_hint = _sql_str(row.get("fixture_sentiment_hint"))
        r_file = _sql_str(row.get("source_file"))
        r_sent = _sql_str(row.get("sentimiento"))
        r_code = _sql_str(row.get("sentimiento_codigo"))
        r_score = _sql_num(row.get("score"))
        r_spos = _sql_num(row.get("score_pos"))
        r_sneg = _sql_num(row.get("score_neg"))
        r_sneu = _sql_num(row.get("score_neu"))

        row_str = (
            f"  ({r_id}, {r_thread}, {r_content}, {r_subject}, {r_author}, {r_state}, "
            f"{r_created}, {r_modified}, {r_conn}, {r_muni}, {r_ingest}, {r_hint}, "
            f"{r_file}, {r_sent}, {r_code}, {r_score}, {r_spos}, {r_sneg}, {r_sneu})"
        )
        value_rows.append(row_str)

    lines.append(",\n".join(value_rows) + ";")
    lines.extend([
        "",
        f"-- 3. CREACIÓN DE LA VISTA GOLD_COMENTARIOS_SENTIMIENTO (CONSOLIDADO POR MUNICIPIO, ID_DATASET Y NOMBRE_DATASET - 9 CAMPOS)",
        f"CREATE OR REPLACE VIEW {VIEW_GOLD} AS",
        "SELECT",
        "    municipio,",
        "    COALESCE(NULLIF(thread_id, ''), subject) AS id_dataset,",
        "    COALESCE(NULLIF(subject, ''), thread_id) AS nombre_dataset,",
        "    COUNT(1) AS total_comentarios,",
        "    COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo') AS positivos,",
        "    COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo') AS negativos,",
        "    COUNTIF(UPPER(sentimiento_codigo) = 'NEU' OR LOWER(sentimiento) = 'neutro') AS neutros,",
        "    ROUND(AVG(score), 4) AS confianza_promedio,",
        "    CASE ",
        "        WHEN COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo') >= COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo')",
        "         AND COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo') >= COUNTIF(UPPER(sentimiento_codigo) = 'NEU' OR LOWER(sentimiento) = 'neutro') THEN 'POS'",
        "        WHEN COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo') >= COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo')",
        "         AND COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo') >= COUNTIF(UPPER(sentimiento_codigo) = 'NEU' OR LOWER(sentimiento) = 'neutro') THEN 'NEG'",
        "        ELSE 'NEU'",
        "    END AS emocion_predominante",
        f"FROM {TABLE_SILVER}",
        "GROUP BY municipio, id_dataset, nombre_dataset;",
        ""
    ])

    OUTPUT_SQL.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_SQL.write_text("\n".join(lines), encoding="utf-8")
    print(f"✅ Generado script SQL BigQuery en: {OUTPUT_SQL} ({len(df)} registros)")
    return OUTPUT_SQL


if __name__ == "__main__":
    generate_sql()
