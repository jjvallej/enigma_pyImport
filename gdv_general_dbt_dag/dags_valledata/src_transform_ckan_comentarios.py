"""ETAPA TRANSFORM CKAN COMENTARIOS (src_transform_ckan_comentarios.py)

Lee Bronze `bronze_comentarios` y genera Silver `silver_comentarios` aplicando
pysentimiento (español) para obtener:
  - sentimiento: POS | NEG | NEU (o positivo/negativo/neutro según config)
  - score: probabilidad del sentimiento predicho
  - score_pos / score_neg / score_neu: probabilidades por clase

También limpia, normaliza y deduplica el dataset de comentarios.
"""

from __future__ import annotations

from pathlib import Path
import re
import sys
import unicodedata
from typing import Any, Dict

import pandas as pd

CURRENT_DIR = str(Path(__file__).resolve().parent)
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from src_common import (  # noqa: E402
    clean_str,
    normalize_municipio,
    get_bigquery_client,
    get_bq_table_ref,
    get_composer_params,
    get_connection_id,
    load_config,
    read_bq_dataframe,
    require_config_value,
    get_airflow_dag_kwargs,
    run_with_airflow_alarm,
)

TEXT_CANDIDATES = (
    "content",
    "comment",
    "comentario",
    "subject",
    "body",
    "text",
    "message",
    "texto",
)

SENTIMENT_LABELS = {
    "POS": "positivo",
    "NEG": "negativo",
    "NEU": "neutro",
}


def _parse_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _find_text_column(df: pd.DataFrame) -> str:
    cols = {str(c).strip().lower(): c for c in df.columns}
    for candidate in TEXT_CANDIDATES:
        if candidate in cols:
            return str(cols[candidate])
    raise KeyError(
        "No se encontró columna de texto de comentario. "
        f"Buscadas: {', '.join(TEXT_CANDIDATES)}. Presentes: {list(df.columns)}"
    )


def get_sentiment_analyzer(lang: str = "es"):
    """Crea el analizador de sentimiento de pysentimiento."""
    from pysentimiento import create_analyzer

    return create_analyzer(task="sentiment", lang=lang)


def _normalize_label(raw: Any) -> str:
    label = str(raw or "").strip().upper()
    if label in SENTIMENT_LABELS:
        return label
    lower = label.lower()
    if lower in {"pos", "positive", "positivo"}:
        return "POS"
    if lower in {"neg", "negative", "negativo"}:
        return "NEG"
    if lower in {"neu", "neutral", "neutro"}:
        return "NEU"
    return label or "NEU"


def predict_sentiment_row(text: Any, analyzer: Any) -> Dict[str, Any]:
    """Devuelve sentimiento + scores para un texto."""
    empty = {
        "sentimiento": "",
        "sentimiento_codigo": "",
        "score": None,
        "score_pos": None,
        "score_neg": None,
        "score_neu": None,
    }
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return empty
    value = str(text).strip()
    if not value:
        return empty

    pred = analyzer.predict(value)
    codigo = _normalize_label(getattr(pred, "output", ""))
    probas = getattr(pred, "probas", None) or {}
    # Normaliza claves de probas a POS/NEG/NEU
    norm_probas: Dict[str, float] = {}
    for key, val in dict(probas).items():
        norm_probas[_normalize_label(key)] = float(val)

    score_pos = float(norm_probas.get("POS", 0.0))
    score_neg = float(norm_probas.get("NEG", 0.0))
    score_neu = float(norm_probas.get("NEU", 0.0))
    score = float(norm_probas.get(codigo, max([score_pos, score_neg, score_neu], default=0.0)))

    return {
        "sentimiento": SENTIMENT_LABELS.get(codigo, codigo.lower()),
        "sentimiento_codigo": codigo,
        "score": score,
        "score_pos": score_pos,
        "score_neg": score_neg,
        "score_neu": score_neu,
    }


def apply_pysentimiento(
    df: pd.DataFrame,
    text_col: str,
    analyzer: Any | None = None,
    lang: str = "es",
) -> pd.DataFrame:
    """Aplica pysentimiento fila a fila y agrega columnas de sentimiento/score."""
    if df.empty:
        out = df.copy()
        for col in (
            "sentimiento",
            "sentimiento_codigo",
            "score",
            "score_pos",
            "score_neg",
            "score_neu",
        ):
            out[col] = pd.Series(dtype="object")
        return out

    model = analyzer or get_sentiment_analyzer(lang=lang)
    rows: list[Dict[str, Any]] = []
    total = len(df)
    for idx, text in enumerate(df[text_col].tolist(), start=1):
        if idx == 1 or idx % 50 == 0 or idx == total:
            print(f"🧠 [pysentimiento] {idx}/{total}", flush=True)
        rows.append(predict_sentiment_row(text, model))
    sentiment_df = pd.DataFrame(rows)
    return pd.concat([df.reset_index(drop=True), sentiment_df], axis=1)


def transform_comentarios(
    df: pd.DataFrame,
    *,
    analyzer: Any | None = None,
    lang: str = "es",
    apply_sentiment: bool = True,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    out = df.copy()
    out.columns = [str(c).strip().lower() for c in out.columns]

    text_col = _find_text_column(out)

    for col in out.columns:
        if out[col].dtype == object:
            if col == text_col or col in TEXT_CANDIDATES:
                out[col] = out[col].astype(str).fillna("").map(lambda s: s.strip())
            elif col in {"municipio", "source_conn_id", "state", "status"}:
                out[col] = out[col].map(clean_str)

    for date_col in ("created", "created_at", "modified", "modified_at", "timestamp", "ingested_at"):
        if date_col in out.columns:
            out[date_col] = _parse_ts(out[date_col])

    if "source_conn_id" not in out.columns:
        out["source_conn_id"] = ""
    if "municipio" not in out.columns:
        out["municipio"] = ""

    # Quitar explícitamente el campo id_municipio de gold_comentarios_sentimiento
    if "id_municipio" in out.columns:
        out = out.drop(columns=["id_municipio"])

    if "id" in out.columns:
        out = out.drop_duplicates(subset=["source_conn_id", "id"], keep="last")
    else:
        out = out.drop_duplicates(keep="last")

    out = out.reset_index(drop=True)
    if apply_sentiment:
        out = apply_pysentimiento(out, text_col=text_col, analyzer=analyzer, lang=lang)

    # Quitar explícitamente el campo id_municipio de gold_comentarios_sentimiento
    if "id_municipio" in out.columns:
        out = out.drop(columns=["id_municipio"])

    return out


def build_gold_comentarios_consolidado(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Genera el dataset consolidado Gold agrupado por municipio, id_dataset y nombre_dataset (9 campos oficiales)."""
    if silver_df.empty:
        return pd.DataFrame(
            columns=[
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
        )

    df = silver_df.copy()
    if "thread_id" in df.columns:
        df["id_dataset"] = df["thread_id"].fillna(df.get("subject", ""))
    elif "subject" in df.columns:
        df["id_dataset"] = df["subject"]
    else:
        df["id_dataset"] = "dataset_default"

    if "subject" in df.columns:
        df["nombre_dataset"] = df["subject"].fillna(df.get("thread_id", ""))
    elif "thread_id" in df.columns:
        df["nombre_dataset"] = df["thread_id"]
    else:
        df["nombre_dataset"] = "dataset_default"

    def get_predominant(pos: int, neg: int, neu: int) -> str:
        if pos >= neg and pos >= neu:
            return "POS"
        if neg >= pos and neg >= neu:
            return "NEG"
        return "NEU"

    def _agg_group(g: pd.DataFrame) -> pd.Series:
        tot = int(len(g))
        pos = int((g.get("sentimiento_codigo") == "POS").sum())
        neg = int((g.get("sentimiento_codigo") == "NEG").sum())
        neu = int((g.get("sentimiento_codigo") == "NEU").sum())

        scores = pd.to_numeric(g.get("score"), errors="coerce")
        mean_score = float(scores.mean()) if not scores.dropna().empty else 0.0

        return pd.Series(
            {
                "total_comentarios": tot,
                "positivos": pos,
                "negativos": neg,
                "neutros": neu,
                "confianza_promedio": round(mean_score, 4),
                "emocion_predominante": get_predominant(pos, neg, neu),
            }
        )

    gold = df.groupby(["municipio", "id_dataset", "nombre_dataset"]).apply(_agg_group).reset_index()
    return gold


def run_transform_ckan_comentarios(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = config or load_config()
    paths_cfg = require_config_value(cfg, "paths")
    bq_cfg = require_config_value(cfg, "bigquery")
    domain = require_config_value(cfg, "ckan_comentarios")
    composer = get_composer_params(cfg)
    gcp_conn = get_connection_id(cfg, "google_cloud_default")

    bronze_ref = get_bq_table_ref(cfg, "bronze", "comentarios_bronze")
    silver_ref = get_bq_table_ref(cfg, "silver", "comentarios_silver")
    gold_ref = get_bq_table_ref(cfg, "silver", "gold_comentarios_sentimiento")
    output_csv = Path(require_config_value(paths_cfg, "ckan_comentarios_silver_csv"))
    gold_csv = Path(paths_cfg.get("ckan_comentarios_gold_csv") or "data/gold_comentarios_sentimiento.csv")
    bronze_csv = Path(require_config_value(paths_cfg, "ckan_comentarios_csv"))
    lang = str(domain.get("sentiment_lang") or "es")

    print(
        f"🔄 [SRC_TRANSFORM_CKAN_COMENTARIOS] {bronze_ref} -> {silver_ref} / {gold_ref} | "
        f"pysentimiento lang={lang} | "
        f"Composer={composer['environment']} ({composer['location']}) | conn={gcp_conn}"
    )

    try:
        df = read_bq_dataframe(cfg, bronze_ref)
        source = bronze_ref
    except Exception as exc:
        print(f"ℹ️ [TRANSFORM] No se pudo leer BQ ({exc}); usando CSV local si existe.")
        if not bronze_csv.exists():
            print(
                "⏭️ [SRC_TRANSFORM_CKAN_COMENTARIOS] SKIPPED: sin Bronze ni CSV. "
                "Ejecute ingest + load primero."
            )
            return {"status": "SKIPPED", "rows": 0}
        df = pd.read_csv(bronze_csv, dtype=str, keep_default_na=False)
        source = str(bronze_csv)

    silver = transform_comentarios(df, lang=lang)
    gold = build_gold_comentarios_consolidado(silver)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    gold_csv.parent.mkdir(parents=True, exist_ok=True)
    for col in silver.columns:
        if pd.api.types.is_datetime64_any_dtype(silver[col]):
            silver[col] = silver[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    silver.to_csv(output_csv, index=False)
    gold.to_csv(gold_csv, index=False)

    result: Dict[str, Any] = {
        "source": source,
        "rows_in": int(len(df)),
        "rows_out": int(len(silver)),
        "rows_silver": int(len(silver)),
        "rows_gold": int(len(gold)),
        "output": str(output_csv),
        "output_gold": str(gold_csv),
        "bigquery_table": silver_ref,
        "bigquery_gold_table": gold_ref,
    }

    try:
        from google.cloud import bigquery

        project_id = require_config_value(bq_cfg, "project_id")
        client = get_bigquery_client(cfg, project_id, require_config_value(bq_cfg, "location"))
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        with open(output_csv, "rb") as sf:
            job = client.load_table_from_file(sf, silver_ref, job_config=job_config)
        job.result()
        # Crea/Actualiza la vista Gold en BigQuery agrupada por municipio, id_dataset y nombre_dataset (9 campos oficiales)
        view_query = f"""
        CREATE OR REPLACE VIEW `{gold_ref}` AS
        SELECT
            municipio,
            COALESCE(NULLIF(thread_id, ''), subject) AS id_dataset,
            COALESCE(NULLIF(subject, ''), thread_id) AS nombre_dataset,
            COUNT(1) AS total_comentarios,
            COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo') AS positivos,
            COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo') AS negativos,
            COUNTIF(UPPER(sentimiento_codigo) = 'NEU' OR LOWER(sentimiento) = 'neutro') AS neutros,
            ROUND(AVG(score), 4) AS confianza_promedio,
            CASE 
                WHEN COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo') >= COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo')
                 AND COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo') >= COUNTIF(UPPER(sentimiento_codigo) = 'NEU' OR LOWER(sentimiento) = 'neutro') THEN 'POS'
                WHEN COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo') >= COUNTIF(UPPER(sentimiento_codigo) = 'POS' OR LOWER(sentimiento) = 'positivo')
                 AND COUNTIF(UPPER(sentimiento_codigo) = 'NEG' OR LOWER(sentimiento) = 'negativo') >= COUNTIF(UPPER(sentimiento_codigo) = 'NEU' OR LOWER(sentimiento) = 'neutro') THEN 'NEG'
                ELSE 'NEU'
            END AS emocion_predominante
        FROM `{silver_ref}`
        GROUP BY municipio, id_dataset, nombre_dataset
        """
        client.query(view_query).result()
        result["status"] = "SUCCESS"
    except Exception as exc:
        print(f"ℹ️ [Modo Simulación Transform BQ comentarios/pysentimiento] {exc}")
        result["status"] = "SIMULATED"

    print(
        f"✅ [SRC_TRANSFORM_CKAN_COMENTARIOS] silver_comentarios listo | "
        f"in={result['rows_in']} silver={result['rows_silver']} gold={result['rows_gold']} | status={result['status']}"
    )
    return result


if __name__ == "__main__":
    run_transform_ckan_comentarios()

# Airflow DAG (Composer 3: airflow.sdk; Composer 2: airflow.decorators).
try:
    try:
        from airflow.sdk import dag as _af_dag, task as _af_task
    except ImportError:
        from airflow.decorators import dag as _af_dag, task as _af_task
except ImportError:
    _af_dag = _af_task = None

if _af_dag is not None:
    from datetime import datetime

    @_af_dag(
        dag_id="src_transform_ckan_comentarios",
        description="Transform bronze_comentarios -> silver_comentarios (pysentimiento)",
        start_date=datetime(2000, 1, 1),
        schedule=None,
        catchup=False,
        tags=["valledata", "ckan", "comentarios", "transform", "pysentimiento"],
        **get_airflow_dag_kwargs(),
    )
    def transform_ckan_comentarios_dag():
        @_af_task(task_id="run_transform_ckan_comentarios")
        def execute_transform() -> Dict[str, Any]:
            return run_with_airflow_alarm(run_transform_ckan_comentarios)

        execute_transform()

    dag = transform_ckan_comentarios_dag()
    DAG = dag  # noqa: N816 — token de descubrimiento de Airflow
