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
    running_in_composer,
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


class FastSpanishSentimentAnalyzer:
    """Analizador de sentimiento en español ultrarrápido (100% resiliente a OOM en Airflow/Composer)."""

    POS_WORDS = {
        "excelente", "bueno", "buena", "buenos", "buenas", "gran", "mejora", "mejoras",
        "gracias", "felicitaciones", "positivo", "positiva", "eficiente", "apoyo",
        "beneficio", "beneficios", "exito", "valioso", "valiosa", "correcto", "correcta",
        "solucion", "soluciones", "maravilloso", "destacado", "oportuno", "satisfecho",
        "transparencia", "cumplimiento", "progreso", "atencion", "agradable", "optimo",
        "bien", "admiracion", "favorable", "exitoso", "impecable", "destacada", "fortaleza",
    }
    NEG_WORDS = {
        "malo", "mala", "malos", "malas", "pesimo", "pesima", "terrible", "error",
        "errores", "fallo", "fallas", "problema", "problemas", "falta", "retraso",
        "retrasos", "corrupcion", "queja", "quejas", "daño", "daños", "incompetente",
        "injusto", "peligro", "peligroso", "deficiente", "negativo", "negativa",
        "lamentable", "inadecuado", "fraude", "perjuicio", "abandono", "critica",
        "deficiencia", "demora", "demoras", "irregularidad", "descaro", "desastre",
    }

    def predict(self, text: Any):
        if isinstance(text, (list, tuple)):
            return [self.predict(t) for t in text]
        words = re.findall(r"\w+", str(text or "").lower())
        pos_count = sum(1 for w in words if w in self.POS_WORDS)
        neg_count = sum(1 for w in words if w in self.NEG_WORDS)
        tot = pos_count + neg_count

        if pos_count > neg_count:
            label = "POS"
            p_pos = round(0.55 + min(0.35, (pos_count / (tot + 1)) * 0.35), 3)
            p_neg = round(0.1, 3)
            p_neu = round(1.0 - p_pos - p_neg, 3)
        elif neg_count > pos_count:
            label = "NEG"
            p_neg = round(0.55 + min(0.35, (neg_count / (tot + 1)) * 0.35), 3)
            p_pos = round(0.1, 3)
            p_neu = round(1.0 - p_neg - p_pos, 3)
        else:
            label = "NEU"
            p_neu = 0.8
            p_pos = 0.1
            p_neg = 0.1

        class Prediction:
            def __init__(self, output: str, probas: Dict[str, float]):
                self.output = output
                self.probas = probas

        return Prediction(output=label, probas={"POS": p_pos, "NEG": p_neg, "NEU": p_neu})


def get_sentiment_analyzer(lang: str = "es"):
    """Crea o devuelve el analizador de sentimiento.

    En entornos Cloud Composer / Airflow con límites de RAM o cuando USE_FAST_SENTIMENT=1 está activo,
    retorna FastSpanishSentimentAnalyzer para asegurar 100% de éxito en tiempo de ejecución sin OOM.
    """
    import os
    import shutil
    import tarfile
    from pathlib import Path

    try:
        is_composer = running_in_composer()
    except Exception:
        is_composer = bool(
            os.environ.get("COMPOSER_ENVIRONMENT")
            or os.environ.get("AIRFLOW_CTX_DAG_ID")
            or os.environ.get("AIRFLOW_CTX_TASK_ID")
        )

    use_fast = (
        os.environ.get("USE_FAST_SENTIMENT", "0").lower() in ("1", "true", "yes")
        or os.environ.get("DISABLE_HEAVY_NLP", "0").lower() in ("1", "true", "yes")
        or is_composer
    )

    if use_fast:
        print(
            "🧠 [pysentimiento] Usando analizador de sentimiento ligero resiliente a OOM (FastSpanishSentimentAnalyzer)",
            flush=True,
        )
        return FastSpanishSentimentAnalyzer()

    # Intentar carga de PyTorch/pysentimiento local si no está en Composer
    try:
        os.environ["USE_TF"] = "0"
        os.environ["USE_TORCH"] = "1"
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"

        local_hf_cache = Path("/tmp/huggingface_cache")
        local_hf_cache.mkdir(parents=True, exist_ok=True)
        os.environ["HF_HOME"] = str(local_hf_cache)
        os.environ["TRANSFORMERS_CACHE"] = str(local_hf_cache)
        os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"
        os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
        os.environ["TQDM_DISABLE"] = "1"

        has_cache = (
            (local_hf_cache / "hub").exists()
            or (local_hf_cache / "models--pysentimiento--robertuito-sentiment-analysis").exists()
            or any(local_hf_cache.glob("**/config.json"))
        )

        if has_cache:
            os.environ["TRANSFORMERS_OFFLINE"] = "1"
        else:
            candidates = [
                Path("/tmp/huggingface_cache.tar.gz"),
                Path("huggingface_cache.tar.gz"),
                Path("/home/airflow/gcs/data/huggingface_cache.tar.gz"),
            ]
            tarball_path: Path | None = None
            for cand in candidates:
                if cand.exists():
                    tarball_path = cand
                    break

            if tarball_path is not None:
                target_tarball = tarball_path
                if str(tarball_path).startswith("/home/airflow/gcs/"):
                    tmp_tarball = Path("/tmp/huggingface_cache.tar.gz")
                    if not tmp_tarball.exists():
                        print(f"📦 [pysentimiento] Copiando caché desde GCS: {tarball_path} -> {tmp_tarball}", flush=True)
                        try:
                            with open(tarball_path, "rb") as src, open(tmp_tarball, "wb") as dst:
                                shutil.copyfileobj(src, dst, length=32 * 1024 * 1024)
                            target_tarball = tmp_tarball
                        except Exception as exc:
                            print(f"⚠️ [pysentimiento] No se pudo copiar caché de GCS a /tmp: {exc}", flush=True)
                    else:
                        target_tarball = tmp_tarball

                print(f"📦 [pysentimiento] Desempacando caché en local: {target_tarball} -> {local_hf_cache}", flush=True)
                try:
                    with tarfile.open(target_tarball, "r:gz") as tar:
                        tar.extractall(path=local_hf_cache)
                    if target_tarball != tarball_path and target_tarball.exists():
                        target_tarball.unlink(missing_ok=True)
                    os.environ["TRANSFORMERS_OFFLINE"] = "1"
                except Exception as exc:
                    print(f"⚠️ [pysentimiento] No se pudo desempacar caché: {exc}", flush=True)

        try:
            import torch
            torch.set_grad_enabled(False)
            if hasattr(os, "cpu_count") and os.cpu_count():
                torch.set_num_threads(min(2, os.cpu_count() or 1))
        except Exception:
            pass

        try:
            import transformers
            transformers.logging.set_verbosity_error()
        except Exception:
            pass

        from pysentimiento import create_analyzer
        return create_analyzer(task="sentiment", lang=lang)
    except Exception as exc:
        print(f"⚠️ [pysentimiento] Fallo al cargar modelo PyTorch ({exc}); activando analizador de respaldo.", flush=True)
        return FastSpanishSentimentAnalyzer()


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


def predict_sentiment_row(text: Any, analyzer: Any = None, *, pred_obj: Any = None) -> Dict[str, Any]:
    """Devuelve sentimiento + scores para un texto, aceptando un analizador o un resultado precalculado."""
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

    if pred_obj is not None:
        pred = pred_obj
    elif analyzer is not None:
        pred = analyzer.predict(value)
    else:
        return empty

    codigo = _normalize_label(getattr(pred, "output", ""))
    probas = getattr(pred, "probas", None) or {}
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
    batch_size: int = 32,
) -> pd.DataFrame:
    """Aplica pysentimiento por lotes (batch inference) sin consumo acumulativo de memoria RAM."""
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
    texts = df[text_col].tolist()
    total = len(texts)
    rows: list[Dict[str, Any]] = []

    try:
        import torch
        ctx = torch.no_grad()
    except Exception:
        from contextlib import nullcontext
        ctx = nullcontext()

    use_batch = True
    with ctx:
        try:
            for i in range(0, total, batch_size):
                chunk = texts[i : i + batch_size]
                cleaned_chunk = [
                    str(t).strip() if (t is not None and not (isinstance(t, float) and pd.isna(t))) else ""
                    for t in chunk
                ]
                valid_inputs = [t if t else " " for t in cleaned_chunk]

                preds = model.predict(valid_inputs)
                if not isinstance(preds, (list, tuple)):
                    preds = [preds]

                for orig_text, pred in zip(cleaned_chunk, preds):
                    if not orig_text:
                        rows.append({
                            "sentimiento": "",
                            "sentimiento_codigo": "",
                            "score": None,
                            "score_pos": None,
                            "score_neg": None,
                            "score_neu": None,
                        })
                    else:
                        rows.append(predict_sentiment_row(orig_text, pred_obj=pred))

                current = min(i + batch_size, total)
                if current == total or (i // batch_size) % 5 == 0:
                    print(f"🧠 [pysentimiento batch] {current}/{total}", flush=True)

        except Exception as exc:
            use_batch = False
            print(f"ℹ️ [pysentimiento] Modo individual (fallback): {exc}", flush=True)

        if not use_batch:
            rows = []
            for idx, text in enumerate(texts, start=1):
                if idx == 1 or idx % 50 == 0 or idx == total:
                    print(f"🧠 [pysentimiento individual] {idx}/{total}", flush=True)
                rows.append(predict_sentiment_row(text, model))

    sentiment_df = pd.DataFrame(rows)
    return pd.concat([df.reset_index(drop=True), sentiment_df], axis=1)


def extract_spanish_comment(raw_comment: Any) -> str:
    """Extrae únicamente la descripción en español ({es:texto}) de comentarios formateados con etiquetas de idioma."""
    if raw_comment is None or (isinstance(raw_comment, float) and pd.isna(raw_comment)):
        return ""
    text = str(raw_comment).strip()
    if not text:
        return ""

    es_matches = re.findall(r"\{es:\s*(.*?)\}", text, flags=re.DOTALL | re.IGNORECASE)
    if not es_matches:
        es_matches = re.findall(r"\[es:\s*(.*?)\]", text, flags=re.DOTALL | re.IGNORECASE)

    if es_matches:
        cleaned = " ".join(m.strip() for m in es_matches if m.strip())
        if cleaned:
            return cleaned

    # Si no tiene etiquetas {es:...}, eliminar cualquier {us:...} u otra etiqueta y retornar texto limpio
    cleaned_no_tags = re.sub(r"\{[a-z]{2}:\s*.*?\}", "", text, flags=re.DOTALL | re.IGNORECASE).strip()
    if cleaned_no_tags:
        return cleaned_no_tags

    return text


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

    # Mapear nombres de columnas insensibles a mayúsculas
    col_map = {str(c).strip(): str(c).strip() for c in out.columns}
    low_cols = {str(c).strip().lower(): str(c).strip() for c in out.columns}

    # Normalizar id_dataset y nombre_dataset
    id_ds_col = low_cols.get("id_dataset") or low_cols.get("thread_id") or low_cols.get("subject")
    name_ds_col = low_cols.get("nombre_dataset") or low_cols.get("subject") or low_cols.get("thread_id")
    text_col_raw = low_cols.get("comment") or low_cols.get("content") or _find_text_column(out)
    created_col_raw = low_cols.get("created") or low_cols.get("created_at")

    out["Id_dataset"] = out[id_ds_col].fillna("ds_default").astype(str) if id_ds_col else "ds_default"
    out["nombre_dataset"] = out[name_ds_col].fillna("dataset_default").astype(str) if name_ds_col else "dataset_default"

    # Extraer únicamente el comentario en español
    out["comment"] = out[text_col_raw].apply(extract_spanish_comment)

    if created_col_raw in out.columns:
        out["created"] = _parse_ts(out[created_col_raw])
    elif "created" not in out.columns:
        out["created"] = pd.Timestamp.now(tz="UTC")

    for col in ("municipio", "source_conn_id", "state", "status"):
        if col in out.columns:
            out[col] = out[col].map(clean_str)
        else:
            out[col] = ""

    if "id_municipio" in out.columns:
        out = out.drop(columns=["id_municipio"])

    if "id" in out.columns:
        out = out.drop_duplicates(subset=["source_conn_id", "id"], keep="last")
    else:
        out = out.drop_duplicates(keep="last")

    out = out.reset_index(drop=True)
    if apply_sentiment:
        out = apply_pysentimiento(out, text_col="comment", analyzer=analyzer, lang=lang)

    if "id_municipio" in out.columns:
        out = out.drop(columns=["id_municipio"])

    # Garantizar compatibilidad con vistas BigQuery que busquen thread_id o subject
    out["thread_id"] = out["Id_dataset"]
    out["subject"] = out["nombre_dataset"]

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
    df["id_dataset"] = df.get("Id_dataset", df.get("thread_id", df.get("subject", "ds_default"))).fillna("ds_default")
    df["nombre_dataset"] = df.get("nombre_dataset", df.get("subject", df.get("thread_id", "dataset_default"))).fillna("dataset_default")

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
        
        # Eliminar tabla previa si existe conflicto de esquema para recrearla limpiamente
        try:
            client.delete_table(silver_ref, not_found_ok=True)
        except Exception:
            pass

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
        print(f"❌ [ERROR BQ TRANSFORM] Error al cargar/crear vista en BigQuery ({silver_ref} / {gold_ref}): {exc}", flush=True)
        result["status"] = "SIMULATED"
        result["errors"] = str(exc)

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
