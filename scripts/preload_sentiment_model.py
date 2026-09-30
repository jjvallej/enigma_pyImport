"""Script de precarga (warmup) para el modelo de sentimiento pysentimiento (NLP).

Descarga e inicializa en caché local/Airflow los pesos del modelo RoBERTa (`pysentimiento/robertuito-sentiment-analysis`)
para evitar retrasos de descarga HTTP en la primera ejecución del DAG `src_transform_ckan_comentarios`.

Uso:
    python scripts/preload_sentiment_model.py
"""

from __future__ import annotations

import sys


def main() -> None:
    import os
    from pathlib import Path

    if "HF_HOME" not in os.environ:
        composer_gcs_data = Path("/home/airflow/gcs/data")
        if composer_gcs_data.exists():
            hf_cache = composer_gcs_data / "huggingface"
            hf_cache.mkdir(parents=True, exist_ok=True)
            os.environ["HF_HOME"] = str(hf_cache)
            os.environ["TRANSFORMERS_CACHE"] = str(hf_cache)
            print(f"📦 [PRELOAD] Asignado caché de GCS Composer: {hf_cache}", flush=True)

    print("⏳ [PRELOAD] Precargando el modelo de análisis de sentimiento (pysentimiento)...", flush=True)
    try:
        from pysentimiento import create_analyzer

        analyzer = create_analyzer(task="sentiment", lang="es")
        sample_res = analyzer.predict("Excelente servicio y atención ciudadana")
        print(
            f"✅ [PRELOAD] Modelo precargado exitosamente en caché. Prueba: '{sample_res.output}'",
            flush=True,
        )
    except Exception as exc:
        print(f"❌ [PRELOAD] Error al descargar/precargar el modelo: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
