"""Script para entrenar y cargar los modelos de predicción y rendimiento agrícola en BigQuery `valledata`.

Recursos/Modelos en BigQuery:
1. datagov-477214.valledata.gold_modelo_pronostico_produccion (MODEL BQML de tipo ARIMA_PLUS para producción en Toneladas)
2. datagov-477214.valledata.gold_pronostico_produccion (TABLE con pronósticos de producción a 3 años vía ML.FORECAST)
3. datagov-477214.valledata.gold_modelo_rendimiento (MODEL BQML de tipo ARIMA_PLUS para rendimiento en Ton/Ha)
4. datagov-477214.valledata.gold_pronostico_rendimiento (TABLE con pronósticos de rendimiento a 3 años vía ML.FORECAST)
5. datagov-477214.valledata.gold_rendimiento (TABLE depurada con rendimientos e índice ONI)
"""

from pathlib import Path
import pandas as pd
import numpy as np
from google.cloud import bigquery

PROJECT_ID = "datagov-477214"
DATASET_ID = "valledata"

MODEL_PRONOSTICO_PRODUCCION = f"{PROJECT_ID}.{DATASET_ID}.gold_modelo_pronostico_produccion"
TABLE_PRONOSTICO_PRODUCCION = f"{PROJECT_ID}.{DATASET_ID}.gold_pronostico_produccion"

MODEL_RENDIMIENTO = f"{PROJECT_ID}.{DATASET_ID}.gold_modelo_rendimiento"
TABLE_PRONOSTICO_RENDIMIENTO = f"{PROJECT_ID}.{DATASET_ID}.gold_pronostico_rendimiento"
TABLE_RENDIMIENTO = f"{PROJECT_ID}.{DATASET_ID}.gold_rendimiento"

data_sources = [
    Path("data/silver_agri_modelo_base.csv"),
    Path("data/dataset_prediccion_rentabilidad.csv"),
    Path("data/dataset_consolidado_valle.csv")
]
CSV_MODELO_BASE = next((p for p in data_sources if p.exists()), Path("data/dataset_consolidado_valle.csv"))

def preparar_gold_modelo_rendimiento(data_path: Path) -> pd.DataFrame:
    """Carga y prepara el dataset de rendimiento agrícola para gold_rendimiento."""
    print(f"--> Preparando gold_rendimiento desde: {data_path}")
    df = pd.read_csv(data_path)
    
    renames = {
        "nombre_cultivo": "cultivo",
        "rendimiento_t_ha": "rendimiento_toneladas_ha",
        "id_municipio": "codigo_municipio",
        "oni": "promedio_oni",
        "indice_oni": "promedio_oni",
    }
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})
    
    if "cultivo" in df.columns:
        df["cultivo"] = df["cultivo"].astype(str).str.strip().str.title()
        
    if ("municipio" not in df.columns or df["municipio"].isnull().all()) and "codigo_municipio" in df.columns:
        if Path("data/municipios_valle_clean.csv").exists():
            df_mun_map = pd.read_csv("data/municipios_valle_clean.csv")[["codigo_municipio", "municipio"]]
            df = pd.merge(df, df_mun_map, on="codigo_municipio", how="left")
        elif Path("data/dataset_consolidado_valle.csv").exists():
            df_cons = pd.read_csv("data/dataset_consolidado_valle.csv")[["codigo_municipio", "municipio"]].drop_duplicates()
            df = pd.merge(df, df_cons, on="codigo_municipio", how="left")

    if "municipio" not in df.columns:
        df["municipio"] = "Valle del Cauca"
    else:
        df["municipio"] = df["municipio"].astype(str).str.strip().str.title()

    if "rendimiento_toneladas_ha" not in df.columns and "produccion_toneladas" in df.columns and "hectareas_cosechadas" in df.columns:
        df["rendimiento_toneladas_ha"] = np.where(
            df["hectareas_cosechadas"] > 0,
            df["produccion_toneladas"] / df["hectareas_cosechadas"],
            np.nan
        )

    if "promedio_oni" not in df.columns:
        df["promedio_oni"] = np.nan

    # Filtrar rendimientos válidos (0 < rendimiento <= 100 Ton/Ha)
    df_rend = df[(df["rendimiento_toneladas_ha"] > 0) & (df["rendimiento_toneladas_ha"] <= 100)].copy()
    
    cols = ["anio", "municipio", "cultivo", "rendimiento_toneladas_ha", "promedio_oni"]
    df_rend = df_rend[[c for c in cols if c in df_rend.columns]].dropna(subset=["rendimiento_toneladas_ha"])
    
    print(f"--> Dataset gold_rendimiento listo ({len(df_rend):,} filas).")
    return df_rend

def entrenar_modelo_bqml_y_cargar_tablas():
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print(f"✅ Cliente BigQuery autenticado para proyecto '{PROJECT_ID}'.")
    except Exception as exc:
        print(f"❌ Error conectando a BigQuery: {exc}")
        return

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )

    # --------------------------------------------------------------------------
    # 1. MODELO BQML DE PRODUCCIÓN (gold_modelo_pronostico_produccion) & TABLA
    # --------------------------------------------------------------------------
    try:
        client.query(f"DROP TABLE IF EXISTS `{MODEL_PRONOSTICO_PRODUCCION}`").result()
    except Exception:
        pass

    print(f"--> Entrenando MODEL BQML ARIMA_PLUS para Producción ({MODEL_PRONOSTICO_PRODUCCION})...")
    query_create_model_prod = f"""
    CREATE OR REPLACE MODEL `{MODEL_PRONOSTICO_PRODUCCION}`
    OPTIONS(
      MODEL_TYPE = 'ARIMA_PLUS',
      TIME_SERIES_TIMESTAMP_COL = 'fecha_anio',
      TIME_SERIES_DATA_COL = 'produccion_toneladas',
      TIME_SERIES_ID_COL = ['municipio', 'cultivo'],
      AUTO_ARIMA = TRUE,
      DATA_FREQUENCY = 'AUTO_FREQUENCY',
      HORIZON = 3,
      CLEAN_SPIKES_AND_DIPS = TRUE,
      DECOMPOSE_TIME_SERIES = TRUE
    ) AS
    SELECT
      PARSE_DATE('%Y', CAST(mb.anio AS STRING)) AS fecha_anio,
      COALESCE(TRIM(INITCAP(m.municipio)), 'Valle Del Cauca') AS municipio,
      TRIM(INITCAP(mb.cultivo)) AS cultivo,
      SUM(CAST(mb.produccion_toneladas AS FLOAT64)) AS produccion_toneladas
    FROM `{PROJECT_ID}.{DATASET_ID}.silver_agri_modelo_base` mb
    LEFT JOIN (
      SELECT DISTINCT codigo_municipio, municipio
      FROM `{PROJECT_ID}.{DATASET_ID}.silver_agri_consolidado`
      WHERE municipio IS NOT NULL
    ) m ON mb.codigo_municipio = m.codigo_municipio
    WHERE mb.anio >= 2000 AND mb.produccion_toneladas IS NOT NULL AND mb.produccion_toneladas > 0
    GROUP BY 1, 2, 3;
    """
    client.query(query_create_model_prod).result()
    print(f"✅ MODEL BQML `{MODEL_PRONOSTICO_PRODUCCION}` creado exitosamente (ARIMA_PLUS Producción).")

    print(f"--> Generando tabla de pronósticos de producción ({TABLE_PRONOSTICO_PRODUCCION})...")
    query_forecast_prod = f"""
    CREATE OR REPLACE TABLE `{TABLE_PRONOSTICO_PRODUCCION}` AS
    SELECT
      municipio,
      cultivo,
      EXTRACT(YEAR FROM forecast_timestamp) AS anio_pronostico,
      forecast_value AS produccion_predicha_ton,
      standard_error,
      confidence_level,
      prediction_interval_lower_bound AS limite_inferior_80,
      prediction_interval_upper_bound AS limite_superior_80
    FROM
      ML.FORECAST(
        MODEL `{MODEL_PRONOSTICO_PRODUCCION}`,
        STRUCT(3 AS horizon, 0.80 AS confidence_level)
      );
    """
    client.query(query_forecast_prod).result()
    print(f"✅ Tabla `{TABLE_PRONOSTICO_PRODUCCION}` creada exitosamente.")

    # --------------------------------------------------------------------------
    # 2. MODELO BQML DE RENDIMIENTO (gold_modelo_rendimiento) & TABLA
    # --------------------------------------------------------------------------
    try:
        client.query(f"DROP TABLE IF EXISTS `{MODEL_RENDIMIENTO}`").result()
    except Exception:
        pass

    print(f"--> Entrenando MODEL BQML ARIMA_PLUS para Rendimiento ({MODEL_RENDIMIENTO})...")
    query_create_model_rend = f"""
    CREATE OR REPLACE MODEL `{MODEL_RENDIMIENTO}`
    OPTIONS(
      MODEL_TYPE = 'ARIMA_PLUS',
      TIME_SERIES_TIMESTAMP_COL = 'fecha_anio',
      TIME_SERIES_DATA_COL = 'rendimiento_toneladas_ha',
      TIME_SERIES_ID_COL = ['municipio', 'cultivo'],
      AUTO_ARIMA = TRUE,
      DATA_FREQUENCY = 'AUTO_FREQUENCY',
      HORIZON = 3,
      CLEAN_SPIKES_AND_DIPS = TRUE,
      DECOMPOSE_TIME_SERIES = TRUE
    ) AS
    SELECT
      PARSE_DATE('%Y', CAST(mb.anio AS STRING)) AS fecha_anio,
      COALESCE(TRIM(INITCAP(m.municipio)), 'Valle Del Cauca') AS municipio,
      TRIM(INITCAP(mb.cultivo)) AS cultivo,
      AVG(CAST(mb.rendimiento_toneladas_ha AS FLOAT64)) AS rendimiento_toneladas_ha
    FROM `{PROJECT_ID}.{DATASET_ID}.silver_agri_modelo_base` mb
    LEFT JOIN (
      SELECT DISTINCT codigo_municipio, municipio
      FROM `{PROJECT_ID}.{DATASET_ID}.silver_agri_consolidado`
      WHERE municipio IS NOT NULL
    ) m ON mb.codigo_municipio = m.codigo_municipio
    WHERE mb.anio >= 2000 
      AND mb.rendimiento_toneladas_ha IS NOT NULL 
      AND mb.rendimiento_toneladas_ha > 0 
      AND mb.rendimiento_toneladas_ha <= 100
    GROUP BY 1, 2, 3;
    """
    client.query(query_create_model_rend).result()
    print(f"✅ MODEL BQML `{MODEL_RENDIMIENTO}` creado exitosamente (ARIMA_PLUS Rendimiento).")

    print(f"--> Generando tabla de pronósticos de rendimiento ({TABLE_PRONOSTICO_RENDIMIENTO})...")
    query_forecast_rend = f"""
    CREATE OR REPLACE TABLE `{TABLE_PRONOSTICO_RENDIMIENTO}` AS
    SELECT
      municipio,
      cultivo,
      EXTRACT(YEAR FROM forecast_timestamp) AS anio_pronostico,
      forecast_value AS rendimiento_predicho_ton_ha,
      standard_error,
      confidence_level,
      prediction_interval_lower_bound AS limite_inferior_80,
      prediction_interval_upper_bound AS limite_superior_80
    FROM
      ML.FORECAST(
        MODEL `{MODEL_RENDIMIENTO}`,
        STRUCT(3 AS horizon, 0.80 AS confidence_level)
      );
    """
    client.query(query_forecast_rend).result()
    print(f"✅ Tabla `{TABLE_PRONOSTICO_RENDIMIENTO}` creada exitosamente.")

    # --------------------------------------------------------------------------
    # 3. CARGAR TABLA HISTÓRICA DE RENDIMIENTO (gold_rendimiento)
    # --------------------------------------------------------------------------
    if CSV_MODELO_BASE.exists():
        df_r = preparar_gold_modelo_rendimiento(CSV_MODELO_BASE)
        print(f"--> Cargando tabla `{TABLE_RENDIMIENTO}` en BigQuery...")
        try:
            client.delete_model(TABLE_RENDIMIENTO, not_found_ok=True)
        except Exception:
            pass
        job = client.load_table_from_dataframe(df_r, TABLE_RENDIMIENTO, job_config=job_config)
        job.result()
        print(f"✅ Tabla `{TABLE_RENDIMIENTO}` cargada exitosamente ({len(df_r):,} filas).")

if __name__ == "__main__":
    entrenar_modelo_bqml_y_cargar_tablas()

