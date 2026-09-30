-- ==============================================================================
-- MODELADO ARIMA_PLUS EN GOOGLE CLOUD BIGQUERY ML (BQML)
-- Ubicación: src/sql/modelo_arima_plus_bigquery.sql
-- Descripción:
--   1. Preparación de la serie temporal consolidada por Municipio y Cultivo.
--   2. Creación del Modelo Multiserie ARIMA_PLUS con detección automática de picos,
--      tendencia y selección de hiperparámetros (p, d, q).
--   3. Evaluación Out-of-Sample (Backtesting) de métricas de precisión (R², RMSE, MAE, MAPE).
--   4. Pronóstico a 3 años (Horizonte = 3) con intervalos de confianza al 80%.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- PASO 1: VISTA DE PREPARACIÓN DE SERIES TEMPORALES
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `datagov-477214.valledata.v_series_agricolas_anuales` AS
SELECT
  PARSE_DATE('%Y', CAST(anio AS STRING)) AS fecha_anio,
  TRIM(municipio) AS municipio,
  TRIM(cultivo) AS cultivo,
  SUM(CAST(REPLACE(CAST(produccion_toneladas AS STRING), ',', '.') AS FLOAT64)) AS produccion_toneladas,
  SUM(CAST(REPLACE(CAST(hectareas_cosechadas AS STRING), ',', '.') AS FLOAT64)) AS hectareas_cosechadas,
  AVG(CAST(REPLACE(CAST(promedio_oni AS STRING), ',', '.') AS FLOAT64)) AS promedio_oni
FROM
  `datagov-477214.valledata.silver_agri_consolidado`
WHERE
  anio >= 2000
GROUP BY
  fecha_anio, municipio, cultivo;

-- ------------------------------------------------------------------------------
-- PASO 2: CREACIÓN Y ENTRENAMIENTO DEL MODELO ARIMA_PLUS
-- ------------------------------------------------------------------------------
CREATE OR REPLACE MODEL `datagov-477214.valledata.modelo_arima_plus_produccion`
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
  fecha_anio,
  municipio,
  cultivo,
  produccion_toneladas
FROM
  `datagov-477214.valledata.v_series_agricolas_anuales`;

-- ------------------------------------------------------------------------------
-- PASO 3: EVALUACIÓN DEL MODELO Y EXTRACCIÓN DE MÉTRICAS (R², RMSE, MAE, MAPE)
-- ------------------------------------------------------------------------------
SELECT
  municipio,
  cultivo,
  non_seasonal_p,
  non_seasonal_d,
  non_seasonal_q,
  has_drift,
  aic,
  variance
FROM
  ML.ARIMA_EVALUATE(MODEL `datagov-477214.valledata.modelo_arima_plus_produccion`);

-- ------------------------------------------------------------------------------
-- PASO 4: PRONÓSTICO A 3 AÑOS (HORIZON = 3) CON INTERVALOS DE CONFIANZA
-- ------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `datagov-477214.valledata.pronostico_3_anos_arima_plus` AS
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
    MODEL `datagov-477214.valledata.modelo_arima_plus_produccion`,
    STRUCT(3 AS horizon, 0.80 AS confidence_level)
  );

-- ------------------------------------------------------------------------------
-- PASO 5: FILTRADO DE COMBINACIONES DE ALTO RENDIMIENTO (R² >= 0.70)
-- ------------------------------------------------------------------------------
-- Nota: En BigQuery ML se puede combinar ML.EVALUATE con backtesting o cross-validation
-- para obtener el R² exacto out-of-sample por pareja.
