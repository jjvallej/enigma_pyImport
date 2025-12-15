{{
  config(
    materialized='table',
    schema=var('gold_dataset'),
    alias=var('evaplan_gold_fact_entidad_table_name', 'FACT_ENTIDAD')
  )
}}

-- Modelo gold: FACT_ENTIDAD
-- Agrega datos de Metas de Producto (MP) y Metas de Resultado (MR) por entidad y año
-- Incluye métricas de avance, eficacia, eficiencia y efectividad

WITH
-- 1. DATOS CRUDOS METAS DE PRODUCTO (MP)
mp_raw AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Meta de Producto' as clasificacion,
    CONCAT(codigo_entidad, ' - ', nombre_entidad) as entidad_dependencia,
    -- Flag Programada
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_esperado_1 AS FLOAT64) > 0, 1, 0)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN IF(SAFE_CAST(valor_esperado_2 AS FLOAT64) > 0, 1, 0)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2026 THEN IF(SAFE_CAST(valor_esperado_3 AS FLOAT64) > 0, 1, 0)
      ELSE IF(SAFE_CAST(valor_esperado_4 AS FLOAT64) > 0, 1, 0)
    END as es_programada,
    -- Métricas
    SAFE_CAST(porcentaje_avance_mp_pa AS FLOAT64) as avance,
    SAFE_CAST(eficacia AS FLOAT64) as eficacia,
    SAFE_CAST(eficiencia AS FLOAT64) as eficiencia,
    SAFE_CAST(efectividad AS FLOAT64) as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_mp_processed_data') }}
),

-- 2. AGREGACIÓN MP
mp_agg AS (
    SELECT
        anio,
        clasificacion,
        entidad_dependencia,
        ANY_VALUE(peri_idp)                     AS peri_idp,
        ANY_VALUE(peri_nombre)                  AS peri_nombre,
        ANY_VALUE(fecha_cierre)                 AS fecha_cierre,
        ANY_VALUE(fecha_apertura)               AS fecha_apertura,
        ANY_VALUE(peri_datosapis)               AS peri_datosapis,
        ANY_VALUE(peri_idperiodogobiernoactivo) AS peri_idperiodogobiernoactivo,
        ANY_VALUE(fecha_lectura)                AS fecha_lectura,
        COUNT(*) as CANTIDAD_METAS,
        SUM(es_programada) as CANTIDAD_PROGRAMADAS,

        -- Rangos de Avance
        SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) as AVANCE_GE_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_80_PORC,
        SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) as AVANCE_GE_25_LT_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_25_LT_80_PORC,
        SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) as AVANCE_LT_25_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_LT_25_PORC,

        -- Calificación
        ROUND(AVG(avance), 2) as CALIFICACION_X_CORTE,

        -- Eficacia
        ROUND(AVG(eficacia), 2) as EFICACIA,
        CASE
            WHEN AVG(eficacia) >= 90 THEN 'EFICACIA SOBRESALIENTE'
            WHEN AVG(eficacia) >= 80 THEN 'EFICACIA SATISFACTORIA'
            WHEN AVG(eficacia) >= 70 THEN 'EFICACIA MEDIA'
            ELSE 'EFICACIA BAJA'
        END as CLASIFICACION_EFICACIA,

        -- Eficiencia
        ROUND(AVG(eficiencia), 2) as EFICIENCIA,
        CASE
            WHEN AVG(eficiencia) >= 1.0 THEN 'EFICIENCIA SOBRESALIENTE'
            WHEN AVG(eficiencia) >= 0.8 THEN 'EFICIENCIA SATISFACTORIA'
            ELSE 'EFICIENCIA BAJA'
        END as CLASIFICACION_EFICIENCIA,

        -- Efectividad
        ROUND(AVG(efectividad), 2) as EFECTIVIDAD,
        CASE
             WHEN AVG(efectividad) >= 90 THEN 'EFECTIVIDAD SOBRESALIENTE'
             ELSE 'EFECTIVIDAD BAJA'
        END as CLASIFICACION_EFECTIVIDAD
    FROM mp_raw
    GROUP BY anio, clasificacion, entidad_dependencia
),

-- 3. DATOS CRUDOS METAS DE RESULTADO (MR)
mr_raw AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Meta de Resultado' as clasificacion,
    CONCAT(codigo_entidad, ' - ', nombre_entidad) as entidad_dependencia,
    -- Flag Programada
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_esperado_1 AS FLOAT64) > 0, 1, 0)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN IF(SAFE_CAST(valor_esperado_2 AS FLOAT64) > 0, 1, 0)
      ELSE IF(SAFE_CAST(valor_esperado_3 AS FLOAT64) > 0, 1, 0)
    END as es_programada,
    -- Avance
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN SAFE_CAST(porcentaje_avance_pa1 AS FLOAT64)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN SAFE_CAST(porcentaje_avance_pa2 AS FLOAT64)
      ELSE SAFE_CAST(porcentaje_avance_pa3 AS FLOAT64)
    END as avance
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_mr_processed_data') }}
),

-- 4. AGREGACIÓN MR
mr_agg AS (
    SELECT
        anio,
        clasificacion,
        entidad_dependencia,
        ANY_VALUE(peri_idp)                     AS peri_idp,
        ANY_VALUE(peri_nombre)                  AS peri_nombre,
        ANY_VALUE(fecha_cierre)                 AS fecha_cierre,
        ANY_VALUE(fecha_apertura)               AS fecha_apertura,
        ANY_VALUE(peri_datosapis)               AS peri_datosapis,
        ANY_VALUE(peri_idperiodogobiernoactivo) AS peri_idperiodogobiernoactivo,
        ANY_VALUE(fecha_lectura)                AS fecha_lectura,
        COUNT(*) as CANTIDAD_METAS,
        SUM(es_programada) as CANTIDAD_PROGRAMADAS,

        SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) as AVANCE_GE_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_80_PORC,
        SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) as AVANCE_GE_25_LT_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_25_LT_80_PORC,
        SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) as AVANCE_LT_25_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_LT_25_PORC,

        ROUND(AVG(avance), 2) as CALIFICACION_X_CORTE,

        -- Eficacia MR (usas el mismo avance)
        ROUND(AVG(avance), 2) as EFICACIA,
        CASE
            WHEN AVG(avance) >= 90 THEN 'EFICACIA SOBRESALIENTE'
            WHEN AVG(avance) >= 80 THEN 'EFICACIA SATISFACTORIA'
            WHEN AVG(avance) >= 70 THEN 'EFICACIA MEDIA'
            ELSE 'EFICACIA BAJA'
        END as CLASIFICACION_EFICACIA,

        -- Nulos para MR (Tipados explícitamente)
        CAST(NULL AS FLOAT64) as EFICIENCIA,
        CAST(NULL AS STRING)  as CLASIFICACION_EFICIENCIA,
        CAST(NULL AS FLOAT64) as EFECTIVIDAD,
        CAST(NULL AS STRING)  as CLASIFICACION_EFECTIVIDAD
    FROM mr_raw
    GROUP BY anio, clasificacion, entidad_dependencia
)

-- 5. UNIÓN FINAL
SELECT
    anio as ANIO,
    peri_idp as PERI_IDP,
    peri_nombre as PERI_NOMBRE,
    fecha_cierre as FECHA_CIERRE,
    fecha_apertura as FECHA_APERTURA,
    peri_datosapis as PERI_DATOSAPIS,
    peri_idperiodogobiernoactivo as PERI_IDPERIODO_GOB_ACTIVO,
    fecha_lectura as FECHA_LECTURA,
    clasificacion as CLASIFICACION,
    entidad_dependencia as `ENTIDAD O DEPENDENCIA`,
    CANTIDAD_METAS,
    CANTIDAD_PROGRAMADAS,
    AVANCE_GE_80_CANT as `AVANCE_>=_80_CANT`,
    AVANCE_GE_80_PORC as `AVANCE_>=_80_PORC`,
    AVANCE_GE_25_LT_80_CANT as `AVANCE_>=25_< 80_CANT`,
    AVANCE_GE_25_LT_80_PORC as `AVANCE_>=25_<80_PORC`,
    AVANCE_LT_25_CANT as `AVANCE_<25_CANT`,
    AVANCE_LT_25_PORC as `AVANCE_< 25_PORC`,
    CALIFICACION_X_CORTE,
    EFICACIA,
    CLASIFICACION_EFICACIA,
    EFICIENCIA,
    CLASIFICACION_EFICIENCIA,
    EFECTIVIDAD as EFECTIVIDA,
    CLASIFICACION_EFECTIVIDAD,
    CAST(NULL AS INT64) as PROYECTOS_X_DEPENDENCIA,
    CAST(NULL AS INT64) as ACTIVIDADES_X_PROYECTO
FROM mp_agg

UNION ALL

SELECT
    anio as ANIO,
    peri_idp as PERI_IDP,
    peri_nombre as PERI_NOMBRE,
    fecha_cierre as FECHA_CIERRE,
    fecha_apertura as FECHA_APERTURA,
    peri_datosapis as PERI_DATOSAPIS,
    peri_idperiodogobiernoactivo as PERI_IDPERIODO_GOB_ACTIVO,
    fecha_lectura as FECHA_LECTURA,
    clasificacion as CLASIFICACION,
    entidad_dependencia as `ENTIDAD O DEPENDENCIA`,
    CANTIDAD_METAS,
    CANTIDAD_PROGRAMADAS,
    AVANCE_GE_80_CANT as `AVANCE_>=_80_CANT`,
    AVANCE_GE_80_PORC as `AVANCE_>=_80_PORC`,
    AVANCE_GE_25_LT_80_CANT as `AVANCE_>=25_< 80_CANT`,
    AVANCE_GE_25_LT_80_PORC as `AVANCE_>=25_<80_PORC`,
    AVANCE_LT_25_CANT as `AVANCE_<25_CANT`,
    AVANCE_LT_25_PORC as `AVANCE_< 25_PORC`,
    CALIFICACION_X_CORTE,
    EFICACIA,
    CLASIFICACION_EFICACIA,
    EFICIENCIA,
    CLASIFICACION_EFICIENCIA,
    EFECTIVIDAD as EFECTIVIDA,
    CLASIFICACION_EFECTIVIDAD,
    CAST(NULL AS INT64) as PROYECTOS_X_DEPENDENCIA,
    CAST(NULL AS INT64) as ACTIVIDADES_X_PROYECTO
FROM mr_agg
ORDER BY ANIO DESC, CLASIFICACION, `ENTIDAD O DEPENDENCIA`

