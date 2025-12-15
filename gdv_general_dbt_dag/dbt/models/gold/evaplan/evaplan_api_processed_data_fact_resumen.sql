{{
  config(
    materialized='table',
    schema=var('gold_dataset'),
    alias=var('evaplan_gold_fact_resumen_table_name', 'FACT_RESUMEN')
  )
}}

-- Modelo gold: FACT_RESUMEN
-- Consolida y resume métricas clave de Evaplan (MP, MR, Subprogramas, Programas, Líneas Estratégicas)
-- por año, clasificación e ítem.

WITH
-- 1. PREPARACIÓN DE DATOS
-- A. METAS DE PRODUCTO (MP) - Única fuente con Eficacia/Eficiencia
mp_data AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Plan de Acción' as clasificacion,
    'METAS DE PRODUCTO' as item,
    -- Lógica Programada: Si valor esperado del año > 0
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_esperado_1 AS FLOAT64) > 0, 1, 0)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN IF(SAFE_CAST(valor_esperado_2 AS FLOAT64) > 0, 1, 0)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2026 THEN IF(SAFE_CAST(valor_esperado_3 AS FLOAT64) > 0, 1, 0)
      ELSE IF(SAFE_CAST(valor_esperado_4 AS FLOAT64) > 0, 1, 0)
    END as es_programada,
    SAFE_CAST(porcentaje_avance_mp_pa AS FLOAT64) as avance,
    SAFE_CAST(eficacia AS FLOAT64) as eficacia,
    SAFE_CAST(eficiencia AS FLOAT64) as eficiencia,
    SAFE_CAST(efectividad AS FLOAT64) as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_mp_processed_data') }}
  UNION ALL
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Período de Gobierno' as clasificacion,
    'METAS DE PRODUCTO' as item,
    -- Lógica Programada PG: Si valor esperado cuatrienio > 0
    IF(SAFE_CAST(valor_cuatrienario_esperado AS FLOAT64) > 0, 1, 0) as es_programada,
    SAFE_CAST(porcentaje_avance_mp_pg AS FLOAT64) as avance,
    SAFE_CAST(eficacia AS FLOAT64) as eficacia,
    SAFE_CAST(eficiencia AS FLOAT64) as eficiencia,
    SAFE_CAST(efectividad AS FLOAT64) as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_mp_processed_data') }}
),

-- B. METAS DE RESULTADO (MR)
mr_data AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Plan de Acción' as clasificacion,
    'METAS DE RESULTADO' as item,
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_esperado_1 AS FLOAT64) > 0, 1, 0)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN IF(SAFE_CAST(valor_esperado_2 AS FLOAT64) > 0, 1, 0)
      ELSE IF(SAFE_CAST(valor_esperado_3 AS FLOAT64) > 0, 1, 0)
    END as es_programada,
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN SAFE_CAST(porcentaje_avance_pa1 AS FLOAT64)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN SAFE_CAST(porcentaje_avance_pa2 AS FLOAT64)
      ELSE SAFE_CAST(porcentaje_avance_pa3 AS FLOAT64)
    END as avance,
    NULL as eficacia, NULL as eficiencia, NULL as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_mr_processed_data') }}
  UNION ALL
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Período de Gobierno' as clasificacion,
    'METAS DE RESULTADO' as item,
    IF(SAFE_CAST(valor_esperado_cuatrienario AS FLOAT64) > 0, 1, 0) as es_programada,
    SAFE_CAST(porcentaje_avance_cuatrienario AS FLOAT64) as avance,
    NULL as eficacia, NULL as eficiencia, NULL as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_mr_processed_data') }}
),

-- C. SUBPROGRAMAS
sub_data AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Plan de Acción' as clasificacion,
    'SUBPROGRAMAS' as item,
    1 as es_programada,
    SAFE_CAST(prom_avance_sub_mp AS FLOAT64) as avance,
    NULL as eficacia, NULL as eficiencia, NULL as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_subprogramas_processed_data') }}
  UNION ALL
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Período de Gobierno' as clasificacion,
    'SUBPROGRAMAS' as item,
    1 as es_programada,
    SAFE_CAST(avanceponderadosub AS FLOAT64) as avance,
    NULL as eficacia, NULL as eficiencia, NULL as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_subprogramas_processed_data') }}
),

-- D. PROGRAMAS
prog_data AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Plan de Acción' as clasificacion,
    'PROGRAMAS' as item,
    1 as es_programada,
    SAFE_CAST(cump_programa AS FLOAT64) as avance,
    NULL as eficacia, NULL as eficiencia, NULL as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_programas_processed_data') }}
  UNION ALL
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    peri_idp,
    peri_nombre,
    fecha_cierre,
    fecha_apertura,
    peri_datosapis,
    peri_idperiodogobiernoactivo,
    fecha_lectura,
    'Período de Gobierno' as clasificacion,
    'PROGRAMAS' as item,
    1 as es_programada,
    SAFE_CAST(avance_ponderado_programa AS FLOAT64) as avance,
    NULL as eficacia, NULL as eficiencia, NULL as efectividad
  FROM {{ source('gold_evaplan', 'evaplan_api_avance_programas_processed_data') }}
),

-- E. LÍNEAS ESTRATÉGICAS
linea_unique AS (
    SELECT DISTINCT
        SAFE_CAST(ano_seguimiento AS INT64) as anio,
        peri_idp,
        peri_nombre,
        fecha_cierre,
        fecha_apertura,
        peri_datosapis,
        peri_idperiodogobiernoactivo,
        fecha_lectura,
        linea_estrategica,
        SAFE_CAST(pa_promedio_cumpl_linea_estrategica AS FLOAT64) as avance_pa,
        SAFE_CAST(pg_suma_cumpl_linea_estrategica AS FLOAT64) as avance_pg
    FROM {{ source('gold_evaplan', 'evaplan_api_avance_programas_processed_data') }}
),
linea_data AS (
    SELECT
        anio,
        peri_idp,
        peri_nombre,
        fecha_cierre,
        fecha_apertura,
        peri_datosapis,
        peri_idperiodogobiernoactivo,
        fecha_lectura,
        'Plan de Acción' as clasificacion,
        'LÍNEAS ESTRATÉGICAS' as item,
        1 as es_programada,
        avance_pa as avance,
        NULL as eficacia, NULL as eficiencia, NULL as efectividad
    FROM linea_unique
    UNION ALL
    SELECT
        anio,
        peri_idp,
        peri_nombre,
        fecha_cierre,
        fecha_apertura,
        peri_datosapis,
        peri_idperiodogobiernoactivo,
        fecha_lectura,
        'Período de Gobierno' as clasificacion,
        'LÍNEAS ESTRATÉGICAS' as item,
        1 as es_programada,
        avance_pg as avance,
        NULL as eficacia, NULL as eficiencia, NULL as efectividad
    FROM linea_unique
),

-- 2. UNIFICACIÓN FINAL
union_todos AS (
    SELECT * FROM mp_data
    UNION ALL
    SELECT * FROM mr_data
    UNION ALL
    SELECT * FROM sub_data
    UNION ALL
    SELECT * FROM prog_data
    UNION ALL
    SELECT * FROM linea_data
)

-- 3. SELECT FINAL (con nombres esperados por Excel)
SELECT
    anio                                         AS ANIO,
    ANY_VALUE(peri_idp)                          AS PERI_IDP,
    ANY_VALUE(peri_nombre)                       AS PERI_NOMBRE,
    ANY_VALUE(fecha_cierre)                      AS FECHA_CIERRE,
    ANY_VALUE(fecha_apertura)                    AS FECHA_APERTURA,
    ANY_VALUE(peri_datosapis)                    AS PERI_DATOSAPIS,
    ANY_VALUE(peri_idperiodogobiernoactivo)      AS PERI_IDPERIODO_GOB_ACTIVO,
    ANY_VALUE(fecha_lectura)                     AS FECHA_LECTURA,
    clasificacion                                AS CLASIFICACION,
    item                                         AS ITEM,
    COUNT(*)                                     AS TOTAL,
    IFNULL(SUM(es_programada), 0)                AS PROGRAMADAS,
    COUNT(*) - IFNULL(SUM(es_programada), 0)     AS NO_PROGRAMADAS,
    -- Rangos de Avance con alias EXACTOS para Excel
    SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END)                         AS `AVANCE_>=_80_CANT`,
    ROUND(SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END)
          / COUNT(*) * 100, 2)                                            AS `AVANCE_>=_80_PORC`,
    SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END)         AS `AVANCE_>=25_< 80_CANT`,
    ROUND(SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END)
          / COUNT(*) * 100, 2)                                            AS `AVANCE_>=25_<80_PORC`,
    SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END)                          AS `AVANCE_<25_CANT`,
    ROUND(SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END)
          / COUNT(*) * 100, 2)                                            AS `AVANCE_< 25_PORC`,
    ROUND(AVG(avance), 2)                                                 AS PROMEDIO_AVANCE_PORC,
    -- Métricas Numéricas
    ROUND(AVG(eficacia), 2)                                               AS EFICACIA,
    ROUND(AVG(eficiencia), 2)                                             AS EFICIENCIA,
    ROUND(AVG(efectividad), 2)                                            AS EFECTIVIDAD
FROM union_todos
WHERE anio IS NOT NULL
GROUP BY
    anio,
    clasificacion,
    item
ORDER BY anio DESC, clasificacion, item

