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
----------------------------------------------------------------------------
-- 1. DEDUPLICACIÓN Y CÁLCULO DE METAS DE PRODUCTO (MP)
----------------------------------------------------------------------------
mp_base AS (
    SELECT
        SAFE_CAST(ano_seguimiento AS INT64) as anio,
        codigo_mp,
        AVG(SAFE_CAST(porcentaje_avance_mp_pa AS FLOAT64)) as avance_pa,
        AVG(SAFE_CAST(porcentaje_avance_mp_pg AS FLOAT64)) as avance_pg,
        MAX(CASE
            WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_alcanzado_1 AS STRING) = 'NP', 0, 1)
            WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN IF(SAFE_CAST(valor_alcanzado_2 AS STRING) = 'NP', 0, 1)
            ELSE IF(SAFE_CAST(valor_alcanzado_3 AS STRING) = 'NP', 0, 1)
        END) as es_programada
    FROM {{ source('gold_evaplan', 'evaplan_api_avance_mp_processed_data') }}
    GROUP BY 1, 2
),

mp_stats AS (
    SELECT
        anio,
        'METAS DE PRODUCTO' as ITEM,
        COUNT(*) as TOTAL,
        SUM(es_programada) as PROGRAMADAS,
        (COUNT(*) - SUM(es_programada)) as NO_PROGRAMADAS,
        AVG(avance_pa) as AVANCE_PA,
        AVG(avance_pg) as AVANCE_PG,
        SUM(CASE WHEN avance_pa >= 80 THEN 1 ELSE 0 END) as pa_ge_80,
        SUM(CASE WHEN avance_pa >= 25 AND avance_pa < 80 THEN 1 ELSE 0 END) as pa_ge_25_80,
        SUM(CASE WHEN avance_pa < 25 THEN 1 ELSE 0 END) as pa_lt_25,
        SUM(CASE WHEN avance_pg >= 80 THEN 1 ELSE 0 END) as pg_ge_80,
        SUM(CASE WHEN avance_pg >= 25 AND avance_pg < 80 THEN 1 ELSE 0 END) as pg_ge_25_80,
        SUM(CASE WHEN avance_pg < 25 THEN 1 ELSE 0 END) as pg_lt_25
    FROM mp_base
    GROUP BY 1
),

----------------------------------------------------------------------------
-- 2. DEDUPLICACIÓN DE METAS DE RESULTADO (MR)
----------------------------------------------------------------------------
mr_base AS (
    SELECT
        SAFE_CAST(ano_seguimiento AS INT64) as anio,
        codigo_mr,
        AVG(SAFE_CAST(porcentaje_avance AS FLOAT64)) as avance_pa,
        AVG(SAFE_CAST(porcentaje_avance_cuatrienario AS FLOAT64)) as avance_pg,
        MAX(CASE
            WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_esperado_1 AS FLOAT64) > 0, 1, 0)
            ELSE IF(SAFE_CAST(valor_esperado_2 AS FLOAT64) > 0, 1, 0)
        END) as es_programada
    FROM {{ source('gold_evaplan', 'evaplan_api_avance_mr_processed_data') }}
    GROUP BY 1, 2
),

mr_stats AS (
    SELECT
        anio,
        'METAS DE RESULTADO' as ITEM,
        COUNT(*) as TOTAL,
        SUM(es_programada) as PROGRAMADAS,
        (COUNT(*) - SUM(es_programada)) as NO_PROGRAMADAS,
        AVG(avance_pa) as AVANCE_PA,
        AVG(avance_pg) as AVANCE_PG,
        SUM(CASE WHEN avance_pa >= 80 THEN 1 ELSE 0 END) as pa_ge_80,
        SUM(CASE WHEN avance_pa >= 25 AND avance_pa < 80 THEN 1 ELSE 0 END) as pa_ge_25_80,
        SUM(CASE WHEN avance_pa < 25 THEN 1 ELSE 0 END) as pa_lt_25,
        SUM(CASE WHEN avance_pg >= 80 THEN 1 ELSE 0 END) as pg_ge_80,
        SUM(CASE WHEN avance_pg >= 25 AND avance_pg < 80 THEN 1 ELSE 0 END) as pg_ge_25_80,
        SUM(CASE WHEN avance_pg < 25 THEN 1 ELSE 0 END) as pg_lt_25
    FROM mr_base
    GROUP BY 1
),

----------------------------------------------------------------------------
-- 3. DEDUPLICACIÓN DE SUBPROGRAMAS
----------------------------------------------------------------------------
sub_base AS (
    SELECT
        SAFE_CAST(ano_seguimiento AS INT64) as anio,
        fimr_subprograma,
        AVG(SAFE_CAST(prom_avance_sub_mp AS FLOAT64)) as avance_pa,
        AVG(SAFE_CAST(avanceponderadosub AS FLOAT64)) as avance_pg,
        MAX(IF(SAFE_CAST(cantidad_validos AS INT64) > 0, 1, 0)) as es_programada
    FROM {{ source('gold_evaplan', 'evaplan_api_avance_subprogramas_processed_data') }}
    GROUP BY 1, 2
),

sub_stats AS (
    SELECT
        anio,
        'SUBPROGRAMAS' as ITEM,
        COUNT(*) as TOTAL,
        SUM(es_programada) as PROGRAMADAS,
        (COUNT(*) - SUM(es_programada)) as NO_PROGRAMADAS,
        AVG(avance_pa) as AVANCE_PA,
        AVG(avance_pg) as AVANCE_PG,
        SUM(CASE WHEN avance_pa >= 80 THEN 1 ELSE 0 END) as pa_ge_80,
        SUM(CASE WHEN avance_pa >= 25 AND avance_pa < 80 THEN 1 ELSE 0 END) as pa_ge_25_80,
        SUM(CASE WHEN avance_pa < 25 THEN 1 ELSE 0 END) as pa_lt_25,
        SUM(CASE WHEN avance_pg >= 80 THEN 1 ELSE 0 END) as pg_ge_80,
        SUM(CASE WHEN avance_pg >= 25 AND avance_pg < 80 THEN 1 ELSE 0 END) as pg_ge_25_80,
        SUM(CASE WHEN avance_pg < 25 THEN 1 ELSE 0 END) as pg_lt_25
    FROM sub_base
    GROUP BY 1
),

----------------------------------------------------------------------------
-- 4. DEDUPLICACIÓN DE PROGRAMAS
----------------------------------------------------------------------------
prog_base AS (
    SELECT
        SAFE_CAST(ano_seguimiento AS INT64) as anio,
        fimr_programa,
        AVG(SAFE_CAST(cump_programa AS FLOAT64)) as avance_pa,
        AVG(SAFE_CAST(avance_ponderado_programa AS FLOAT64)) as avance_pg,
        1 as es_programada
    FROM {{ source('gold_evaplan', 'evaplan_api_avance_programas_processed_data') }}
    GROUP BY 1, 2
),

prog_stats AS (
    SELECT
        anio,
        'PROGRAMAS' as ITEM,
        COUNT(*) as TOTAL,
        SUM(es_programada) as PROGRAMADAS,
        (COUNT(*) - SUM(es_programada)) as NO_PROGRAMADAS,
        AVG(avance_pa) as AVANCE_PA,
        AVG(avance_pg) as AVANCE_PG,
        SUM(CASE WHEN avance_pa >= 80 THEN 1 ELSE 0 END) as pa_ge_80,
        SUM(CASE WHEN avance_pa >= 25 AND avance_pa < 80 THEN 1 ELSE 0 END) as pa_ge_25_80,
        SUM(CASE WHEN avance_pa < 25 THEN 1 ELSE 0 END) as pa_lt_25,
        SUM(CASE WHEN avance_pg >= 80 THEN 1 ELSE 0 END) as pg_ge_80,
        SUM(CASE WHEN avance_pg >= 25 AND avance_pg < 80 THEN 1 ELSE 0 END) as pg_ge_25_80,
        SUM(CASE WHEN avance_pg < 25 THEN 1 ELSE 0 END) as pg_lt_25
    FROM prog_base
    GROUP BY 1
),

----------------------------------------------------------------------------
-- 5. LÍNEAS ESTRATÉGICAS (Deduplicadas)
----------------------------------------------------------------------------
linea_base AS (
    SELECT
        SAFE_CAST(ano_seguimiento AS INT64) as anio,
        linea_estrategica,
        AVG(SAFE_CAST(pa_promedio_cumpl_linea_estrategica AS FLOAT64)) as avance_pa,
        AVG(SAFE_CAST(pg_suma_cumpl_linea_estrategica AS FLOAT64)) as avance_pg
    FROM {{ source('gold_evaplan', 'evaplan_api_avance_programas_processed_data') }}
    GROUP BY 1, 2
),

linea_stats AS (
    SELECT
        anio,
        'LÍNEAS ESTRATÉGICAS' as ITEM,
        COUNT(*) as TOTAL,
        COUNT(*) as PROGRAMADAS,
        0 as NO_PROGRAMADAS,
        AVG(avance_pa) as AVANCE_PA,
        AVG(avance_pg) as AVANCE_PG,
        SUM(CASE WHEN avance_pa >= 80 THEN 1 ELSE 0 END) as pa_ge_80,
        SUM(CASE WHEN avance_pa >= 25 AND avance_pa < 80 THEN 1 ELSE 0 END) as pa_ge_25_80,
        SUM(CASE WHEN avance_pa < 25 THEN 1 ELSE 0 END) as pa_lt_25,
        SUM(CASE WHEN avance_pg >= 80 THEN 1 ELSE 0 END) as pg_ge_80,
        SUM(CASE WHEN avance_pg >= 25 AND avance_pg < 80 THEN 1 ELSE 0 END) as pg_ge_25_80,
        SUM(CASE WHEN avance_pg < 25 THEN 1 ELSE 0 END) as pg_lt_25
    FROM linea_base
    GROUP BY 1
),

----------------------------------------------------------------------------
-- 6. UNIÓN DE TODO (Estadísticas Base)
----------------------------------------------------------------------------
all_stats AS (
    SELECT * FROM mp_stats
    UNION ALL SELECT * FROM mr_stats
    UNION ALL SELECT * FROM sub_stats
    UNION ALL SELECT * FROM prog_stats
    UNION ALL SELECT * FROM linea_stats
),

----------------------------------------------------------------------------
-- 7. DATOS EXTERNOS (INDICADORES EXCEL)
----------------------------------------------------------------------------
datos_indicadores AS (
    SELECT
        UPPER(TRIM(item)) as item_join,
        SAFE_CAST(eficacia_val AS FLOAT64) as eficacia,
        UPPER(TRIM(eficacia_clasif)) as clasif_eficacia,
        SAFE_CAST(eficiencia_val AS FLOAT64) as eficiencia,
        UPPER(TRIM(eficiencia_clasif)) as clasif_eficiencia,
        SAFE_CAST(efectividad_val AS FLOAT64) as efectividad,
        UPPER(TRIM(efectividad_clasif)) as clasif_efectividad
    FROM `{{ var('project_id') }}.{{ var('bronze_dataset') }}.stg_resumen_pa_2024`
),

----------------------------------------------------------------------------
-- 8. INFO DE PERÍODO POR AÑO
----------------------------------------------------------------------------
period_info AS (
    SELECT
        SAFE_CAST(ano_seguimiento AS INT64) AS anio,
        ANY_VALUE(peri_idp)                     AS peri_idp,
        ANY_VALUE(peri_nombre)                  AS peri_nombre,
        ANY_VALUE(fecha_cierre)                 AS fecha_cierre,
        ANY_VALUE(fecha_apertura)               AS fecha_apertura,
        ANY_VALUE(peri_datosapis)               AS peri_datosapis,
        ANY_VALUE(peri_idperiodogobiernoactivo) AS peri_idperiodogobiernoactivo,
        ANY_VALUE(fecha_lectura)                AS fecha_lectura
    FROM {{ source('gold_evaplan', 'evaplan_api_avance_mp_processed_data') }}
    GROUP BY 1
)

----------------------------------------------------------------------------
-- 9. SELECT FINAL (Generando filas para PA y PG) CON PERÍODO
----------------------------------------------------------------------------
SELECT
    T1.anio as ANIO,
    P.peri_idp                     AS PERI_IDP,
    P.peri_nombre                  AS PERI_NOMBRE,
    P.fecha_cierre                 AS FECHA_CIERRE,
    P.fecha_apertura               AS FECHA_APERTURA,
    P.peri_datosapis               AS PERI_DATOSAPIS,
    P.peri_idperiodogobiernoactivo AS PERI_IDPERIODO_GOB_ACTIVO,
    P.fecha_lectura                AS FECHA_LECTURA,
    'PLAN DE ACCIÓN' as CLASIFICACION,
    T1.ITEM,
    T1.TOTAL,
    T1.PROGRAMADAS,
    T1.NO_PROGRAMADAS,
    T1.pa_ge_80 as `AVANCE_>=_80_CANT`,
    ROUND(SAFE_DIVIDE(T1.pa_ge_80 * 100, T1.TOTAL), 2) as `AVANCE_>=_80_PORC`,
    T1.pa_ge_25_80 as `AVANCE_>=25_< 80_CANT`,
    ROUND(SAFE_DIVIDE(T1.pa_ge_25_80 * 100, T1.TOTAL), 2) as `AVANCE_>=25_<80_PORC`,
    T1.pa_lt_25 as `AVANCE_<25_CANT`,
    ROUND(SAFE_DIVIDE(T1.pa_lt_25 * 100, T1.TOTAL), 2) as `AVANCE_< 25_PORC`,
    ROUND(T1.AVANCE_PA, 2) as PROMEDIO_AVANCE_PORC,
   
    CASE WHEN T1.anio = 2024 THEN COALESCE(T2.eficacia, 0) ELSE 0 END as EFICACIA,
    CASE WHEN T1.anio = 2024 THEN T2.clasif_eficacia ELSE NULL END as CLASIF_EFICACIA,
    CASE WHEN T1.anio = 2024 THEN COALESCE(T2.eficiencia, 0) ELSE 0 END as EFICIENCIA,
    CASE WHEN T1.anio = 2024 THEN T2.clasif_eficiencia ELSE NULL END as CLASIF_EFICIENCIA,
    CASE WHEN T1.anio = 2024 THEN COALESCE(T2.efectividad, 0) ELSE 0 END as EFECTIVIDAD,
    CASE WHEN T1.anio = 2024 THEN T2.clasif_efectividad ELSE NULL END as CLASIF_EFECTIVIDAD

FROM all_stats T1
LEFT JOIN datos_indicadores T2
    ON UPPER(TRIM(T1.ITEM)) = T2.item_join
LEFT JOIN period_info P
    ON T1.anio = P.anio

UNION ALL

SELECT
    T1.anio as ANIO,
    P.peri_idp                     AS PERI_IDP,
    P.peri_nombre                  AS PERI_NOMBRE,
    P.fecha_cierre                 AS FECHA_CIERRE,
    P.fecha_apertura               AS FECHA_APERTURA,
    P.peri_datosapis               AS PERI_DATOSAPIS,
    P.peri_idperiodogobiernoactivo AS PERI_IDPERIODO_GOB_ACTIVO,
    P.fecha_lectura                AS FECHA_LECTURA,
    'PERÍODO DE GOBIERNO' as CLASIFICACION,
    T1.ITEM,
    T1.TOTAL,
    T1.TOTAL as PROGRAMADAS,
    0 as NO_PROGRAMADAS,
    T1.pg_ge_80,
    ROUND(SAFE_DIVIDE(T1.pg_ge_80 * 100, T1.TOTAL), 2),
    T1.pg_ge_25_80,
    ROUND(SAFE_DIVIDE(T1.pg_ge_25_80 * 100, T1.TOTAL), 2),
    T1.pg_lt_25,
    ROUND(SAFE_DIVIDE(T1.pg_lt_25 * 100, T1.TOTAL), 2),
    ROUND(T1.AVANCE_PG, 2),
    0, NULL, 0, NULL, 0, NULL

FROM all_stats T1
LEFT JOIN period_info P
    ON T1.anio = P.anio
ORDER BY ANIO DESC, CLASIFICACION, ITEM
