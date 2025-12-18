

-- Modelo gold: FACT_ENTIDAD
-- Agrega datos de Metas de Producto (MP) y Metas de Resultado (MR) por entidad y año
-- Incluye métricas de avance, eficacia, eficiencia y efectividad

WITH
-- 1. DATOS CRUDOS METAS DE PRODUCTO (MP)
mp_raw AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    'META DE PRODUCTO' as clasificacion,
    UPPER(CONCAT(CAST(codigo_entidad AS STRING), ' - ', nombre_entidad)) as entidad_dependencia,
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_alcanzado_1 AS STRING) = 'NP', 0, 1)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN IF(SAFE_CAST(valor_alcanzado_2 AS STRING) = 'NP', 0, 1)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2026 THEN IF(SAFE_CAST(valor_alcanzado_3 AS STRING) = 'NP', 0, 1)
      ELSE IF(SAFE_CAST(valor_alcanzado_4 AS STRING) = 'NP', 0, 1)
    END as es_programada,
    SAFE_CAST(porcentaje_avance_mp_pa AS FLOAT64) as avance
  FROM `datagov-473122`.`gold_dpt_planeacion_municipal_dev`.`evaplan_api_avance_mp_processed_data`
),

-- 2. AGREGACIÓN MP (Por Entidad)
mp_agg AS (
    SELECT
        anio,
        clasificacion,
        entidad_dependencia,
        COUNT(*) as CANTIDAD_METAS,
        SUM(es_programada) as CANTIDAD_PROGRAMADAS,
        SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) as AVANCE_GE_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_80_PORC,
        SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) as AVANCE_GE_25_LT_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_25_LT_80_PORC,
        SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) as AVANCE_LT_25_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_LT_25_PORC,
        ROUND(AVG(avance), 2) as CALIFICACION_X_CORTE
    FROM mp_raw
    GROUP BY 1, 2, 3
),

-- 3. DATOS CRUDOS METAS DE RESULTADO (MR)
mr_raw AS (
  SELECT
    SAFE_CAST(ano_seguimiento AS INT64) as anio,
    'META DE RESULTADO' as clasificacion,
    UPPER(CONCAT(CAST(codigo_entidad AS STRING), ' - ', nombre_entidad)) as entidad_dependencia,
    CASE
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2024 THEN IF(SAFE_CAST(valor_esperado_1 AS FLOAT64) > 0, 1, 0)
      WHEN SAFE_CAST(ano_seguimiento AS INT64) = 2025 THEN IF(SAFE_CAST(valor_esperado_2 AS FLOAT64) > 0, 1, 0)
      ELSE IF(SAFE_CAST(valor_esperado_3 AS FLOAT64) > 0, 1, 0)
    END as es_programada,
    SAFE_CAST(porcentaje_avance AS FLOAT64) as avance
  FROM `datagov-473122`.`gold_dpt_planeacion_municipal_dev`.`evaplan_api_avance_mr_processed_data`
),

-- 4. AGREGACIÓN MR
mr_agg AS (
    SELECT
        anio,
        clasificacion,
        entidad_dependencia,
        COUNT(*) as CANTIDAD_METAS,
        SUM(es_programada) as CANTIDAD_PROGRAMADAS,
        SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) as AVANCE_GE_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_80_PORC,
        SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) as AVANCE_GE_25_LT_80_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance >= 25 AND avance < 80 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_GE_25_LT_80_PORC,
        SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) as AVANCE_LT_25_CANT,
        ROUND(SAFE_DIVIDE(SUM(CASE WHEN avance < 25 THEN 1 ELSE 0 END) * 100, COUNT(*)), 2) as AVANCE_LT_25_PORC,
        ROUND(AVG(avance), 2) as CALIFICACION_X_CORTE
    FROM mr_raw
    GROUP BY 1, 2, 3
),

union_all AS (
    SELECT * FROM mp_agg
    UNION ALL
    SELECT * FROM mr_agg
),

datos_indicadores AS (
    SELECT 
        UPPER(TRIM(entidad_dependencia)) as entidad_join,
        SAFE_CAST(indice_eficacia AS FLOAT64)      as indice_eficacia,
        UPPER(TRIM(clasificacion_eficacia))        as clasificacion_eficacia,
        SAFE_CAST(indice_eficiencia AS FLOAT64)    as indice_eficiencia,
        UPPER(TRIM(clasificacion_eficiencia))      as clasificacion_eficiencia,
        SAFE_CAST(indice_efectividad AS FLOAT64)   as indice_efectividad,
        UPPER(TRIM(clasificacion_efectividad))     as clasificacion_efectividad
    FROM `datagov-473122.bronze_dpt_planeacion_municipal_dev.stg_indicadores_pa_entidad_2024`
),

period_info AS (
    SELECT
        SAFE_CAST(ano_seguimiento AS INT64) as anio,
        ANY_VALUE(peri_idp)                     AS peri_idp,
        ANY_VALUE(peri_nombre)                  AS peri_nombre,
        ANY_VALUE(fecha_cierre)                 AS fecha_cierre,
        ANY_VALUE(fecha_apertura)               AS fecha_apertura,
        ANY_VALUE(peri_datosapis)               AS peri_datosapis,
        ANY_VALUE(peri_idperiodogobiernoactivo) AS peri_idperiodogobiernoactivo,
        ANY_VALUE(fecha_lectura)                AS fecha_lectura
    FROM `datagov-473122`.`gold_dpt_planeacion_municipal_dev`.`evaplan_api_avance_mp_processed_data`
    GROUP BY 1
)

SELECT 
    T1.anio                                       AS ANIO,
    p.peri_idp                                    AS PERI_IDP,
    p.peri_nombre                                 AS PERI_NOMBRE,
    p.fecha_cierre                                AS FECHA_CIERRE,
    p.fecha_apertura                              AS FECHA_APERTURA,
    p.peri_datosapis                              AS PERI_DATOSAPIS,
    p.peri_idperiodogobiernoactivo                AS PERI_IDPERIODO_GOB_ACTIVO,
    p.fecha_lectura                               AS FECHA_LECTURA,

    T1.clasificacion                              AS CLASIFICACION,
    T1.entidad_dependencia                        AS `ENTIDAD O DEPENDENCIA`,
    T1.CANTIDAD_METAS,
    T1.CANTIDAD_PROGRAMADAS,
    
    T1.AVANCE_GE_80_CANT                          AS `AVANCE_>=_80_CANT`,
    T1.AVANCE_GE_80_PORC                          AS `AVANCE_>=_80_PORC`,
    T1.AVANCE_GE_25_LT_80_CANT                    AS `AVANCE_>=25_< 80_CANT`,
    T1.AVANCE_GE_25_LT_80_PORC                    AS `AVANCE_>=25_<80_PORC`,
    T1.AVANCE_LT_25_CANT                          AS `AVANCE_<25_CANT`,
    T1.AVANCE_LT_25_PORC                          AS `AVANCE_< 25_PORC`,
    
    T1.CALIFICACION_X_CORTE,
    
    CASE 
        WHEN T1.anio = 2024 THEN COALESCE(T2.indice_eficacia, 0) 
        ELSE 0 
    END                                           AS EFICACIA,
    
    CASE 
        WHEN T1.anio = 2024 THEN T2.clasificacion_eficacia 
        ELSE NULL 
    END                                           AS CLASIFICACION_EFICACIA,

    CASE 
        WHEN T1.anio = 2024 THEN COALESCE(T2.indice_eficiencia, 0) 
        ELSE 0 
    END                                           AS EFICIENCIA,
    
    CASE 
        WHEN T1.anio = 2024 THEN T2.clasificacion_eficiencia 
        ELSE NULL 
    END                                           AS CLASIFICACION_EFICIENCIA,

    CASE 
        WHEN T1.anio = 2024 THEN COALESCE(T2.indice_efectividad, 0) 
        ELSE 0 
    END                                           AS EFECTIVIDA,
    
    CASE 
        WHEN T1.anio = 2024 THEN T2.clasificacion_efectividad 
        ELSE NULL 
    END                                           AS CLASIFICACION_EFECTIVIDAD,
    
    CAST(NULL AS INT64)                           AS PROYECTOS_X_DEPENDENCIA,
    CAST(NULL AS INT64)                           AS ACTIVIDADES_X_PROYECTO

FROM union_all T1
LEFT JOIN period_info p
    ON T1.anio = p.anio
LEFT JOIN datos_indicadores T2 
    ON T1.entidad_dependencia = T2.entidad_join
WHERE T1.anio IS NOT NULL
ORDER BY ANIO DESC, CLASIFICACION, `ENTIDAD O DEPENDENCIA`