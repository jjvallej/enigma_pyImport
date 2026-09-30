{{
  config(
    materialized='table',
    schema=var('gold_dataset'),
    alias=var('evaplan_gold_fact_programa_table_name', 'FACT_PROGRAMA')
  )
}}

-- Modelo gold: FACT_PROGRAMA
-- Agrega datos de programas y subprogramas, incluyendo métricas de avance y ponderación.

SELECT
    -- 0. PERÍODO / CORTE
    SAFE_CAST(ano_seguimiento AS INT64)       AS ANIO,
    peri_idp                                  AS PERI_IDP,
    peri_nombre                               AS PERI_NOMBRE,
    fecha_cierre                              AS FECHA_CIERRE,
    fecha_apertura                            AS FECHA_APERTURA,
    peri_datosapis                            AS PERI_DATOSAPIS,
    peri_idperiodogobiernoactivo              AS PERI_IDPERIODO_GOB_ACTIVO,
    fecha_lectura                             AS FECHA_LECTURA,
    -- 1. PROGRAMA
    UPPER(popr_nombre)                        AS PROGRAMA,
    -- 2. SUBPROGRAMA
    CONCAT(
        LPAD(CAST(fimr_subprograma AS STRING), 2, '0'),
        ' - ',
        UPPER(nombre_subprograma)
    )                                         AS SUBPROGRAMA,
    -- 3. MÉTRICAS DE METAS DE PRODUCTO
    SAFE_CAST(mpxsubp AS INT64)               AS MP_X_SUBPROG,
    SAFE_CAST(prom_avance_sub_mp AS FLOAT64)  AS PROM_AVANCE_SUBPROGRAMA_X_MP,
    SAFE_CAST(cantidad_programadas AS INT64)  AS MP_PROGRAMADA_X_SUBPROGRAMA,
    -- 4. PONDERACIONES
    SAFE_CAST(ponderacion_subprograma AS FLOAT64) AS PONDERACION_SUBPROGRAMA,
    SAFE_CAST(avance_ponderado_sub AS FLOAT64)    AS AVANCE_PONDERADO_SUBPROGRAMA
FROM {{ source('gold_evaplan', 'evaplan_api_avance_general_processed_data') }}
WHERE ano_seguimiento IS NOT NULL
ORDER BY ANIO DESC, PROGRAMA, SUBPROGRAMA

