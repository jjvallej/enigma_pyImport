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
    -- 1. DATOS DE PERÍODO / CORTE
    SAFE_CAST(A.ano_seguimiento AS INT64) as ANIO,
    A.peri_idp                              as PERI_IDP,
    A.peri_nombre                           as PERI_NOMBRE,
    A.fecha_cierre                          as FECHA_CIERRE,
    A.fecha_apertura                        as FECHA_APERTURA,
    A.peri_datosapis                        as PERI_DATOSAPIS,
    A.peri_idperiodogobiernoactivo          as PERI_IDPERIODO_GOB_ACTIVO,
    A.fecha_lectura                         as FECHA_LECTURA,

    -- 2. PROGRAMA (Concatenación Código - Nombre)
    CONCAT(
        A.codigo_programa,
        ' - ',
        A.nombre_programa
    ) as PROGRAMA,

    -- 3. SUBPROGRAMA (Concatenación Código - Nombre con formato 00)
    CONCAT(
        LPAD(CAST(A.codigo_subprograma AS STRING), 2, '0'),
        ' - ',
        A.nombre_subprograma
    ) as SUBPROGRAMA,

    -- 4. MÉTRICAS DE METAS DE PRODUCTO
    SAFE_CAST(A.mpxsubp AS INT64)              as MP_X_SUBPROG,
    -- Promedio de Avance (Métrica de Gestión)
    SAFE_CAST(A.prom_avance_sub_mp AS FLOAT64) as PROM_AVANCE_SUBPROGRAMA_X_MP,
    -- Metas Programadas (Traído de la tabla auxiliar B)
    SAFE_CAST(B.cantidad_validos AS INT64)     as MP_PROGRAMADA_X_SUBPROGRAMA,

    -- 5. PONDERACIONES (Pesos Financieros/Estratégicos)
    SAFE_CAST(A.ponderacionsubprograma AS FLOAT64) as PONDERACION_SUBPROGRAMA,
    -- Avance Ponderado (Impacto real en el cumplimiento del plan)
    SAFE_CAST(A.avanceponderadosub AS FLOAT64)     as AVANCE_PONDERADO_SUBPROGRAMA

FROM {{ source('gold_evaplan', 'evaplan_api_avance_x_subprograma_processed_data') }} A
LEFT JOIN {{ source('gold_evaplan', 'evaplan_api_avance_subprogramas_processed_data') }} B
    ON A.peri_idp           = B.peri_idp
   AND A.ano_seguimiento    = B.ano_seguimiento
   AND A.codigo_programa    = B.fimr_programa
   AND A.codigo_subprograma = B.fimr_subprograma
WHERE A.ano_seguimiento IS NOT NULL
ORDER BY ANIO DESC, PROGRAMA, SUBPROGRAMA

