

-- Modelo gold: Une la tabla de periodos con la tabla de avance_programas
-- JOIN por peri_idp para tener toda la información del periodo junto con los datos de avance_programas
-- Desanida el campo detalle_programas_json para crear una fila por cada programa

SELECT
    -- Campos de periodos
    p.peri_idp AS peri_idp,
    p.peri_nombre AS peri_nombre,
    p.fecha_cierre AS fecha_cierre,
    p.fecha_apertura AS fecha_apertura,
    p.peri_datosapis AS peri_datosapis,
    p.ano_seguimiento AS ano_seguimiento,
    p.peri_idperiodogobiernoactivo AS peri_idperiodogobiernoactivo,
    p.fecha_lectura AS fecha_lectura,
    -- Campos de avance_programas (sin detalle_programas_json que se desanida)
    a.linea_estrategica AS linea_estrategica,
    a.pg_suma_cumpl_linea_estrategica AS pg_suma_cumpl_linea_estrategica,
    a.pa_promedio_cumpl_linea_estrategica AS pa_promedio_cumpl_linea_estrategica,
    -- Campos desanidados y renombrados a snake_case del JSON
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.MPxSUBP') AS INT64) AS mpxsubp,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.popr_valor') AS FLOAT64) AS popr_valor,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.CANTIDAD_NP') AS INT64) AS cantidad_np,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.CUMP_PROGRAMA') AS FLOAT64) AS cump_programa,
    JSON_VALUE(detalle_programa, '$.fimr_programa') AS fimr_programa,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.APORTE_CUMPL_MR') AS FLOAT64) AS aporte_cumpl_mr,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.CANTIDAD_VALIDOS') AS INT64) AS cantidad_validos,
    JSON_VALUE(detalle_programa, '$.fimr_subprograma') AS fimr_subprograma,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.PROM_AVANCE_SUB_MP') AS FLOAT64) AS prom_avance_sub_mp,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.AVANCE_PONDERADO_SUB') AS FLOAT64) AS avance_ponderado_sub,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.PONDERACION_SUBPROGRAMA') AS FLOAT64) AS ponderacion_subprograma,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.APORTE_CUMPL_SUBPROGRAMA') AS FLOAT64) AS aporte_cumpl_subprograma,
    SAFE_CAST(JSON_VALUE(detalle_programa, '$.AVANCE_PONDERADO_PROGRAMA') AS FLOAT64) AS avance_ponderado_programa
FROM `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`evaplan_api_periodos_transformed_data` p
INNER JOIN `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`evaplan_api_avance_programas_transformed_data` a
  ON CAST(p.peri_idp AS INT64) = CAST(a.peri_idp AS INT64)
CROSS JOIN UNNEST(JSON_QUERY_ARRAY(a.detalle_programas_json)) AS detalle_programa