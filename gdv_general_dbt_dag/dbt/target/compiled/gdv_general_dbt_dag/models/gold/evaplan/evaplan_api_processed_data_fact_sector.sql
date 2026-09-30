

-- Modelo gold: FACT_SECTOR
-- Agrega datos de SectorMP con métricas de avance por sector
-- Incluye rangos de avance, porcentajes y calificación

SELECT
    -- Campos de identificación y periodos (en MAYÚSCULAS)
    peri_idp AS ANIO,
    UPPER(peri_nombre) AS PERI_NOMBRE,
    UPPER(CAST(fecha_cierre AS STRING)) AS FECHA_CIERRE,
    UPPER(CAST(fecha_apertura AS STRING)) AS FECHA_APERTURA,
    UPPER(peri_datosapis) AS PERI_DATOSAPIS,
    UPPER(CAST(ano_seguimiento AS STRING)) AS ANO_SEGUIMIENTO,
    UPPER(CAST(peri_idperiodogobiernoactivo AS STRING)) AS PERI_IDPERIODOGOBIERNOACTIVO,
    fecha_lectura AS FECHA_LECTURA,

    -- Mapeo de dimensiones y conversión a numéricos
    CAST(sector_mga AS FLOAT64) AS COD_MGA,
    CAST(sector_sap AS FLOAT64) AS COD_SAP,
    UPPER(nombre_sector_sap) AS SECTOR,
    
    -- Métricas base
    CAST(cant_mp AS FLOAT64) AS CAN_MP,
    CAST(cantidad_np AS FLOAT64) AS CANTIDAD_NP,
    CAST(cantidad_programadas AS FLOAT64) AS MP_PROGRAMADAS,
    
    -- Rangos de Avance (Cantidades)
    CAST(mayores_iguales_80 AS FLOAT64) AS `AVANCE_>=_80_CANT`,
    CAST(menores_80_mayores_25 AS FLOAT64) AS `AVANCE_>=25_<80_CANT`,
    CAST(menores_25 AS FLOAT64) AS `AVANCE_<25_CANT`,

    -- Porcentajes (Calculados)
    SAFE_DIVIDE(CAST(mayores_iguales_80 AS FLOAT64), CAST(cant_mp AS FLOAT64)) * 100 AS `AVANCE_>=_80_PORC`,
    SAFE_DIVIDE(CAST(menores_80_mayores_25 AS FLOAT64), CAST(cant_mp AS FLOAT64)) * 100 AS `AVANCE_>=25_<80_PORC`,
    SAFE_DIVIDE(CAST(menores_25 AS FLOAT64), CAST(cant_mp AS FLOAT64)) * 100 AS `AVANCE_< 25_PORC`,

    -- Calificación
    CAST(calificacion_mp AS FLOAT64) AS `CALIFICACIÓN_x_MP_x_CORTE`

FROM `datagov-473122`.`gold_dpt_planeacion_municipal_dev`.`evaplan_api_sector_mp_processed_data`