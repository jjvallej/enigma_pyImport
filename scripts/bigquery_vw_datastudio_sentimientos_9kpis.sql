-- ==============================================================================
-- VALLEDATA - VISTAS BIGQUERY OPTIMIZADAS PARA GOOGLE LOOKER STUDIO (DATA STUDIO)
-- Proyecto GCP: datagov-477214 | Dataset: valledata
-- Vistas:
--   1. valledata.gold_vw_datastudio_sentimiento_9kpis (DS_GOLD)
--   2. valledata.gold_vw_datastudio_polaridad_unpivot (DS_POLARIDAD)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- VISTA 1: DS_GOLD - Consolidado principal con 9 KPIs
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `datagov-477214.valledata.gold_vw_datastudio_sentimiento_9kpis` AS
WITH base AS (
    SELECT
        municipio,
        id_municipio,
        id_dataset,
        COALESCE(id_dataset, 'Desconocido') AS nombre_dataset,
        total_comentarios,
        positivos,
        negativos,
        neutros,
        confianza_promedio,
        emocion_predominante,
        
        -- KPI-02: NSS por fila (municipio x dataset)
        SAFE_DIVIDE(positivos - negativos, total_comentarios) * 100 AS nss_fila,
        
        -- KPI-03: Tasa de criticidad por fila
        SAFE_DIVIDE(negativos, total_comentarios) * 100 AS tasa_criticidad_fila,
        
        -- Flag para KPI-07: Municipio en estado CRÍTICO (NSS < 0)
        CASE WHEN (positivos - negativos) < 0 THEN 1 ELSE 0 END AS es_municipio_critico,
        
        -- Flag para KPI-08: Dataset bajo umbral de revisión (confianza < 0.70 O criticidad > 25%)
        CASE WHEN confianza_promedio < 0.70 OR SAFE_DIVIDE(negativos, total_comentarios) > 0.25 THEN 1 ELSE 0 END AS es_dataset_bajo_umbral,
        
        -- Homologación de etiqueta de emoción para UI (KPI-09)
        CASE 
            WHEN UPPER(emocion_predominante) IN ('POS', 'POSITIVO') THEN 'Positivo'
            WHEN UPPER(emocion_predominante) IN ('NEG', 'NEGATIVO') THEN 'Negativo'
            WHEN UPPER(emocion_predominante) IN ('NEU', 'NEUTRO') THEN 'Neutro'
            ELSE 'Sin Dato'
        END AS etiqueta_emocion_predominante
    FROM `datagov-477214.valledata.gold_comentarios_sentimiento`
)
SELECT
    municipio,
    id_municipio,
    id_dataset,
    nombre_dataset,
    total_comentarios,
    positivos,
    negativos,
    neutros,
    confianza_promedio,
    emocion_predominante,
    etiqueta_emocion_predominante,
    
    -- --------------------------------------------------------------------------
    -- CATÁLOGO DE LOS 9 KPIS DISPONIBLES DIRECTAMENTE EN LOOKER STUDIO
    -- --------------------------------------------------------------------------
    total_comentarios AS kpi_01_volumen_comentarios,
    ROUND(nss_fila, 2) AS kpi_02_nss_porcentaje,
    ROUND(tasa_criticidad_fila, 2) AS kpi_03_tasa_criticidad_porcentaje,
    confianza_promedio AS kpi_04_confianza_ia,
    ROUND(SAFE_DIVIDE(positivos, total_comentarios) * 100, 2) AS kpi_05_porcentaje_positivo,
    ROUND(SAFE_DIVIDE(neutros, total_comentarios) * 100, 2) AS kpi_06_porcentaje_neutro,
    es_municipio_critico AS kpi_07_es_municipio_critico,
    es_dataset_bajo_umbral AS kpi_08_es_dataset_bajo_umbral,
    etiqueta_emocion_predominante AS kpi_09_emocion_predominante
FROM base;

-- ------------------------------------------------------------------------------
-- VISTA 2: DS_POLARIDAD - SQL Unpivot para Dona de Polaridad (Sección 3.2)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `datagov-477214.valledata.gold_vw_datastudio_polaridad_unpivot` AS
SELECT municipio, id_municipio, id_dataset, 'POS' AS sentimiento, 'Positivo' AS etiqueta_sentimiento, positivos AS n, total_comentarios, confianza_promedio, emocion_predominante
FROM `datagov-477214.valledata.gold_comentarios_sentimiento`
UNION ALL
SELECT municipio, id_municipio, id_dataset, 'NEG' AS sentimiento, 'Negativo' AS etiqueta_sentimiento, negativos AS n, total_comentarios, confianza_promedio, emocion_predominante
FROM `datagov-477214.valledata.gold_comentarios_sentimiento`
UNION ALL
SELECT municipio, id_municipio, id_dataset, 'NEU' AS sentimiento, 'Neutro' AS etiqueta_sentimiento, neutros AS n, total_comentarios, confianza_promedio, emocion_predominante
FROM `datagov-477214.valledata.gold_comentarios_sentimiento`;

