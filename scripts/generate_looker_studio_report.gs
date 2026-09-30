/**
 * ==============================================================================
 * GENERADOR DE REPORTE Y PLANTILLA LOOKER STUDIO (DATA STUDIO) - VALLEDATA
 * Proyecto GCP: datagov-477214 | Dataset: valledata
 * Fuente: valledata.gold.gold_comentarios_sentimiento
 * ==============================================================================
 * Este script se puede ejecutar en Google Apps Script para generar la URL
 * preconfigurada con el esquema oficial de los 9 KPIs y 6 Páginas.
 */

var VALLEDATA_CONFIG = {
  PROJECT_ID: 'datagov-477214',
  DATASET_ID: 'valledata',
  TABLE_GOLD: 'gold_comentarios_sentimiento',
  VIEW_9KPIS: 'gold_vw_datastudio_sentimiento_9kpis',
  VIEW_POLARIDAD: 'gold_vw_datastudio_polaridad_unpivot',
  DEFAULT_PARAMS: {
    P_UMBRAL_CONF: 0.70,
    P_CONF_OPTIMO: 0.80,
    P_CRIT_ALERTA: 30,
    P_CRIT_CRITICO: 50,
    P_TOP_N: 10,
    P_MIN_VOLUMEN: 20
  }
};

/**
 * Función principal para obtener el enlace directo de creación de informe en Looker Studio.
 * Copiar la URL generada y abrirla en la ventana del navegador con la cuenta logueada.
 */
function generarEnlaceDirectoLookerStudio() {
  var bqTableUrl = 'https://lookerstudio.google.com/reporting/create?' +
    'c.reportId=' + encodeURIComponent('valledata_sentimientos_v2') +
    '&ds.ds0.connector=bigQuery' +
    '&ds.ds0.projectId=' + encodeURIComponent(VALLEDATA_CONFIG.PROJECT_ID) +
    '&ds.ds0.datasetId=' + encodeURIComponent(VALLEDATA_CONFIG.DATASET_ID) +
    '&ds.ds0.tableId=' + encodeURIComponent(VALLEDATA_CONFIG.VIEW_9KPIS) +
    '&ds.ds0.type=VIEW';

  Logger.log('==============================================================================');
  Logger.log('🚀 ENLACE DE CREACIÓN DE INFORME LOOKER STUDIO PARA CUENTA LOGUEADA:');
  Logger.log(bqTableUrl);
  Logger.log('==============================================================================');
  
  return bqTableUrl;
}

/**
 * Retorna la especificación completa del diccionario de datos y los 9 KPIs
 * para importación directa en la fuente de datos de Looker Studio.
 */
function obtenerCatalogoCamposCalculados() {
  return [
    {
      nombre: "NSS",
      tipo: "Número",
      formula: "((SUM(positivos) - SUM(negativos)) / NULLIF(SUM(total_comentarios), 0)) * 100",
      descripcion: "KPI-02: Net Sentiment Score (-100% a +100%)"
    },
    {
      nombre: "Tasa criticidad",
      tipo: "Número",
      formula: "(SUM(negativos) / NULLIF(SUM(total_comentarios), 0)) * 100",
      descripcion: "KPI-03: Porcentaje de comentarios negativos"
    },
    {
      nombre: "Confianza IA ponderada",
      tipo: "Número",
      formula: "SUM(confianza_promedio * total_comentarios) / NULLIF(SUM(total_comentarios), 0)",
      descripcion: "KPI-04: Confianza promedio ponderada por volumen"
    },
    {
      nombre: "KPI-07: Municipios en estado CRÍTICO",
      tipo: "Número",
      formula: "COUNT_DISTINCT(CASE WHEN (positivos - negativos) < 0 THEN municipio END)",
      descripcion: "Conteo de municipios cuyo conteo de comentarios negativos supera a los positivos"
    },
    {
      nombre: "KPI-08: Datasets bajo umbral de revisión",
      tipo: "Número",
      formula: "COUNT_DISTINCT(CASE WHEN confianza_promedio < 0.70 OR (negativos / NULLIF(total_comentarios, 0)) > 0.25 THEN id_dataset END)",
      descripcion: "Conteo de datasets con confianza probabilística < 0.70 o criticidad > 25%"
    },
    {
      nombre: "KPI-09: Etiqueta emoción / Sentimiento Predominante",
      tipo: "Texto",
      formula: "CASE WHEN SUM(positivos) >= SUM(negativos) AND SUM(positivos) >= SUM(neutros) THEN 'Positivo' WHEN SUM(negativos) >= SUM(positivos) AND SUM(negativos) >= SUM(neutros) THEN 'Negativo' ELSE 'Neutro' END",
      descripcion: "Clasificación de emoción predominante del universo filtrado"
    },
    {
      nombre: "Estado semáforo",
      tipo: "Texto",
      formula: "CASE WHEN Tasa criticidad > 50 THEN 'CRÍTICO' WHEN Tasa criticidad >= 30 THEN 'ALERTA' ELSE 'NORMAL' END",
      descripcion: "Evaluación semafórica de criticidad"
    },
    {
      nombre: "Estado confianza IA",
      tipo: "Texto",
      formula: "CASE WHEN Confianza IA ponderada >= 0.80 THEN 'ÓPTIMO' WHEN Confianza IA ponderada >= 0.70 THEN 'ACEPTABLE' ELSE 'REQUIERE REVISIÓN' END",
      descripcion: "Evaluación semafórica de confianza del modelo"
    }
  ];
}
