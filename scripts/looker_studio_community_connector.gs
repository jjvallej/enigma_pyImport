/**
 * ==============================================================================
 * LOOKER STUDIO (DATA STUDIO) COMMUNITY CONNECTOR: VALLEDATA SENTIMIENTOS
 * Proyecto GCP: datagov-477214 | Dataset: valledata
 * Tabla: valledata.gold.gold_comentarios_sentimiento
 * ==============================================================================
 * Este conector se ejecuta DENTRO de Looker Studio mediante Google Apps Script
 * y disponibiliza los 9 KPIs (especialmente KPI-07, KPI-08 y KPI-09).
 */

var cc = DataStudioApp.createCommunityConnector();

function getAuthType() {
  return cc.newAuthTypeResponse()
    .setAuthType(cc.AuthType.NONE)
    .build();
}

function getConfig(request) {
  var config = cc.getConfig();
  
  config.newInfo()
    .setId('instructions')
    .setText('Conector oficial de ValleDATA para consumo de Sentimientos en Looker Studio (BigQuery ID: datagov-477214.valledata).');
    
  config.newTextInput()
    .setId('projectId')
    .setName('ID del Proyecto GCP')
    .setPlaceholder('datagov-477214')
    .setDefaultValue('datagov-477214');

  config.newTextInput()
    .setId('datasetId')
    .setName('Dataset BigQuery')
    .setPlaceholder('valledata')
    .setDefaultValue('valledata');

  return config.build();
}

function getFields() {
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;

  // Dimensiones básicas
  fields.newDimension()
    .setId('municipio')
    .setName('Municipio')
    .setType(types.STRING);

  fields.newDimension()
    .setId('id_municipio')
    .setName('Código DIVIPOLA')
    .setType(types.NUMBER);

  fields.newDimension()
    .setId('id_dataset')
    .setName('ID Dataset / Temática')
    .setType(types.STRING);

  // MÈTRICAS Y KPIS

  // KPI-01: Volumen total
  fields.newMetric()
    .setId('kpi_01_volumen')
    .setName('KPI-01: Volumen Total Comentarios')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);

  // KPI-02: NSS (%)
  fields.newMetric()
    .setId('kpi_02_nss')
    .setName('KPI-02: Net Sentiment Score (NSS %)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);

  // KPI-03: Tasa de Criticidad (%)
  fields.newMetric()
    .setId('kpi_03_criticidad')
    .setName('KPI-03: Tasa de Criticidad (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);

  // KPI-04: Confianza IA Ponderada
  fields.newMetric()
    .setId('kpi_04_confianza')
    .setName('KPI-04: Confianza IA Ponderada')
    .setType(types.NUMBER)
    .setAggregation(aggregations.AVG);

  // KPI-05: Participación Positiva
  fields.newMetric()
    .setId('kpi_05_positivo')
    .setName('KPI-05: % Positivo')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);

  // KPI-06: Participación Neutra
  fields.newMetric()
    .setId('kpi_06_neutro')
    .setName('KPI-06: % Neutro')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);

  // KPI-07: Municipios en Estado CRÍTICO ⭐
  fields.newMetric()
    .setId('kpi_07_municipios_criticos')
    .setName('KPI-07: Municipios en Estado CRÍTICO (NSS < 0)')
    .setDescription('Conteo de municipios cuyo conteo de comentarios negativos supera a los positivos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.COUNT_DISTINCT);

  // KPI-08: Datasets Bajo Umbral de Revisión ⭐
  fields.newMetric()
    .setId('kpi_08_datasets_revision')
    .setName('KPI-08: Datasets Bajo Umbral de Revisión')
    .setDescription('Conteo de datasets con confianza probabilística < 0.70 o criticidad > 25%')
    .setType(types.NUMBER)
    .setAggregation(aggregations.COUNT_DISTINCT);

  // KPI-09: Emoción Predominante ⭐
  fields.newDimension()
    .setId('kpi_09_emocion_predominante')
    .setName('KPI-09: Emoción Predominante')
    .setDescription('Categoría de sentimiento predominante (Positivo, Negativo, Neutro)')
    .setType(types.STRING);

  return fields;
}

function getSchema(request) {
  return { schema: getFields().build() };
}

function getData(request) {
  var projectId = request.configParams.projectId || 'datagov-477214';
  var datasetId = request.configParams.datasetId || 'valledata';

  var query = 'SELECT municipio, id_municipio, id_dataset, total_comentarios, positivos, negativos, neutros, confianza_promedio, emocion_predominante ' +
              'FROM `' + projectId + '.' + datasetId + '.gold_comentarios_sentimiento`';

  var bqRequest = BigQuery.newQueryRequest();
  bqRequest.query = query;
  bqRequest.useLegacySql = false;

  var queryResults = BigQuery.Jobs.query(bqRequest, projectId);
  var rows = queryResults.rows || [];

  var requestedFields = getFields().forIds(request.fields.map(function(field) { return field.name; }));

  var data = rows.map(function(row) {
    var f = row.f;
    var municipio = f[0].v;
    var id_municipio = Number(f[1].v);
    var id_dataset = f[2].v;
    var total_comentarios = Number(f[3].v);
    var positivos = Number(f[4].v);
    var negativos = Number(f[5].v);
    var neutros = Number(f[6].v);
    var confianza_promedio = Number(f[7].v);
    var emocion_raw = f[8].v;

    var nss = total_comentarios > 0 ? (positivos - negativos) / total_comentarios : 0;
    var criticidad = total_comentarios > 0 ? negativos / total_comentarios : 0;
    var pct_pos = total_comentarios > 0 ? positivos / total_comentarios : 0;
    var pct_neu = total_comentarios > 0 ? neutros / total_comentarios : 0;

    // KPI-07
    var es_critico = (positivos - negativos) < 0 ? municipio : null;
    
    // KPI-08
    var es_bajo_umbral = (confianza_promedio < 0.70 || criticidad > 0.25) ? id_dataset : null;

    // KPI-09
    var emocion_label = 'Neutro';
    if (emocion_raw === 'POS' || emocion_raw === 'positivo') emocion_label = 'Positivo';
    if (emocion_raw === 'NEG' || emocion_raw === 'negativo') emocion_label = 'Negativo';

    var values = [];
    request.fields.forEach(function(field) {
      switch (field.name) {
        case 'municipio': values.push(municipio); break;
        case 'id_municipio': values.push(id_municipio); break;
        case 'id_dataset': values.push(id_dataset); break;
        case 'kpi_01_volumen': values.push(total_comentarios); break;
        case 'kpi_02_nss': values.push(nss); break;
        case 'kpi_03_criticidad': values.push(criticidad); break;
        case 'kpi_04_confianza': values.push(confianza_promedio); break;
        case 'kpi_05_positivo': values.push(pct_pos); break;
        case 'kpi_06_neutro': values.push(pct_neu); break;
        case 'kpi_07_municipios_criticos': values.push(es_critico); break;
        case 'kpi_08_datasets_revision': values.push(es_bajo_umbral); break;
        case 'kpi_09_emocion_predominante': values.push(emocion_label); break;
        default: values.push(null);
      }
    });
    return { values: values };
  });

  return {
    schema: requestedFields.build(),
    rows: data
  };
}
