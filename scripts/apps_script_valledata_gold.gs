/**
 * ==============================================================================
 * GOOGLE APPS SCRIPT: CONSUMO DE TABLAS GOLD DE BIGQUERY (VALLEDATA)
 * Proyecto GCP: datagov-477214
 * Dataset BigQuery: valledata
 * ==============================================================================
 *
 * REQUISITOS EN GOOGLE APPS SCRIPT:
 * 1. Abrir Google Sheets -> Extensiones -> Apps Script.
 * 2. En el panel izquierdo, ir a "Servicios" (+), buscar "BigQuery API" y agregar.
 * 3. Copiar y pegar este código en Code.gs.
 * 4. Guardar y recargar la hoja de cálculo. Aparecerá el menú "📊 ValleData Gold".
 */

// Configuración Global de GCP / BigQuery
var CONFIG = {
  PROJECT_ID: 'datagov-477214',
  DATASET_ID: 'valledata',
  PAGE_SIZE: 1000,
  TABLAS_GOLD: {
    COMENTARIOS_SENTIMIENTO: 'gold_comentarios_sentimiento',
    PRONOSTICO_PRODUCCION: 'gold_pronostico_produccion',
    RENDIMIENTO_AGRICOLA: 'gold_rendimiento',
    CULTIVOS_GEO: 'gold_cultivos_valle_geo'
  }
};

/**
 * Agrega el menú personalizado al abrir la hoja de cálculo de Google Sheets.
 * Nota: Se ejecuta automáticamente al abrir/recargar la hoja de cálculo.
 */
function onOpen(e) {
  try {
    var ui = SpreadsheetApp.getUi();
    ui.createMenu('📊 ValleData Gold')
      .addItem('🔄 Sincronizar Todas las Tablas Gold', 'importarTodasLasTablasGold')
      .addSeparator()
      .addItem('💬 Sincronizar Comentarios y Sentimiento', 'importarComentariosSentimiento')
      .addItem('📈 Sincronizar Pronóstico Producción (ARIMA 3 años)', 'importarPronosticoProduccion')
      .addItem('🌾 Sincronizar Rendimiento y Rentabilidad', 'importarRendimientoAgricola')
      .addItem('🗺️ Sincronizar Cultivos Georreferenciados', 'importarCultivosGeo')
      .addToUi();
  } catch (error) {
    Logger.log('ℹ️ Aviso: onOpen() se ejecutó desde el editor de código sin una interfaz gráfica activa de Sheets. Para probar manualmente desde el botón ▶ Ejecutar, selecciona la función "importarTodasLasTablasGold" o "importarComentariosSentimiento".');
  }
}

/**
 * Función principal para importar TODAS las tablas Gold a hojas separadas.
 */
function importarTodasLasTablasGold() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (ss) {
    ss.toast('Iniciando sincronización masiva de tablas Gold...', 'BigQuery', 5);
  }
  
  try {
    importarComentariosSentimiento();
    importarPronosticoProduccion();
    importarRendimientoAgricola();
    importarCultivosGeo();
    
    if (ss && SpreadsheetApp.getUi) {
      try {
        SpreadsheetApp.getUi().alert('✅ Sincronización Exitosa', 'Todas las tablas Gold han sido importadas desde BigQuery a Google Sheets.', SpreadsheetApp.getUi().ButtonSet.OK);
      } catch (uiErr) {
        Logger.log('Sincronización masiva finalizada correctamente.');
      }
    }
  } catch (error) {
    Logger.log('Error en sincronización masiva: ' + error.toString());
    try {
      SpreadsheetApp.getUi().alert('❌ Error de Sincronización', error.toString(), SpreadsheetApp.getUi().ButtonSet.OK);
    } catch (uiErr) {
      // Ignorar si se ejecuta sin UI
    }
  }
}

/**
 * Importa la vista gold_comentarios_sentimiento
 */
function importarComentariosSentimiento() {
  var query = 'SELECT municipio, id_dataset, nombre_dataset, total_comentarios, positivos, negativos, neutros, confianza_promedio, emocion_predominante ' +
              'FROM `' + CONFIG.PROJECT_ID + '.' + CONFIG.DATASET_ID + '.' + CONFIG.TABLAS_GOLD.COMENTARIOS_SENTIMIENTO + '` ' +
              'ORDER BY total_comentarios DESC';
  ejecutarConsultaEImportarHoja('gold_comentarios_sentimiento', query);
}

/**
 * Importa la tabla gold_modelo_pronostico_produccion
 */
function importarPronosticoProduccion() {
  var query = 'SELECT * FROM `' + CONFIG.PROJECT_ID + '.' + CONFIG.DATASET_ID + '.' + CONFIG.TABLAS_GOLD.PRONOSTICO_PRODUCCION + '`';
  ejecutarConsultaEImportarHoja('gold_pronostico_produccion', query);
}

/**
 * Importa la tabla gold_modelo_rendimiento
 */
function importarRendimientoAgricola() {
  var query = 'SELECT * FROM `' + CONFIG.PROJECT_ID + '.' + CONFIG.DATASET_ID + '.' + CONFIG.TABLAS_GOLD.RENDIMIENTO_AGRICOLA + '`';
  ejecutarConsultaEImportarHoja('gold_modelo_rendimiento', query);
}

/**
 * Importa la tabla gold_cultivos_valle_geo
 */
function importarCultivosGeo() {
  var query = 'SELECT * FROM `' + CONFIG.PROJECT_ID + '.' + CONFIG.DATASET_ID + '.' + CONFIG.TABLAS_GOLD.CULTIVOS_GEO + '`';
  ejecutarConsultaEImportarHoja('gold_cultivos_valle_geo', query);
}

/**
 * Función genérica que consulta BigQuery mediante la API de Apps Script y escribe los resultados en una pestaña de Google Sheets.
 *
 * @param {string} nombreHoja - Nombre de la pestaña destino en la hoja de cálculo.
 * @param {string} sqlQuery - Consulta SQL a ejecutar en BigQuery.
 */
function ejecutarConsultaEImportarHoja(nombreHoja, sqlQuery) {
  Logger.log('Ejecutando consulta BigQuery para: ' + nombreHoja);
  
  var request = {
    query: sqlQuery,
    useLegacySql: false
  };

  var queryResults;
  try {
    queryResults = BigQuery.Jobs.query(request, CONFIG.PROJECT_ID);
  } catch (e) {
    throw new Error('Falló la consulta BigQuery para ' + nombreHoja + ': ' + e.message);
  }

  var jobId = queryResults.jobReference.jobId;

  // Esperar si la consulta tarda en procesar
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    queryResults = BigQuery.Jobs.getQueryResults(CONFIG.PROJECT_ID, jobId);
  }

  // Obtener todos los resultados (con paginación si supera el tamaño de página)
  var rows = queryResults.rows || [];
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(CONFIG.PROJECT_ID, jobId, {
      pageToken: queryResults.pageToken
    });
    if (queryResults.rows) {
      rows = rows.concat(queryResults.rows);
    }
  }

  if (rows.length === 0) {
    Logger.log('La consulta no devolvió resultados para: ' + nombreHoja);
  }

  // Preparar o buscar la pestaña en la hoja de cálculo activa
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    throw new Error('No se encontró una Hoja de Cálculo activa. Asegúrate de vincular este script desde Extensiones -> Apps Script en Google Sheets.');
  }

  var sheet = ss.getSheetByName(nombreHoja);
  if (!sheet) {
    sheet = ss.insertSheet(nombreHoja);
  } else {
    sheet.clearContents();
  }

  // Obtener los nombres de los encabezados (columnas)
  var headers = [];
  var fields = queryResults.schema.fields;
  for (var i = 0; i < fields.length; i++) {
    headers.push(fields[i].name);
  }

  // Convertir las filas devueltas por la API de BigQuery a matriz 2D
  var dataMatrix = [];
  dataMatrix.push(headers);

  for (var r = 0; r < rows.length; r++) {
    var rowValues = [];
    var f = rows[r].f;
    for (var c = 0; c < f.length; c++) {
      var val = f[c].v;
      rowValues.push(val !== null ? val : '');
    }
    dataMatrix.push(rowValues);
  }

  // Escribir la matriz completa en la hoja de cálculo
  if (dataMatrix.length > 0) {
    var range = sheet.getRange(1, 1, dataMatrix.length, headers.length);
    range.setValues(dataMatrix);

    // Formatear la fila de encabezados
    var headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#1A73E8');
    headerRange.setFontColor('#FFFFFF');
    sheet.setFrozenRows(1);
    
    // Auto-ajustar ancho de columnas si hay datos
    if (dataMatrix.length < 500) {
      for (var col = 1; col <= headers.length; col++) {
        sheet.autoResizeColumn(col);
      }
    }
  }

  Logger.log('✅ Importación completada en pestaña: ' + nombreHoja + ' (' + (dataMatrix.length - 1) + ' filas).');
}

/**
 * Web App REST Endpoint (opcional)
 * Permite consultar cualquier tabla Gold vía HTTP GET devolviendo un payload JSON.
 * Ejemplo: https://script.google.com/macros/s/.../exec?table=gold_comentarios_sentimiento
 */
function doGet(e) {
  var tableName = (e && e.parameter && e.parameter.table) ? e.parameter.table : CONFIG.TABLAS_GOLD.COMENTARIOS_SENTIMIENTO;
  
  try {
    var query = 'SELECT * FROM `' + CONFIG.PROJECT_ID + '.' + CONFIG.DATASET_ID + '.' + tableName + '` LIMIT 500';
    var request = { query: query, useLegacySql: false };
    var queryResults = BigQuery.Jobs.query(request, CONFIG.PROJECT_ID);
    
    var fields = queryResults.schema.fields;
    var rows = queryResults.rows || [];
    
    var resultList = [];
    for (var i = 0; i < rows.length; i++) {
      var item = {};
      for (var j = 0; j < fields.length; j++) {
        item[fields[j].name] = rows[i].f[j].v;
      }
      resultList.push(item);
    }
    
    return ContentService.createTextOutput(JSON.stringify({
      status: 'success',
      table: tableName,
      count: resultList.length,
      data: resultList
    })).setMimeType(ContentService.MimeType.JSON);
    
  } catch (error) {
    return ContentService.createTextOutput(JSON.stringify({
      status: 'error',
      message: error.toString()
    })).setMimeType(ContentService.MimeType.JSON);
  }
}
