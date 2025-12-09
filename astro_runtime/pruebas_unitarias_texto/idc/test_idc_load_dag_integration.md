test_dag_integration_load_idc

Objetivo: Verificar la integración completa del DAG de carga de IDC con BigQuery.
Validaciones:
- El dataset bronze se crea correctamente si no existe
- El archivo Excel se descarga correctamente desde GCS
- Las 3 hojas del Excel se procesan correctamente (eliminando primera hoja y primera fila de cada hoja restante)
- Las 3 tablas se crean correctamente en BigQuery: idc_raw_data_dato_original, idc_raw_data_valor_normalizado, idc_raw_data_valor_ranking
- Los datos se cargan correctamente en cada tabla según el mapeo de hojas
- Los archivos temporales se eliminan correctamente después de la carga
- El DAG de transformación se dispara correctamente al finalizar
  Estado esperado: ✅ PASS

