test_dag_sql_generation_load_ipm_sisben

Objetivo: Verificar que la función get_create_external_table_sql genera el SQL correcto para crear la tabla externa.
Validaciones:
- La función get_create_external_table_sql construye el GCS URI correctamente: f"gs://{GCS_BUCKET_NAME}/{GCS_PATH}"
- El SQL generado incluye CREATE EXTERNAL TABLE IF NOT EXISTS
- El SQL incluye el PROJECT_ID, DATASET_ID_BRONZE y TABLE_NAME correctos
- El SQL define todas las columnas necesarias con tipo STRING
- El SQL incluye la sección OPTIONS con format='CSV'
- El SQL incluye el URI de GCS en la sección uris
- El SQL incluye skip_leading_rows con el valor de configuración
- El SQL incluye field_delimiter con el valor de configuración
- El SQL incluye allow_quoted_newlines con el valor de configuración convertido a mayúsculas
- La función retorna el SQL como string sin espacios iniciales/finales
  Estado esperado: ✅ PASS

