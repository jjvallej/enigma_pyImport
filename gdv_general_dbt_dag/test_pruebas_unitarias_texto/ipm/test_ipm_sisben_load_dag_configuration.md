test_dag_configuration_load_ipm_sisben

Objetivo: Verificar que la configuración del DAG de carga de IPM SISBEN se lee correctamente desde config.yaml.
Validaciones:
- TABLE_NAME se obtiene de CONF.ipm_sisben.tables.bronze
- GCS_BUCKET_NAME se obtiene de DEFAULT_BUCKET_NAME desde modules.config
- GCS_PATH se obtiene de CONF.ipm_sisben.external_table.gcs_path
- SKIP_LEADING_ROWS se obtiene de CONF.ipm_sisben.external_table.skip_leading_rows
- FIELD_DELIMITER se obtiene de CONF.ipm_sisben.external_table.field_delimiter
- ALLOW_QUOTED_NEWLINES se obtiene de CONF.ipm_sisben.external_table.allow_quoted_newlines
- PROJECT_ID se importa desde modules.config
- LOCATION se importa desde modules.config
- DATASET_ID_BRONZE se importa desde modules.config
- La función get_create_external_table_sql utiliza todos los parámetros de configuración
- No hay valores hardcodeados en el código
  Estado esperado: ✅ PASS

