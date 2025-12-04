test_dag_configuration_ingest_ipm_sisben

Objetivo: Verificar que la configuración del DAG de ingesta de IPM SISBEN se lee correctamente desde config.yaml.
Validaciones:
- SOURCE_FOLDER se obtiene de CONF.ipm_sisben.gcs_temp_folder
- DESTINATION_FOLDER se obtiene de CONF.ipm_sisben.gcs_folder
- DEFAULT_BUCKET_NAME se importa desde modules.config
- No hay valores hardcodeados en el código
- La función _move_file_task utiliza los valores de configuración correctamente
- El bucket_name se asigna desde DEFAULT_BUCKET_NAME
- El source_folder se asigna desde SOURCE_FOLDER
- El destination_folder se asigna desde DESTINATION_FOLDER
- El file_pattern se establece como "*.xlsx" para solo archivos Excel
  Estado esperado: ✅ PASS

