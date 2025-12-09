test_dag_configuration_ingest_ipm

Objetivo: Verificar que la configuración del DAG de ingesta de IPM se lee correctamente desde config.yaml.
Validaciones:
- DEFAULT_FOLDER_NAME se obtiene de CONF.ipm.gcs_folder
- DRIVE_URL se obtiene de CONF.ipm.drive_url
- DEFAULT_BUCKET_NAME se importa desde modules.config
- No hay valores hardcodeados en el código
- La función _upload_file_task utiliza los valores de configuración correctamente
- El bucket_name se asigna desde DEFAULT_BUCKET_NAME
- El folder_name se construye como f"{DEFAULT_FOLDER_NAME}/dane"
- destination_file_name se establece como None para extracción automática
  Estado esperado: ✅ PASS

