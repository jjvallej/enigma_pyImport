test_dag_configuration_ingest_idc_dictionary

Objetivo: Verificar que la configuración del DAG de ingesta del diccionario IDC se lee correctamente desde config.yaml.
Validaciones:
- DEFAULT_FOLDER_NAME se obtiene de CONF.idc.gcs_folder
- DRIVE_URL se obtiene de CONF.idc.dictionary_drive_url
- DEFAULT_BUCKET_NAME se importa desde modules.config
- No hay valores hardcodeados en el código
- La función _upload_file_task utiliza los valores de configuración correctamente
- El bucket_name se asigna desde DEFAULT_BUCKET_NAME
- El folder_name se asigna desde DEFAULT_FOLDER_NAME
- destination_file_name se establece como None para extracción automática
  Estado esperado: ✅ PASS

