test_dag_configuration_load_idc_dictionary

Objetivo: Verificar que la configuración del DAG de carga del diccionario IDC se lee correctamente desde config.yaml.
Validaciones:
- GCS_BUCKET_NAME se obtiene de DEFAULT_BUCKET_NAME
- GCS_FOLDER_PATH se obtiene de CONF.idc.gcs_folder
- TABLE_NAME se obtiene de CONF.idc.tables.dictionary
- DATASET_ID_GOLD se importa desde modules.config
- No hay valores hardcodeados en el código
- La función _download_csv_task utiliza get_latest_csv_from_gcs_folder correctamente
- La función _load_csv_task utiliza load_csv_to_bq con el dataset y tabla correctos
  Estado esperado: ✅ PASS

