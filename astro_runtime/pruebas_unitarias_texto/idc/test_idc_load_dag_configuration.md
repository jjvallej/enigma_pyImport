test_dag_configuration_load_idc

Objetivo: Verificar que la configuración del DAG de carga de IDC se lee correctamente desde config.yaml.
Validaciones:
- GCS_BUCKET_NAME se obtiene de DEFAULT_BUCKET_NAME
- GCS_FOLDER_PATH se obtiene de CONF.idc.gcs_folder
- SHEET_TO_TABLE_MAPPING se obtiene de CONF.idc.tables.raw_data_mapping
- DATASET_ID_BRONZE se importa desde modules.config
- No hay valores hardcodeados en el código
- El mapeo de hojas a tablas contiene: Dato_original, Valor_normalizado, Valor_ranking
- La función _download_excel_task utiliza get_latest_excel_from_gcs_folder correctamente
- La función _load_all_sheets_task utiliza load_all_sheets_to_bq con el mapeo correcto
  Estado esperado: ✅ PASS

