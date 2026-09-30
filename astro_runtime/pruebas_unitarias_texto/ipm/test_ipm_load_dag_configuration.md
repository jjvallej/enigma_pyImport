test_dag_configuration_load_ipm

Objetivo: Verificar que la configuración del DAG de carga de IPM se lee correctamente desde config.yaml.
Validaciones:
- GCS_BUCKET_NAME se obtiene de DEFAULT_BUCKET_NAME desde modules.config
- GCS_FOLDER_PATH se obtiene de CONF.ipm.gcs_folder
- TABLE_NAME_BRONZE se obtiene de CONF.ipm.tables.bronze
- DATASET_ID_BRONZE se importa desde modules.config
- SHEET_INDEX está configurado como 0
- Las funciones utilizan los valores de configuración correctamente
- No hay valores hardcodeados en el código
- La función _download_excel_task intenta usar Variables de Airflow con fallback a valores por defecto
  Estado esperado: ✅ PASS

