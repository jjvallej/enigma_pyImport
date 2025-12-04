test_dag_configuration_transform_ipm_sisben

Objetivo: Verificar que la configuración del DAG de transformación de IPM SISBEN se lee correctamente desde config.yaml.
Validaciones:
- DATASET_ID_SILVER se importa desde modules.config
- DATASET_ID_GOLD se importa desde modules.config
- DBT_PROJECT_DIR se calcula dinámicamente desde project_root
- get_dbt_command se importa desde modules.config
- DBT_MODEL_SILVER está definido como "ipm_sisben_stg"
- DBT_MODEL_GOLD está definido como "ipm_sisben_processed_data"
- Las tareas dbt utilizan get_dbt_command con los comandos correctos
- Todas las tareas dbt tienen append_env=True
- No hay valores hardcodeados en el código
- ensure_dataset se importa desde modules.ipm.ipm_sisben_load
  Estado esperado: ✅ PASS

