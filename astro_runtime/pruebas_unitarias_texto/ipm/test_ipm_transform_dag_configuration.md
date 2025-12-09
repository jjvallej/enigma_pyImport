test_dag_configuration_transform_ipm

Objetivo: Verificar que la configuración del DAG de transformación de IPM se lee correctamente desde config.yaml.
Validaciones:
- TABLE_NAME_SILVER se obtiene de CONF.ipm.tables.silver
- TABLE_NAME_GOLD se obtiene de CONF.ipm.tables.gold
- DATASET_ID_SILVER se importa desde modules.config
- DATASET_ID_GOLD se importa desde modules.config
- DBT_PROJECT_DIR se calcula dinámicamente desde project_root
- get_dbt_command se importa desde modules.config
- Las tareas dbt utilizan get_dbt_command con los comandos correctos
- Todas las tareas dbt tienen append_env=True
- La tarea dbt_run_transform_types tiene execution_timeout de 15 minutos
- No hay valores hardcodeados en el código
  Estado esperado: ✅ PASS

