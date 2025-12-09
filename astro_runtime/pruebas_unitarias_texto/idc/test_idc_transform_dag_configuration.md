test_dag_configuration_transform_idc

Objetivo: Verificar que la configuración del DAG de transformación de IDC se lee correctamente desde config.yaml.
Validaciones:
- DATASET_ID_SILVER se importa desde modules.config
- DATASET_ID_GOLD se importa desde modules.config
- DBT_PROJECT_DIR se calcula dinámicamente desde project_root
- No hay valores hardcodeados en el código
- Las funciones de transformación utilizan los nombres de tablas correctos desde config.yaml
- El comando dbt utiliza get_dbt_command con las variables correctas
- El modelo dbt ejecutado es "idc_processed_data"
  Estado esperado: ✅ PASS

