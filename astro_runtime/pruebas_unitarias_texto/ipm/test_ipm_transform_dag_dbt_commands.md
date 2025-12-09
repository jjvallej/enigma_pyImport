test_dag_dbt_commands_transform_ipm

Objetivo: Verificar que los comandos dbt generados en el DAG de transformación de IPM son correctos.
Validaciones:
- El comando para dbt_run_stg es: "dbt run --select ipm_transform_stg"
- El comando para dbt_run_normalize_text es: "dbt run --select ipm_transform_normalize_text"
- El comando para dbt_run_transform_types es: "dbt run --select ipm_transform_transform_types"
- El comando para dbt_run_clean_numbers es: "dbt run --select ipm_transform_clean_numbers"
- El comando para dbt_run_detect_negatives es: "dbt run --select ipm_transform_detect_negatives"
- El comando para dbt_run_apply_validations es: "dbt run --select ipm_transform_apply_validations"
- El comando para dbt_run_clean es: "dbt run --select ipm_transform_clean"
- El comando para dbt_test en silver es: "dbt test --select ipm_transform_clean"
- El comando para dbt_run_gold es: "dbt run --select ipm_processed_data"
- El comando para dbt_test en gold es: "dbt test --select ipm_processed_data"
- Todos los comandos se generan usando get_dbt_command con DBT_PROJECT_DIR
- Los comandos incluyen las variables de entorno necesarias
  Estado esperado: ✅ PASS

