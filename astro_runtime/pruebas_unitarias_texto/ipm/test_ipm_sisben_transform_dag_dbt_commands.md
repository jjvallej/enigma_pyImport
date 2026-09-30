test_dag_dbt_commands_transform_ipm_sisben

Objetivo: Verificar que los comandos dbt generados en el DAG de transformación de IPM SISBEN son correctos.
Validaciones:
- El comando para dbt_ipm_sisben_stg es: "dbt run --select ipm_sisben_stg"
- El comando para dbt_ipm_sisben_processed_data es: "dbt run --select ipm_sisben_processed_data"
- Todos los comandos se generan usando get_dbt_command con DBT_PROJECT_DIR
- Los comandos incluyen las variables de entorno necesarias
- Los comandos se pasan correctamente al BashOperator
- No hay comandos de prueba dbt en este DAG
- Los modelos dbt referenciados existen en el proyecto dbt
  Estado esperado: ✅ PASS

