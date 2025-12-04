test_dag_task_functionality_transform_ipm_sisben

Objetivo: Verificar que las tareas del DAG de transformación de IPM SISBEN ejecutan correctamente sus funciones.
Validaciones:
- Las tareas ensure_dataset en silver y gold ejecutan las funciones wrapper correctas
- _ensure_dataset_silver_task llama a ensure_dataset con DATASET_ID_SILVER
- _ensure_dataset_gold_task llama a ensure_dataset con DATASET_ID_GOLD
- La tarea dbt_ipm_sisben_stg es un BashOperator que ejecuta dbt run --select ipm_sisben_stg
- La tarea dbt_ipm_sisben_processed_data es un BashOperator que ejecuta dbt run --select ipm_sisben_processed_data
- Los comandos dbt se generan correctamente usando get_dbt_command
- Las tareas dbt tienen append_env=True para pasar variables de entorno
- Las dependencias entre tareas están correctamente definidas
- El DAG no incluye tareas de prueba dbt (a diferencia del DAG de IPM regular)
  Estado esperado: ✅ PASS

