test_dag_task_functionality_transform_ipm

Objetivo: Verificar que las tareas del DAG de transformación de IPM ejecutan correctamente sus funciones.
Validaciones:
- Las tareas ensure_dataset en silver y gold ejecutan las funciones wrapper correctas
- _ensure_dataset_silver_task llama a ensure_dataset con DATASET_ID_SILVER
- _ensure_dataset_gold_task llama a ensure_dataset con DATASET_ID_GOLD
- Todas las tareas dbt en silver son BashOperator que ejecutan comandos dbt
- Los comandos dbt se generan correctamente usando get_dbt_command
- Los comandos dbt incluyen --select con el modelo correcto
- La tarea dbt_test en silver ejecuta dbt test sobre ipm_transform_clean
- La tarea dbt_run_gold ejecuta dbt run sobre ipm_processed_data
- La tarea dbt_test en gold ejecuta dbt test sobre ipm_processed_data
- Las tareas dbt tienen append_env=True para pasar variables de entorno
- Las dependencias entre tareas dbt están correctamente definidas
  Estado esperado: ✅ PASS

