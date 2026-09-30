test_dag_task_functionality_transform_idc

Objetivo: Verificar que las tareas del DAG de transformación de IDC ejecutan correctamente sus funciones.
Validaciones:
- La tarea ensure_dataset en silver ejecuta _ensure_dataset_silver_task
- Las tareas normalize_columns ejecutan normalize_columns_step para cada tabla (dato_original, valor_normalizado, valor_ranking)
- Las tareas uppercase_departamento ejecutan uppercase_departamento_step para cada tabla
- Las tareas fill_nulls ejecutan fill_nulls_step para cada tabla
- Las tareas transform ejecutan transform_table_complete para cada tabla creando las tablas finales en silver
- La tarea ensure_dataset en gold ejecuta _ensure_dataset_gold_task
- La tarea dbt_fact_idc ejecuta el comando dbt run --select idc_processed_data
- El modelo dbt une las 3 tablas de silver con el diccionario dim_idc en gold
- Todas las tareas tienen execution_timeout configurado (30 minutos)
  Estado esperado: ✅ PASS

