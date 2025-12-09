test_dag_task_functionality_ingest_ipm_sisben

Objetivo: Verificar que las tareas del DAG de ingesta de IPM SISBEN ejecutan correctamente sus funciones.
Validaciones:
- La tarea move_file_within_gcs ejecuta _move_file_task
- _move_file_task llama a move_file_within_gcs con los parámetros correctos
- La función imprime información de depuración apropiada incluyendo bucket, carpeta origen y destino
- La función retorna el URI de GCS donde se movió el archivo
- move_file_within_gcs se importa desde modules.ipm.ipm_sisben_ingest
- El file_pattern "*.xlsx" se pasa correctamente a move_file_within_gcs
- Las tareas start y end son EmptyOperator
- No hay tareas adicionales más allá de las necesarias
  Estado esperado: ✅ PASS

