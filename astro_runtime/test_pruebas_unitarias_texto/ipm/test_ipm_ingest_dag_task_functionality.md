test_dag_task_functionality_ingest_ipm

Objetivo: Verificar que las tareas del DAG de ingesta de IPM ejecutan correctamente sus funciones.
Validaciones:
- La tarea upload_file_from_drive_to_gcs ejecuta _upload_file_task
- _upload_file_task llama a move_file_from_drive_to_gcs con los parámetros correctos
- La función imprime información de depuración apropiada
- La función retorna el URI de GCS donde se almacenó el archivo
- La tarea trigger_load_ipm es un TriggerDagRunOperator
- trigger_load_ipm tiene trigger_dag_id="src_planeacion_load_ipm"
- trigger_load_ipm tiene wait_for_completion=False
- Las tareas start y end son EmptyOperator
  Estado esperado: ✅ PASS

