test_dag_task_functionality_load_ipm_sisben

Objetivo: Verificar que las tareas del DAG de carga de IPM SISBEN ejecutan correctamente sus funciones.
Validaciones:
- La tarea ensure_dataset es un BigQueryCreateEmptyDatasetOperator
- ensure_dataset tiene dataset_id=DATASET_ID_BRONZE
- ensure_dataset tiene project_id=PROJECT_ID
- ensure_dataset tiene location=LOCATION
- ensure_dataset tiene exists_ok=True
- La tarea create_external_table es un BigQueryInsertJobOperator
- create_external_table ejecuta el SQL generado por get_create_external_table_sql
- create_external_table tiene useLegacySql=False
- create_external_table tiene location=LOCATION y project_id=PROJECT_ID
- La tarea trigger_transform_dag es un TriggerDagRunOperator
- trigger_transform_dag tiene trigger_dag_id="src_planeacion_transform_ipm_sisben"
- trigger_transform_dag tiene wait_for_completion=False
- trigger_transform_dag tiene reset_dag_run=True
  Estado esperado: ✅ PASS

