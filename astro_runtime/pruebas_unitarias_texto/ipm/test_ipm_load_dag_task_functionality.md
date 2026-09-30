test_dag_task_functionality_load_ipm

Objetivo: Verificar que las tareas del DAG de carga de IPM ejecutan correctamente sus funciones.
Validaciones:
- La tarea ensure_dataset ejecuta _ensure_dataset_bronze_task que llama a ensure_dataset del módulo
- La tarea download_excel ejecuta _download_excel_task que busca y descarga el archivo más reciente
- _download_excel_task utiliza get_latest_excel_from_gcs_folder y download_excel_from_gcs
- La tarea transform_dataframe ejecuta _transform_task que transforma el Excel a DataFrame
- _transform_task utiliza transform_excel y serializa el DataFrame en pickle
- La tarea load_to_bq ejecuta _load_task que carga el DataFrame a BigQuery
- _load_task utiliza load_dataframe_to_bq con el dataset y tabla correctos
- La tarea cleanup_temp_files ejecuta _cleanup_temp_files_task con TriggerRule.ALL_DONE
- _cleanup_temp_files_task utiliza cleanup_temp_paths para eliminar archivos temporales
- Las tareas utilizan XCom correctamente para pasar datos entre ellas
  Estado esperado: ✅ PASS

