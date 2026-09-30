test_dag_task_functionality_load_idc_dictionary

Objetivo: Verificar que las tareas del DAG de carga del diccionario IDC ejecutan correctamente sus funciones.
Validaciones:
- La tarea ensure_dataset ejecuta _ensure_dataset_gold_task que llama a ensure_dataset del módulo
- La tarea download_csv ejecuta _download_csv_task que busca y descarga el archivo CSV más reciente
- _download_csv_task utiliza get_latest_csv_from_gcs_folder y download_csv_from_gcs
- La tarea load_csv ejecuta _load_csv_task que carga el CSV a BigQuery
- _load_csv_task utiliza load_csv_to_bq con el dataset gold y tabla dim_idc correctos
- La tarea cleanup_temp_files ejecuta _cleanup_temp_files_task con TriggerRule.ALL_DONE
- _cleanup_temp_files_task utiliza cleanup_temp_paths para eliminar archivos temporales
- Las tareas utilizan XCom correctamente para pasar datos entre ellas
- La tarea trigger_ingest_idc dispara el DAG "src_planeacion_ingest_idc" sin esperar su finalización
  Estado esperado: ✅ PASS

