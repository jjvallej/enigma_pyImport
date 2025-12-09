test_dag_task_functionality_load_idc

Objetivo: Verificar que las tareas del DAG de carga de IDC ejecutan correctamente sus funciones.
Validaciones:
- La tarea ensure_dataset ejecuta _ensure_dataset_bronze_task que llama a ensure_dataset del módulo
- La tarea download_excel ejecuta _download_excel_task que busca y descarga el archivo Excel más reciente
- _download_excel_task utiliza get_latest_excel_from_gcs_folder y download_excel_from_gcs
- La tarea load_all_sheets ejecuta _load_all_sheets_task que carga todas las hojas del Excel a BigQuery
- _load_all_sheets_task utiliza load_all_sheets_to_bq con el mapeo correcto de hojas a tablas
- Se cargan correctamente las 3 tablas: idc_raw_data_dato_original, idc_raw_data_valor_normalizado, idc_raw_data_valor_ranking
- La tarea cleanup_temp_files ejecuta _cleanup_temp_files_task con TriggerRule.ALL_DONE
- _cleanup_temp_files_task utiliza cleanup_temp_paths para eliminar archivos temporales
- Las tareas utilizan XCom correctamente para pasar datos entre ellas
- La tarea trigger_transf_idc dispara el DAG "src_planeacion_transf_idc" sin esperar su finalización
  Estado esperado: ✅ PASS

