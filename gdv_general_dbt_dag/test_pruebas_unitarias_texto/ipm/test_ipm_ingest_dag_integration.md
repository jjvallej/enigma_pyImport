test_dag_integration_ingest_ipm

Objetivo: Verificar la integración del DAG de ingesta de IPM con otros componentes del sistema.
Validaciones:
- El DAG importa correctamente move_file_from_drive_to_gcs desde modules.ipm.ipm_ingest
- El DAG importa correctamente CONF y DEFAULT_BUCKET_NAME desde modules.config
- La función add_project_root_to_path se ejecuta correctamente
- El módulo modules.ipm.ipm_ingest está disponible y accesible
- El TriggerDagRunOperator dispara el DAG correcto (src_planeacion_load_ipm)
- El DAG puede ejecutarse sin errores de importación
- Las dependencias entre tareas están correctamente definidas
  Estado esperado: ✅ PASS

