test_dag_integration_load_ipm

Objetivo: Verificar la integración del DAG de carga de IPM con otros componentes del sistema.
Validaciones:
- El DAG importa correctamente las funciones desde modules.ipm.ipm_load
- El DAG importa correctamente CONF, DEFAULT_BUCKET_NAME y DATASET_ID_BRONZE desde modules.config
- La función add_project_root_to_path se ejecuta correctamente
- El módulo modules.ipm.ipm_load está disponible y accesible
- El TriggerDagRunOperator dispara el DAG correcto (src_planeacion_transf_ipm)
- El TriggerDagRunOperator tiene wait_for_completion=False
- Las dependencias entre tareas dentro del TaskGroup están correctamente definidas
- El TaskGroup bronze agrupa correctamente las tareas relacionadas
- El DAG puede ejecutarse sin errores de importación
  Estado esperado: ✅ PASS

