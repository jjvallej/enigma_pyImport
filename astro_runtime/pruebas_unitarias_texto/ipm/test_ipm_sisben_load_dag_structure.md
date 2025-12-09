test_dag_structure_load_ipm_sisben

Objetivo: Verificar la estructura correcta del DAG de carga de IPM SISBEN.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de tareas (5: start, ensure_dataset, create_external_table, trigger_transform_dag, end)
- Dependencias correctas: start >> ensure_dataset >> create_external_table >> trigger_transform_dag >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:carga", "fuente:ipm_sisben", "ejecución:manual"]
- dag_id correcto: "src_planeacion_load_ipm_sisben"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
- ensure_dataset es un BigQueryCreateEmptyDatasetOperator
- create_external_table es un BigQueryInsertJobOperator
- trigger_transform_dag es un TriggerDagRunOperator
  Estado esperado: ✅ PASS

