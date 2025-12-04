test_dag_structure_ingest_ipm_sisben

Objetivo: Verificar la estructura correcta del DAG de ingesta de IPM SISBEN.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de tareas (3: start, move_file_within_gcs, end)
- Dependencias correctas: start >> move_file_within_gcs >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:ingesta", "fuente:ipm_sisben", "ejecución:manual"]
- dag_id correcto: "src_planeacion_ingest_ipm_sisben"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
- No hay TriggerDagRunOperator en este DAG (a diferencia del DAG de IPM regular)
  Estado esperado: ✅ PASS

