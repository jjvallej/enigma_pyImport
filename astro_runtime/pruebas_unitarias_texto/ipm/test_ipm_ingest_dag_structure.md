test_dag_structure_ingest_ipm

Objetivo: Verificar la estructura correcta del DAG de ingesta de IPM.
Validaciones: 
- El DAG se carga sin errores de sintaxis
- Número correcto de tareas (4: start, upload_file_from_drive_to_gcs, trigger_load_ipm, end)
- Dependencias correctas: start >> upload_file_from_drive_to_gcs >> trigger_load_ipm >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:ingesta", "fuente:ipm", "ejecución:manual"]
- dag_id correcto: "src_planeacion_ingest_ipm"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

