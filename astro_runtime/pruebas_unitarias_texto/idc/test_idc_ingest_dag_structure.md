test_dag_structure_ingest_idc

Objetivo: Verificar la estructura correcta del DAG de ingesta de IDC.
Validaciones: 
- El DAG se carga sin errores de sintaxis
- Número correcto de tareas (4: start, upload_file_from_drive_to_gcs, trigger_load_idc, end)
- Dependencias correctas: start >> upload_file_from_drive_to_gcs >> trigger_load_idc >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:ingesta", "fuente:idc", "ejecución:manual"]
- dag_id correcto: "src_planeacion_ingest_idc"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

