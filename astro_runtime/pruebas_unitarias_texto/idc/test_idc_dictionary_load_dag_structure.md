test_dag_structure_load_idc_dictionary

Objetivo: Verificar la estructura correcta del DAG de carga del diccionario IDC.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de tareas principales: start, gold (TaskGroup), end, trigger_ingest_idc
- El TaskGroup gold contiene 4 tareas: ensure_dataset, download_csv, load_csv, cleanup_temp_files
- Dependencias correctas dentro de gold: ensure_dataset >> download_csv >> load_csv >> cleanup_temp_files
- Dependencias correctas del DAG: start >> gold_group >> end >> trigger_ingest_idc
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:carga", "fuente:idc_dictionary", "ejecución:manual"]
- dag_id correcto: "src_planeacion_load_idc_dictionary"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

