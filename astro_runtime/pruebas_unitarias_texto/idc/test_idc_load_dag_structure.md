test_dag_structure_load_idc

Objetivo: Verificar la estructura correcta del DAG de carga de IDC.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de tareas principales: start, bronze (TaskGroup), trigger_transf_idc, end
- El TaskGroup bronze contiene 4 tareas: ensure_dataset, download_excel, load_all_sheets, cleanup_temp_files
- Dependencias correctas dentro de bronze: ensure_dataset >> download_excel >> load_all_sheets >> cleanup_temp_files
- Dependencias correctas del DAG: start >> bronze_group >> trigger_transf_dag >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:carga", "fuente:idc", "ejecución:manual"]
- dag_id correcto: "src_planeacion_load_idc"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

