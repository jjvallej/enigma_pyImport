test_dag_structure_load_ipm

Objetivo: Verificar la estructura correcta del DAG de carga de IPM.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de tareas principales: start, bronze (TaskGroup), trigger_transf_ipm, end
- El TaskGroup bronze contiene 5 tareas: ensure_dataset, download_excel, transform_dataframe, load_to_bq, cleanup_temp_files
- Dependencias correctas dentro de bronze: ensure_dataset >> download_excel >> transform_dataframe >> load_to_bq >> cleanup_temp_files
- Dependencias correctas del DAG: start >> bronze_group >> trigger_transf_dag >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:carga", "fuente:ipm", "ejecución:manual"]
- dag_id correcto: "src_planeacion_load_ipm"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

