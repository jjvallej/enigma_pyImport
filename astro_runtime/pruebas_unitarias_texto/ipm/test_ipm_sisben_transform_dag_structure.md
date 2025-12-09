test_dag_structure_transform_ipm_sisben

Objetivo: Verificar la estructura correcta del DAG de transformación de IPM SISBEN.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de grupos principales: start, silver (TaskGroup), gold (TaskGroup), end
- El TaskGroup silver contiene 2 tareas: ensure_dataset, dbt_ipm_sisben_stg
- El TaskGroup gold contiene 2 tareas: ensure_dataset, dbt_ipm_sisben_processed_data
- Dependencias correctas en silver: ensure_dataset >> dbt_ipm_sisben_stg
- Dependencias correctas en gold: ensure_dataset >> dbt_ipm_sisben_processed_data
- Dependencias correctas del DAG: start >> silver_group >> gold_group >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:transformacion", "fuente:ipm_sisben", "ejecución:manual"]
- dag_id correcto: "src_planeacion_transform_ipm_sisben"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

