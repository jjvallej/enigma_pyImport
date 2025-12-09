test_dag_structure_transform_ipm

Objetivo: Verificar la estructura correcta del DAG de transformación de IPM.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de grupos principales: start, silver (TaskGroup), gold (TaskGroup), end
- El TaskGroup silver contiene 9 tareas: ensure_dataset, dbt_run_stg, dbt_run_normalize_text, dbt_run_transform_types, dbt_run_clean_numbers, dbt_run_detect_negatives, dbt_run_apply_validations, dbt_run_clean, dbt_test
- El TaskGroup gold contiene 3 tareas: ensure_dataset, dbt_run_gold, dbt_test
- Dependencias correctas en silver: ensure_dataset >> stg >> normalize_text >> [transform_types, clean_numbers] >> detect_negatives >> apply_validations >> clean >> test
- Dependencias correctas en gold: ensure_dataset >> dbt_run_gold >> dbt_test
- Dependencias correctas del DAG: start >> silver_group >> gold_group
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:transformacion", "fuente:ipm", "ejecución:manual"]
- dag_id correcto: "src_planeacion_transf_ipm"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

