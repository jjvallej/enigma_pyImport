test_dag_structure_transform_idc

Objetivo: Verificar la estructura correcta del DAG de transformación de IDC.
Validaciones:
- El DAG se carga sin errores de sintaxis
- Número correcto de grupos principales: start, silver (TaskGroup), gold (TaskGroup), end
- El TaskGroup silver contiene 13 tareas: ensure_dataset, normalize_columns_dato_original, normalize_columns_valor_normalizado, normalize_columns_valor_ranking, uppercase_departamento_dato_original, uppercase_departamento_valor_normalizado, uppercase_departamento_valor_ranking, fill_nulls_dato_original, fill_nulls_valor_normalizado, fill_nulls_valor_ranking, transform_dato_original, transform_valor_normalizado, transform_valor_ranking
- El TaskGroup gold contiene 2 tareas: ensure_dataset, dbt_fact_idc
- Dependencias correctas en silver: ensure_dataset >> [normalize en paralelo] >> [uppercase en paralelo] >> [fill_nulls en paralelo] >> [transform final en paralelo]
- Dependencias correctas en gold: ensure_dataset >> dbt_fact_idc
- Dependencias correctas del DAG: start >> silver_group, [transform finales] >> gold_group >> end
- Schedule interval configurado como None (ejecución manual)
- Tags apropiados presentes: ["secretaria:planeacion", "actividad:transformacion", "fuente:idc", "ejecución:manual"]
- dag_id correcto: "src_planeacion_transf_idc"
- start_date configurado: datetime(2024, 1, 1)
- catchup configurado como False
  Estado esperado: ✅ PASS

