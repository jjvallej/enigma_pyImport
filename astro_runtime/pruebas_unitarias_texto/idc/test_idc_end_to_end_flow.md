test_dag_end_to_end_flow_idc

Objetivo: Verificar el flujo completo end-to-end del proceso IDC desde ingesta hasta gold.
Validaciones:
- El DAG de ingesta del diccionario transfiere el CSV desde Google Drive a GCS
- El DAG de carga del diccionario carga el CSV a BigQuery como tabla DIM_IDC en gold
- El DAG de ingesta de IDC transfiere el Excel desde Google Drive a GCS
- El DAG de carga de IDC carga las 3 hojas del Excel a BigQuery como tablas bronze
- El DAG de transformación de IDC procesa las 3 tablas bronze a silver con todas las transformaciones
- Las transformaciones en silver incluyen: normalización de columnas, uppercase de departamento, reemplazo de NULLs, redondeo/conversión numérica
- El DAG de transformación crea la tabla FACT_IDC en gold uniendo las 3 tablas silver con DIM_IDC
- La tabla FACT_IDC contiene la estructura final correcta con todos los campos requeridos
- Todos los DAGs se ejecutan en el orden correcto mediante triggers
  Estado esperado: ✅ PASS

