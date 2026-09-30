test_dag_dbt_commands_transform_idc

Objetivo: Verificar que los comandos dbt del DAG de transformación de IDC se ejecutan correctamente.
Validaciones:
- El comando dbt utiliza get_dbt_command con el modelo "idc_processed_data"
- El comando incluye las variables de entorno correctas: DBT_PROJECT_ID, DBT_DATASET_BRONZE, DBT_DATASET_SILVER, DBT_DATASET_GOLD
- El comando incluye --vars con las variables dbt correctas
- El comando se ejecuta en el directorio correcto (DBT_PROJECT_DIR)
- El modelo dbt idc_processed_data se ejecuta correctamente
- El modelo crea la tabla FACT_IDC en gold con la estructura correcta: DEPARTAMENTO, ANIO, ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, VALOR_ORIGINAL, VALOR_NORMALIZADO, VALOR_RANKING
- El modelo une correctamente las 3 tablas de silver con el diccionario DIM_IDC
  Estado esperado: ✅ PASS

