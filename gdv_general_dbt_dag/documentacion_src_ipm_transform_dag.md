# DOCUMENTACIÓN DETALLADA DEL DAG src_ipm_transform_dag.py

Este documento describe de forma detallada y continua el DAG de Airflow src_planeacion_transf_ipm definido en el archivo src_ipm_transform_dag.py. El DAG orquesta la transformacion de los datos del IPM desde la capa bronze a las capas silver y gold utilizando modelos dbt.

## Propósito general

Automatizar el flujo de transformacion de los datos del IPM desde bronze hasta silver y gold. El DAG:
- Asegura la existencia de los datasets silver y gold en BigQuery.
- Construye la capa silver mediante modelos dbt encadenados que limpian, validan y estructuran los datos desde bronze.
- Materializa una tabla final gold lista para consumo analítico.

**Nota importante:** Este DAG espera que los datos ya estén disponibles en la tabla `bronze_dpt_planeacion_municipal_dev.ipm_raw_data`, que es poblada por el DAG `src_planeacion_extrac_ipm`.

## Configuración principal en el DAG

Constantes relevantes:
- `DATASET_ID_SILVER`: `silver_dpt_planeacion_municipal_dev`.
- `DATASET_ID_GOLD`: `gold_dpt_planeacion_municipal_dev`.
- `TABLE_NAME_SILVER`: `ipm_transformed_data`.
- `TABLE_NAME_GOLD`: `ipm_processed_data`.
- `DBT_PROJECT_DIR`: ruta del proyecto dbt dentro del contenedor de Airflow.

Variables de entorno para dbt establecidas en cada `BashOperator`:
- `DBT_PROFILES_DIR`: `/opt/airflow/include/dbt` (perfil del proyecto).
- `GOOGLE_APPLICATION_CREDENTIALS`: `/opt/airflow/include/sa.json` (cuenta de servicio).
- `PATH`: asegura que `~/.local/bin` esté al frente para encontrar `dbt` instalado por pip del usuario.

## Estructura del DAG

El DAG src_planeacion_transf_ipm se ejecuta manualmente (schedule_interval=None) y no hace catchup. Se ejecuta automaticamente al finalizar el DAG `src_planeacion_extrac_ipm`, o puede ejecutarse manualmente si los datos ya estan en bronze. Organiza las tareas en dos grupos logicos usando TaskGroup.

### Grupo silver

Objetivo: ejecutar en orden los modelos dbt que limpian y validan los datos hasta materializar la tabla final de silver.

Tareas:
1. `ensure_dataset` (`PythonOperator`): asegura el dataset silver.
2. `dbt_run_stg` (`BashOperator`): ejecuta `ipm_transform_stg` que lee de bronze y normaliza nombres de columnas a `snake_case`.
3. `dbt_run_normalize_text` (`BashOperator`): ejecuta `ipm_transform_normalize_text` que normaliza `cod_mpio` y `municipio` (quita acentos y usa mayúsculas).
4. `dbt_run_transform_types` (`BashOperator`): ejecuta `ipm_transform_transform_types` que intenta convertir tipos numéricos usando `SAFE_CAST` y redondea porcentajes.
5. `dbt_run_clean_numbers` (`BashOperator`): ejecuta `ipm_transform_clean_numbers` que limpia números eliminando letras y símbolos y convierte a enteros.
6. `dbt_run_detect_negatives` (`BashOperator`): ejecuta `ipm_transform_detect_negatives` que crea `flags` sobre negativos y totales vacíos o cero.
7. `dbt_run_apply_validations` (`BashOperator`): ejecuta `ipm_transform_apply_validations` que aplica reglas de negocio: poner en cero si hay negativos o total inválido; convierte porcentajes a `FLOAT64`.
8. `dbt_run_clean` (`BashOperator`): ejecuta `ipm_transform_clean` que materializa la tabla final de silver y convierte `fecha_lectura` a `DATE`.
9. `dbt_test` (`BashOperator`): ejecuta pruebas sobre `ipm_transform_clean`.

Dependencias clave:
- Secuencia inicial: `ensure_dataset -> dbt_run_stg -> dbt_run_normalize_text`.
- Paralelismo controlado: desde `normalize_text`, `transform_types` y `clean_numbers` pueden ejecutarse en paralelo; sin embargo, la cadena efectiva continúa desde `clean_numbers` hacia `detect_negatives -> apply_validations -> clean -> test`.

Todas las tareas dbt exportan el entorno necesario y ejecutan `~/.local/bin/dbt` con fallback a `dbt` del `PATH`.

### Grupo gold

Objetivo: crear la tabla de consumo final.

Tareas:
1. `ensure_dataset` (`PythonOperator`): asegura el dataset gold.
2. `dbt_run_gold` (`BashOperator`): ejecuta `ipm_processed_data`, que toma la tabla final de silver y la simplifica al esquema de consumo, sin porcentajes ni `fecha_lectura`.
3. `dbt_test` (`BashOperator`): ejecuta pruebas sobre el modelo gold.

Dependencias gold: `ensure_dataset -> dbt_run_gold -> dbt_test`.

### Dependencias entre grupos

`start -> bronze -> silver -> gold`.

## Manejo de errores

- En silver y gold, los errores de dbt hacen fallar la tarea correspondiente; el operador Bash está configurado para devolver código distinto de cero si `dbt` falla, lo que marca la tarea como `FAILED`.
- El DAG espera que los datos ya estén en bronze. Si la tabla `ipm_raw_data` no existe o está vacía, las tareas dbt fallarán.

## Consideraciones de idempotencia

- En silver, las vistas intermedias se recalculan; la tabla final `ipm_transformed_data` se materializa explícitamente como tabla.
- Gold materializa tabla nueva a partir de la salida de silver.

## Ejecución del DAG y parámetros

- El DAG no tiene `schedule` y se dispara automáticamente al finalizar el DAG `src_planeacion_extrac_ipm`, o manualmente desde la UI de Airflow o vía API si los datos ya están en bronze.
- El DAG lee directamente desde la tabla bronze `bronze_dpt_planeacion_municipal_dev.ipm_raw_data`, por lo que no requiere configuración adicional.

## Requisitos previos

- Datos ya disponibles en la tabla `bronze_dpt_planeacion_municipal_dev.ipm_raw_data` (normalmente cargados por el DAG `src_planeacion_extrac_ipm`).
- Cuenta de servicio válida en `/opt/airflow/include/sa.json` con permisos de lectura/escritura en BigQuery.
- Perfil dbt válido en `/opt/airflow/include/dbt` apuntando al proyecto y datasets correctos.
- El proyecto dbt en `DBT_PROJECT_DIR` con los modelos `ipm_transform_*` y `ipm_processed_data` presentes y configurados.

## Resumen del flujo

1. Silver: lee desde bronze, normaliza texto, limpia números, detecta inconsistencias, aplica reglas de negocio y materializa la tabla final `ipm_transformed_data` con `fecha_lectura` como `DATE`.
2. Gold: genera la tabla `ipm_processed_data` de consumo con nombres estandarizados y solo valores absolutos.

**Flujo completo del pipeline:**
- `src_planeacion_inges_ipm` (src_ipm_load_dag.py) → carga archivo desde Drive a GCS
- `src_planeacion_extrac_ipm` (src_ipm_extract_dag.py) → extrae desde GCS a bronze
- `src_planeacion_transf_ipm` (src_ipm_transform_dag.py) → transforma desde bronze a silver/gold

