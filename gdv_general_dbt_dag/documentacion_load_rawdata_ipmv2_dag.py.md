# DOCUMENTACIÓN DETALLADA DEL DAG load_rawdata_ipmv2_dag.py

Este documento describe de forma detallada y continua el DAG de Airflow `scr_planeacion_transf_ipm_manual` definido en el archivo `load_rawdata_ipmv2_dag.py`. El DAG orquesta la ingesta de un archivo Excel del IPM desde Google Cloud Storage GCS, su transformación mínima en Python para la capa bronze, y la ejecución de modelos dbt para construir las capas silver y gold en BigQuery.

## Propósito general

Automatizar el flujo de extremo a extremo para preparar datos del IPM a partir de un Excel subido previamente a GCS. El DAG:
- Asegura la existencia de los datasets en BigQuery bronze, silver y gold.
- Localiza y descarga automáticamente el archivo Excel más reciente en una carpeta de GCS.
- Realiza transformaciones mínimas en pandas manteniendo los datos en formato texto para preservarlos en bronze.
- Construye la capa silver mediante modelos dbt encadenados que limpian, validan y estructuran los datos.
- Materializa una tabla final gold lista para consumo analítico.

## Configuración principal en el DAG

Constantes relevantes:
- `GCS_BUCKET_NAME`: bucket de origen en GCS. Por defecto `datalake_gdv`.
- `GCS_FOLDER_PATH`: carpeta con los Excel. Por defecto `data_staging/dpt_planeacion_municipal/ipm`.
- `DATASET_ID_BRONZE`: `bronze_dpt_planeacion_municipal_dev`.
- `DATASET_ID_SILVER`: `silver_dpt_planeacion_municipal_dev`.
- `DATASET_ID_GOLD`: `gold_dpt_planeacion_municipal_dev`.
- `TABLE_NAME_BRONZE`: `bronze_dpt_planeacion_municipal_dev_ipm`.
- `TABLE_NAME_SILVER`: `silver_dpt_planeacion_municipal_dev_ipm`.
- `TABLE_NAME_GOLD`: `gold_dpt_planeacion_municipal_dev_ipm`.
- `SHEET_INDEX`: índice de hoja de Excel a procesar. Por defecto 0.
- `DBT_PROJECT_DIR`: ruta del proyecto dbt dentro del contenedor de Airflow.

Variables de Airflow opcionales que sobrescriben las constantes de GCS:
- `ipm_gcs_bucket`
- `ipm_gcs_folder`

Si existen, el DAG las utilizará; si no, empleará los valores por defecto.

Variables de entorno para dbt establecidas en cada `BashOperator`:
- `DBT_PROFILES_DIR`: `/opt/airflow/include/dbt` (perfil del proyecto).
- `GOOGLE_APPLICATION_CREDENTIALS`: `/opt/airflow/include/sa.json` (cuenta de servicio).
- `PATH`: asegura que `~/.local/bin` esté al frente para encontrar `dbt` instalado por pip del usuario.

## Estructura del DAG

El DAG `scr_planeacion_transf_ipm_manual` se ejecuta manualmente (`schedule_interval=None`) y no hace catchup. Organiza las tareas en tres grupos lógicos usando `TaskGroup`.

### Grupo bronze

Objetivo: construir la tabla bronze en BigQuery preservando los datos originales como texto.

Tareas:
1. `ensure_dataset` (`PythonOperator`): llama a `ensure_dataset(dataset_id=bronze)` para crear el dataset si no existe.
2. `download_excel` (`PythonOperator`):
   - Resuelve bucket y carpeta desde Variables de Airflow o constantes.
   - Busca en GCS el archivo `.xlsx` más reciente en la carpeta indicada.
   - Descarga el archivo a un path temporal local.
3. `transform_dataframe` (`PythonOperator`):
   - Lee el Excel descargado.
   - Elimina la primera fila guía si existe.
   - Renombra columnas al esquema esperado del IPM.
   - Convierte todas las columnas a texto para preservar los valores originales.
   - Agrega `fecha_lectura` en UTC.
   - Serializa el DataFrame en un `.pkl` temporal y devuelve su ruta vía XCom.
4. `load_to_bq` (`PythonOperator`):
   - Lee el `.pkl` desde XCom, deserializa el DataFrame y lo carga en BigQuery en la tabla bronze usando `WRITE_TRUNCATE` con esquema de strings y `fecha_lectura` como `TIMESTAMP`.
5. `cleanup_temp_files` (`PythonOperator`, `TriggerRule.ALL_DONE`):
   - Limpia los archivos temporales del Excel y el `.pkl` independientemente del resultado de las tareas previas.

Dependencias dentro del grupo bronze:
`ensure_dataset -> download_excel -> transform_dataframe -> load_to_bq -> cleanup_temp_files`.

### Grupo silver

Objetivo: ejecutar en orden los modelos dbt que limpian y validan los datos hasta materializar la tabla final de silver.

Tareas:
1. `ensure_dataset` (`PythonOperator`): asegura el dataset silver.
2. `dbt_run_stg` (`BashOperator`): ejecuta `rawdata_ipmv2_stg` que lee de bronze y normaliza nombres de columnas a `snake_case`.
3. `dbt_run_normalize_text` (`BashOperator`): ejecuta `rawdata_ipmv2_normalize_text` que normaliza `cod_mpio` y `municipio` (quita acentos y usa mayúsculas).
4. `dbt_run_transform_types` (`BashOperator`): ejecuta `rawdata_ipmv2_transform_types` que intenta convertir tipos numéricos usando `SAFE_CAST` y redondea porcentajes.
5. `dbt_run_clean_numbers` (`BashOperator`): ejecuta `rawdata_ipmv2_clean_numbers` que limpia números eliminando letras y símbolos y convierte a enteros.
6. `dbt_run_detect_negatives` (`BashOperator`): ejecuta `rawdata_ipmv2_detect_negatives` que crea `flags` sobre negativos y totales vacíos o cero.
7. `dbt_run_apply_validations` (`BashOperator`): ejecuta `rawdata_ipmv2_apply_validations` que aplica reglas de negocio: poner en cero si hay negativos o total inválido; convierte porcentajes a `FLOAT64`.
8. `dbt_run_clean` (`BashOperator`): ejecuta `rawdata_ipmv2_clean` que materializa la tabla final de silver y convierte `fecha_lectura` a `DATE`.
9. `dbt_test` (`BashOperator`): ejecuta pruebas sobre `rawdata_ipmv2_clean`.

Dependencias clave:
- Secuencia inicial: `ensure_dataset -> dbt_run_stg -> dbt_run_normalize_text`.
- Paralelismo controlado: desde `normalize_text`, `transform_types` y `clean_numbers` pueden ejecutarse en paralelo; sin embargo, la cadena efectiva continúa desde `clean_numbers` hacia `detect_negatives -> apply_validations -> clean -> test`.

Todas las tareas dbt exportan el entorno necesario y ejecutan `~/.local/bin/dbt` con fallback a `dbt` del `PATH`.

### Grupo gold

Objetivo: crear la tabla de consumo final.

Tareas:
1. `ensure_dataset` (`PythonOperator`): asegura el dataset gold.
2. `dbt_run_gold` (`BashOperator`): ejecuta `rawdata_ipmv2_gold`, que toma la tabla final de silver y la simplifica al esquema de consumo, sin porcentajes ni `fecha_lectura`.
3. `dbt_test` (`BashOperator`): ejecuta pruebas sobre el modelo gold.

Dependencias gold: `ensure_dataset -> dbt_run_gold -> dbt_test`.

### Dependencias entre grupos

`start -> bronze -> silver -> gold`.

## Intercambio de datos entre tareas XCom

- `transform_dataframe` devuelve la ruta del `.pkl` con el DataFrame transformado.
- `load_to_bq` lee esa ruta desde XCom.
- `cleanup_temp_files` lee desde XCom tanto la ruta del Excel descargado como la ruta del `.pkl` para eliminarlos.

## Manejo de errores y limpieza

- La tarea de limpieza en bronze usa `TriggerRule.ALL_DONE` para garantizar la eliminación de temporales incluso si fallan tareas previas.
- En silver y gold, los errores de dbt hacen fallar la tarea correspondiente; el operador Bash está configurado para devolver código distinto de cero si `dbt` falla, lo que marca la tarea como `FAILED`.

## Consideraciones de idempotencia

- Bronze carga con `WRITE_TRUNCATE`: cada ejecución reemplaza completamente la tabla de bronze.
- En silver, las vistas intermedias se recalculan; la tabla final `rawdata_ipmv2_clean` se materializa explícitamente como tabla.
- Gold materializa tabla nueva a partir de la salida de silver.

## Ejecución del DAG y parámetros

- El DAG no tiene `schedule` y se dispara manualmente desde la UI de Airflow o vía API.
- Para cambiar el origen GCS sin editar código, definir Variables de Airflow:
  - `ipm_gcs_bucket`
  - `ipm_gcs_folder`
- Para cambiar la hoja del Excel, ajustar `SHEET_INDEX` en el código.

## Requisitos previos

- Cuenta de servicio válida en `/opt/airflow/include/sa.json` con permisos de lectura en GCS y lectura/escritura en BigQuery.
- Perfil dbt válido en `/opt/airflow/include/dbt` apuntando al proyecto y datasets correctos.
- El proyecto dbt en `DBT_PROJECT_DIR` con los modelos `rawdata_ipmv2_*` presentes y configurados.

## Resumen del flujo

1. Bronze: descarga el Excel más reciente, estandariza columnas en pandas, persiste todo como texto y carga a BigQuery.
2. Silver: normaliza texto, limpia números, detecta inconsistencias, aplica reglas de negocio y materializa la tabla final con `fecha_lectura` como `DATE`.
3. Gold: genera una tabla de consumo con nombres estandarizados y solo valores absolutos.
