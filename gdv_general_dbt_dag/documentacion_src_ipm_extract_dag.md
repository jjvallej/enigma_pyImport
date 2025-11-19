# DOCUMENTACIÓN DETALLADA DEL DAG src_ipm_extract_dag.py

Este documento describe de forma detallada y continua el DAG de Airflow src_planeacion_extrac_ipm definido en el archivo src_ipm_extract_dag.py. El DAG orquesta la extracción de un archivo Excel del IPM desde Google Cloud Storage GCS, su transformación mínima en Python para la capa bronze, y la carga a BigQuery.

## Propósito general

Automatizar el flujo de extracción de datos del IPM a partir de un Excel subido previamente a GCS. El DAG:
- Asegura la existencia del dataset bronze en BigQuery.
- Localiza y descarga automáticamente el archivo Excel más reciente en una carpeta de GCS.
- Realiza transformaciones mínimas en pandas manteniendo los datos en formato texto para preservarlos en bronze.
- Carga los datos transformados a la tabla `ipm_raw_data` en el dataset bronze.

## Configuración principal en el DAG

Constantes relevantes:
- `GCS_BUCKET_NAME`: bucket de origen en GCS. Por defecto `datalake_gdv`.
- `GCS_FOLDER_PATH`: carpeta con los Excel. Por defecto `data_staging/dpt_planeacion_municipal/ipm`.
- `DATASET_ID_BRONZE`: `bronze_dpt_planeacion_municipal_dev`.
- `TABLE_NAME_BRONZE`: `ipm_raw_data`.
- `SHEET_INDEX`: índice de hoja de Excel a procesar. Por defecto 0.

Variables de Airflow opcionales que sobrescriben las constantes de GCS:
- `ipm_gcs_bucket`
- `ipm_gcs_folder`

Si existen, el DAG las utilizará; si no, empleará los valores por defecto.

## Estructura del DAG

El DAG src_planeacion_extrac_ipm se ejecuta manualmente (schedule_interval=None) y no hace catchup. Organiza las tareas en un grupo lógico usando TaskGroup.

### Grupo extract (extracción a bronze)

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

Dependencias dentro del grupo extract:
`ensure_dataset -> download_excel -> transform_dataframe -> load_to_bq -> cleanup_temp_files`.

### Tarea de disparo al DAG de transformación

Después de completar la extracción exitosamente:
- `trigger_transf_ipm` (`TriggerDagRunOperator`): ejecuta el DAG `src_planeacion_transf_ipm` que transforma los datos desde bronze a silver y gold. Espera a que termine completamente antes de finalizar.

### Dependencias entre grupos

`start -> bronze -> trigger_transf_ipm -> end`.

## Intercambio de datos entre tareas XCom

- `download_excel` devuelve la ruta del archivo Excel descargado.
- `transform_dataframe` devuelve la ruta del `.pkl` con el DataFrame transformado.
- `load_to_bq` lee esa ruta desde XCom.
- `cleanup_temp_files` lee desde XCom tanto la ruta del Excel descargado como la ruta del `.pkl` para eliminarlos.

## Manejo de errores y limpieza

- La tarea de limpieza usa `TriggerRule.ALL_DONE` para garantizar la eliminación de temporales incluso si fallan tareas previas.
- Si el DAG de transformación falla, la tarea `trigger_transf_ipm` también fallará, asegurando que todo el pipeline falle de manera controlada.

## Consideraciones de idempotencia

- Bronze carga con `WRITE_TRUNCATE`: cada ejecución reemplaza completamente la tabla `ipm_raw_data`.

## Ejecución del DAG y parámetros

- El DAG no tiene `schedule` y se dispara manualmente desde la UI de Airflow o vía API, o es llamado automáticamente por el DAG `src_planeacion_inges_ipm`.
- Para cambiar el origen GCS sin editar código, definir Variables de Airflow:
  - `ipm_gcs_bucket`
  - `ipm_gcs_folder`
- Para cambiar la hoja del Excel, ajustar `SHEET_INDEX` en el código.

## Requisitos previos

- Cuenta de servicio válida en `/opt/airflow/include/sa.json` con permisos de lectura en GCS y lectura/escritura en BigQuery.
- Datos ya disponibles en GCS en la carpeta `data_staging/dpt_planeacion_municipal/ipm` (normalmente subidos por el DAG `src_planeacion_inges_ipm`).

## Resumen del flujo

1. Extracción: descarga el Excel más reciente desde GCS, estandariza columnas en pandas, persiste todo como texto y carga a BigQuery en la tabla `ipm_raw_data`.
2. Disparo: ejecuta automáticamente el DAG de transformación que procesa los datos desde bronze hasta silver y gold.

