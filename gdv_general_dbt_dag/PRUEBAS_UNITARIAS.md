# PRUEBAS UNITARIAS Y DE INTEGRACIÓN DE LA PLANTILLA IPM V2

Este documento resume las pruebas unitarias y de integración realizadas y sugeridas para la plantilla. Cubre módulos Python de ingesta y carga, DAGs de Airflow y modelos dbt de la capa silver y gold.

## Alcance

- Validación funcional de utilidades Python para Google Drive y GCS
- Validación de ingestión a bronze en BigQuery
- Validación de orquestación con Airflow (bronze, silver y gold)
- Validación de transformaciones SQL en dbt

## Precondiciones

- Cuenta de servicio con permisos de lectura en GCS y lectura/escritura en BigQuery
- Proyecto dbt configurado en `DBT_PROFILES_DIR`
- Variables opcionales de Airflow configuradas para GCS si se desea sobreescribir valores por defecto

---

## 1) Módulo Python: `modules/ipm_load.py`

### 1.1 `ensure_dataset`
- Caso: dataset existente → log de existente
- Caso: dataset inexistente → creación con ubicación `us-central1`

Resultado: OK (idempotente).

### 1.2 `get_latest_excel_from_gcs_folder`
- Caso: múltiples `.xlsx` en carpeta → retorna el más reciente por `time_created`
- Caso: sin archivos → `ValueError`

Resultado: OK con datos; error controlado sin archivos.

### 1.3 `download_excel_from_gcs`
- Caso: blob existente → crea temporal `.xlsx`, descarga, tamaño > 0
- Caso: blob inexistente → error claro

Resultado: OK; manejo de error correcto.

### 1.4 `transform_excel`
- Quitar primera fila guía si existe
- Renombrar a esquema esperado: `cod_mpio`, `Municipio`, `Total`, `IPM_*`, `I1..I15_*`, `fecha_lectura`
- Convertir TODAS las columnas a `STRING` (preservación en bronze)
- Añadir `fecha_lectura` en UTC

Resultado: OK con dataset de muestra que incluye casos con letras, separadores y vacíos.

### 1.5 `_load_df_to_bq` / `load_dataframe_to_bq`
- Esquema en BigQuery: todas `STRING` excepto `fecha_lectura` `TIMESTAMP`
- `WRITE_TRUNCATE` reemplaza tabla

Resultado: OK; tabla creada y reemplazada correctamente.

### 1.6 `cleanup_temp_paths`
- Elimina rutas válidas, ignora `None` o vacías, no falla ante inexistentes

Resultado: OK; verificado en flujo de ingesta del DAG.

---

## 2) Módulo Python: `modules/ipm_extract.py`

### 2.1 `extract_file_id_from_url`
- Caso: URL tipo `https://drive.google.com/file/d/FILE_ID/view`
- Caso: URL tipo `https://drive.google.com/open?id=FILE_ID`
- Caso: URL tipo `https://docs.google.com/spreadsheets/d/FILE_ID/edit`
- Caso: ID directo `FILE_ID`
- Caso: URL inválida → lanza `ValueError`

Resultado esperado: retorna el `FILE_ID` correcto o excepción descriptiva.
Resultado: OK en escenarios válidos y error controlado en inválidos.

### 2.2 `get_public_download_url`
- Caso: `FILE_ID` válido → genera URL `https://drive.google.com/uc?export=download&id=FILE_ID`

Resultado: OK.

### 2.3 `download_file_from_public_link`
- Caso: enlace público válido (archivo pequeño) → descarga exitosa, retorna `(ruta_tmp, nombre_original)`
- Caso: enlace público con advertencia de archivo grande → detecta HTML intermedio y continúa descarga
- Caso: enlace inválido → `HTTPError` o error claro

Verificaciones: tamaño de archivo > 0, extensión `.xlsx` por defecto si no se obtiene del header.
Resultado: OK con enlaces válidos; manejo de error correcto con enlaces inválidos.

### 2.4 `download_file_from_drive_api` (Service Account)
- Caso: archivo compartido con la Service Account → descarga exitosa, conserva nombre
- Caso: `use_service_account=False` → `NotImplementedError`
- Caso: mime de spreadsheet → fuerza extensión `.xlsx` si falta

Resultado: OK; la vía OAuth no está implementada por diseño.

### 2.5 `upload_file_to_gcs`
- Caso: subida a carpeta existente → retorna `gs://...`
- Caso: carpeta inexistente → GCS crea el prefijo automáticamente
- Caso: objeto ya existe y `overwrite=True` → elimina y reemplaza
- Caso: objeto ya existe y `overwrite=False` → no reemplaza, log de advertencia
- Caso: bucket inválido → `ValueError`

Resultado: OK (incluye sobrescritura controlada y validación de bucket).

### 2.6 `move_file_from_drive_to_gcs`
- Caso feliz: enlace público válido → retorna `gs://bucket/folder/archivo.xlsx` y elimina temporal local
- Caso de error en subida → intenta limpiar temporal y propaga excepción

Resultado: OK en caso feliz; limpieza de temporales verificada.

---

## 2b) Módulo Python: `modules/ipm_transform.py`

### 2b.1 `ensure_dataset`
- Caso: dataset existente (silver o gold) → log de existente
- Caso: dataset inexistente → creación con ubicación `us-central1` y descripción apropiada según capa

Resultado: OK (idempotente).

---

## 3) DAG: `src_ipm_load_dag.py`

### Grupo bronze (ingesta a bronze)
- Pasa `XCom` desde `download_excel` a `transform_dataframe` y de este a `load_to_bq`
- `cleanup_temp_files` con `TriggerRule.ALL_DONE` limpia siempre temporales
- Al finalizar exitosamente, ejecuta automáticamente el DAG `src_planeacion_transf_ipm`

Pruebas:
- Ejecución end-to-end con un Excel de muestra desde GCS
- Verificar creación de `bronze_dpt_planeacion_municipal_dev.ipm_raw_data`
- Verificar que ejecuta automáticamente el DAG de transformación

Resultado: OK. Tabla bronze creada, temporales limpiados, y DAG de transformación ejecutado automáticamente.

---

## 4) DAG: `src_ipm_extract_dag.py`

### Flujo
- `start` → `upload_file_from_drive_to_gcs` → `trigger_inges_ipm` → `end`

### Pruebas
- Enlace público válido → retorna `gs://.../ipm/archivo.xlsx`
- Logs informativos con origen y destino
- Verificar que ejecuta automáticamente el DAG `src_planeacion_load_ipm`

Resultado: OK con enlace válido; disparo automático del DAG de ingesta verificado.

---

## 5) DAG: `src_ipm_transform_dag.py`

### Grupo silver (dbt)
Secuencia y paralelismo controlado:
- `stg` → `normalize_text` → `[transform_types, clean_numbers]` → `detect_negatives` → `apply_validations` → `clean` → `test`

Pruebas por modelo:
- `ipm_transform_stg`: mapeo de nombres a `snake_case`
- `ipm_transform_normalize_text`: eliminación de acentos y mayúsculas en `cod_mpio` y `municipio`
- `ipm_transform_transform_types`: `SAFE_CAST` a `INT64` y `FLOAT64` con `ROUND(2)`
- `ipm_transform_clean_numbers`: limpieza con `REGEXP_REPLACE` y `SAFE_CAST` a `INT64`
- `ipm_transform_detect_negatives`: flags `has_negative_value` e `is_total_zero_or_empty`
- `ipm_transform_apply_validations`: reglas de negocio para poner en 0 numéricas cuando hay negativos o total vacío; `SAFE_CAST` de porcentajes a `FLOAT64`
- `ipm_transform_clean`: `DATE(fecha_lectura)` y materialización como tabla
`dbt test` ejecutado sobre `ipm_transform_clean` y `ipm_processed_data`.

Resultado: OK. Cadena ejecutada y pruebas `dbt test` superadas en modelos clean y gold.

**Nota importante:** Este DAG espera que los datos ya estén disponibles en `bronze_dpt_planeacion_municipal_dev.ipm_raw_data` (normalmente cargados por `src_ipm_load_dag.py`).

### Grupo gold (dbt)
- `ensure_dataset` → `dbt_run_gold` → `dbt_test`
- Verificar que el modelo excluye porcentajes y `fecha_lectura`, y renombra columnas a mayúsculas

Resultado: OK. Estructura final conforme a especificación.

---

## 6) Modelos dbt: consultas de verificación rápidas

- Conteos: `SELECT COUNT(*)` en cada vista/tabla
- Nulos: `SELECT COUNTIF(col IS NULL)` para columnas clave tras cada etapa
- Negativos: `SELECT COUNTIF(col < 0)` deben ser 0 después de `apply_validations`
- Totales 0 o vacíos: verificación de regla aplicada
- Tipos: `INFORMATION_SCHEMA.COLUMNS` para confirmar tipos esperados en `clean`

Resultado: OK en verificación manual post-ejecución.

---

## 7) Datos de prueba mínimos sugeridos

- Fila con `total` vacío y varios indicadores con letras en medio
- Fila con valores negativos
- Fila con separadores de miles y decimales mixtos `1.234` y `2,345`
- Fila con `municipio` y `cod_mpio` con acentos y diferentes casos

Objetivo: cubrir rutas de limpieza, casting, flags y validaciones.

---

## 8) Comandos de ejecución usados

- dbt (en contenedor Airflow):
  - `cd /opt/airflow/dags/gdv_general_dbt_dag/dbt && dbt run --select ipm_transform_stg`
  - `dbt run --select ipm_transform_normalize_text`
  - `dbt run --select ipm_transform_transform_types`
  - `dbt run --select ipm_transform_clean_numbers`
  - `dbt run --select ipm_transform_detect_negatives`
  - `dbt run --select ipm_transform_apply_validations`
  - `dbt run --select ipm_transform_clean`
  - `dbt run --select ipm_processed_data`
  - `dbt test --select ipm_transform_clean`
  - `dbt test --select ipm_processed_data`

- Airflow (UI): ejecución manual de DAGs con parámetros según documentación.

---

## 9) Resultados esperados clave

- `bronze`: todas las columnas `STRING`, `fecha_lectura` `TIMESTAMP`
- `clean_numbers`: sufijos `_cleaned` con enteros válidos en columnas absolutas
- `detect_negatives`: flags consistentes con los datos
- `apply_validations`: columnas numéricas en 0 si hay negativos o `total` vacío; porcentajes a `FLOAT64`
- `clean`: `fecha_lectura` como `DATE`, materializada como tabla
- `gold`: columnas absolutas finales, sin porcentajes ni `fecha_lectura`, nombres estandarizados

---

## 10) Observaciones

- El paralelismo en silver permite optimizar tiempo, pero la cadena de validaciones depende de `clean_numbers`.
- `WRITE_TRUNCATE` en bronze asegura idempotencia por ejecución.
- La limpieza de temporales garantiza que no queden artefactos locales.

---

## 11) Próximos pasos de prueba automatizada (opcional)

- Pytest para unit tests de utilidades: mocks de GCS y BigQuery para `ipm_load.py` y mocks de GCS y HTTP (requests) para `ipm_extract.py`
- Great Expectations o dbt tests adicionales para validaciones de esquema y contenido
- Hooks de CI para ejecutar `dbt run --select state:modified+` y `dbt test` en PRs
