# Flujo detallado de DAGs - Fuente database_sc_stackdb

Este flujo se ejecuta con dos DAGs: `src_database_sc_stackdb_ingest_dag` y `src_database_sc_stackdb_load_dag`. El orden operativo es primero ingesta (extrae desde Postgres a GCS) y luego carga (de GCS a BigQuery bronze). A diferencia de otras fuentes, aquí no existe un DAG de transformación en esta ruta. En el estado actual ambos DAGs leen su frecuencia desde `database_sc_stackdb.schedule_interval`.

La ejecución inicia en `dags_database_sc_stackdb/src_database_sc_stackdb_ingest_dag.py`. Este DAG usa parámetros de `config/config.yaml` en la sección `database_sc_stackdb`, especialmente `connection_id`, `schema`, `table`, `gcs_base_folder`, `export_filename`, `export_format`, `field_delimiter`, `gzip` y `use_server_side_cursor`. Primero valida conexión con `check_db_connection` (usa `PostgresHook`), luego asegura el prefijo en Cloud Storage con `ensure_gcs_folder`, y finalmente ejecuta `PostgresToGCSOperator` para exportar datos de `sc_stackdb.encuesta_hogares` hacia `gs://<bucket>/<gcs_base_folder>/<export_filename>`. Actualmente el SQL de exportación está definido como `SELECT * FROM {schema}.{table} LIMIT 1000`.

La carga está en `dags_database_sc_stackdb/src_database_sc_stackdb_load_dag.py`. Este DAG usa `DEFAULT_BUCKET_NAME`, `PROJECT_ID`, `DATASET_ID_BRONZE` y la configuración de `database_sc_stackdb` (`target_dataset`, `target_table`, `source_format`, `write_disposition`, `skip_leading_rows`, `field_delimiter`, `autodetect`). Con `GCSToBigQueryOperator` toma el objeto exportado en GCS y lo carga en la tabla destino `PROJECT_ID.target_dataset.target_table` dentro de bronze. Internamente el TaskGroup se llama `brone` (nombre histórico en código) y contiene la tarea `load_database_test_raw_data`.

Las variables globales de entorno y proyecto se resuelven en `modules/config.py` mediante `ENVIRONMENT` y pueden sobreescribirse con `GCP_PROJECT`, `GCP_LOCATION`, `GCS_BUCKET_NAME`, `BQ_DATASET_BRONZE`, `BQ_DATASET_SILVER` y `BQ_DATASET_GOLD`. En esta fuente aplican directamente `DEFAULT_BUCKET_NAME`, `PROJECT_ID` y el dataset bronze efectivo.

Lo parametrizable en `config/config.yaml` para esta fuente está en `database_sc_stackdb`: `connection_id`, `database`, `schema`, `table`, `host`, `port`, `user`, `password`, `gcs_base_folder`, `export_filename`, `export_format`, `field_delimiter`, `gzip`, `use_server_side_cursor`, `target_dataset`, `target_table`, `write_disposition`, `source_format`, `skip_leading_rows`, `autodetect`, `schedule_interval` y `start_days_ago`. También es parametrizable por ambiente en `environments.<env>`: `project_id`, `location`, `bucket_name`, `dataset_bronze`, `dataset_silver` y `dataset_gold`.

## Explicación simple (modo no técnico)

Este flujo mueve datos de una base PostgreSQL hacia BigQuery en dos pasos. En el primer paso, un DAG se conecta a la base `sc_stackdb`, saca datos de la tabla `encuesta_hogares` y los guarda como archivo en Cloud Storage. En el segundo paso, otro DAG toma ese archivo del bucket y lo carga en una tabla de BigQuery en la capa bronze.

En resumen, el camino es: tabla en PostgreSQL -> archivo en Cloud Storage -> tabla bronze en BigQuery. Si cambian conexión, rutas, formato del archivo o nombre de la tabla destino, esos ajustes se hacen en `config/config.yaml` sin rehacer toda la lógica del flujo.
