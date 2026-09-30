Documentacion: DAG src_database_sc_stackdb_load_dag

1. Informacion general
Este DAG carga a BigQuery (bronze) el CSV generado por el DAG de ingesta. Toma el archivo desde GCS y lo escribe en la tabla database_test_raw_data del dataset bronze configurado.

2. Estructura del DAG
Tareas en secuencia: start, TaskGroup brone con load_database_test_raw_data, end.

3. Flujo de tareas
 start: marca inicio.
 brone.load_database_test_raw_data: GCSToBigQueryOperator carga gs://<bucket>/<gcs_base_folder>/<export_filename> a <PROJECT_ID>.<target_dataset>.<target_table>.
 end: marca fin.

4. Configuracion y parametros
 Origen GCS: gcs_base_folder y export_filename desde config.yaml.database_sc_stackdb.
 Destino BQ: target_dataset y target_table desde config.yaml.database_sc_stackdb; por defecto bronze_dpt_planeacion_municipal.database_test_raw_data.
 Parametros de carga: write_disposition=WRITE_TRUNCATE, source_format=CSV, field_delimiter=",", skip_leading_rows=1, autodetect=true.
 Bucket y project se toman de modules.config segun ENVIRONMENT.

5. Dependencias y triggers
 No hay triggers a otros DAGs; depende de que el DAG de ingesta haya escrito el archivo en GCS.

6. Manejo de datos de entrada
 Requiere que exista el archivo CSV en GCS con el nombre indicado por export_filename. Si el archivo no existe, la carga fallara.

7. Parametros sensibles
 No usa credenciales directas; se apoya en la configuracion de entorno de GCP en Composer para acceder a GCS y BigQuery.

8. Esquema y particion
 Usa autodetect=true; si se necesita schema fijo, se puede declarar en el operador. No se configura particion en esta etapa (bronze crudo).

9. Formato y delimitadores
 source_format=CSV, field_delimiter=",". Si el CSV no tiene encabezado, ajustar skip_leading_rows=0 en config.yaml.

10. Disposicion de escritura
 write_disposition=WRITE_TRUNCATE por defecto. Cambiar a WRITE_APPEND si se desea acumulacion.

11. Tolerancia a errores
 No hay reintentos custom; se usan los defaults del DAG. Si falla por archivo faltante o esquema, revisar GCS y la configuracion.

12. Observabilidad
 Revisar logs de la tarea load_database_test_raw_data para confirmar filas cargadas. El operador reporta conteo al finalizar.

13. Consideraciones
 Asegurar que el bucket y datasets correspondan al ambiente (ENVIRONMENT). Verificar permisos del servicio de Composer para leer GCS y escribir en BigQuery.



