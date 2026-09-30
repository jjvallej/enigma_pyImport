Documentacion: DAG src_database_sc_stackdb_ingest_dag

1. Informacion general
Este DAG ingiere datos desde la base Postgres DataStackDB (esquema sc_stackdb, tabla encuesta_hogares) y exporta el resultado a GCS en CSV plano.

2. Conexion en Airflow
La conexión es necesaria porque los DAGs no guardan credenciales en código; Airflow provee host, puerto, base, usuario y clave. Sin esta conexión el DAG no puede consultar Postgres, por lo que a continuación se presenta un guia para crear la conexión llamada “con_database_sc_stackdb”:

Objetivo: habilitar la conexión Postgres que usan los DAGs de ingesta y carga de database_sc_stackdb.

 Pasos en la UI:
  - Abrir Admin > Connections > +.
  - Conn Id: con_database_sc_stackdb.
  - Conn Type: Postgres.
  - Host: 172.16.19.3
  - Port: 5432
  - Schema o Database: DataStackDB
  - Login: app_owner
  - Password: Ecf5#a87gd5A17bC_9BCB9
  - Extra (JSON): {"options": "-c search_path=sc_stackdb"}
  - Guardar.
 Notas:
  - Si las credenciales cambian, solo se actualiza la conexion en Airflow; no hace falta modificar config.yaml.
  - El esquema sc_stackdb debe existir y contener la tabla encuesta_hogares.
  - El Conn Id debe coincidir con config.yaml.database_sc_stackdb.connection_id.



3. Estructura del DAG
Tareas principales en secuencia: start, check_db_connection, ensure_gcs_folder, export_encuesta_hogares_to_gcs, end.

4. Flujo de tareas
 start: marca el inicio.    
 check_db_connection: valida la conexion Postgres con el conn_id con_database_sc_stackdb; imprime host, puerto, base, usuario, password (a solicitud) y ejecuta SELECT 1.
 ensure_gcs_folder: crea el prefijo en GCS si no existe.
 export_encuesta_hogares_to_gcs: usa PostgresToGCSOperator con la consulta SELECT * FROM {schema}.{table} LIMIT 1000 y escribe en gs://<bucket>/<gcs_base_folder>/<export_filename>.
 end: marca el fin.

5. Configuracion y parametros
 Conexion: con_database_sc_stackdb en Airflow (contiene credenciales reales).
 Parametros desde config.yaml.database_sc_stackdb: schema, table, gcs_base_folder, export_filename, export_format=csv, field_delimiter=",", gzip=false, use_server_side_cursor=true.
 Bucket: DEFAULT_BUCKET_NAME segun ambiente (modules.config).
 Limite: 1000 filas para prueba rapida; se puede quitar o cambiar.

6. Consideraciones
 Las credenciales se toman solo de la conexion de Airflow; en config.yaml se definen SQL y rutas.
 gzip en false para inspeccion facil; activar si se requiere.
 Si se cambia tabla o esquema origen, actualizar schema y table en config.yaml.
 El uso de server side cursor evita cargar todo en memoria en PostgresToGCS.
 El prefijo en GCS se crea antes de exportar para evitar fallas por rutas inexistentes.

7. Ultima actualizacion
2025-12-17

