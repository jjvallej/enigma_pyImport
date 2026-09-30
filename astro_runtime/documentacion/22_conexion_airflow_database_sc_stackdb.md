Guia: Crear conexion Postgres en Airflow con_database_sc_stackdb

1. Objetivo
Configurar en Airflow la conexion Postgres que usan los DAGs de ingesta y carga de database_sc_stackdb.

2. Pasos en la UI de Airflow
Abrir Admin > Connections > +.
Conn Id: con_database_sc_stackdb.
Conn Type: Postgres.
Campos:
 Host: 172.16.19.3
 Port: 5432
 Schema o Database: DataStackDB
 Login: app_owner
 Password: Ecf5#a87gd5A17bC_9BCB9
Extra (JSON):
 {"options": "-c search_path=sc_stackdb"}
Guardar.

3. Notas
Si las credenciales cambian, solo actualiza la conexion en Airflow; no es necesario modificar config.yaml para la conexion.
El esquema sc_stackdb debe existir y contener la tabla encuesta_hogares.
El Conn Id debe coincidir con config.yaml.database_sc_stackdb.connection_id.

4. Ultima actualizacion
2025-12-17

