# Documentación: dags_idi/src_idi_load_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_load_idi" que se encarga de cargar archivos CSV del IDI desde Google Cloud Storage hacia la capa bronze de BigQuery. El DAG está diseñado para buscar automáticamente todas las carpetas de años en GCS, descargar los archivos CSV correspondientes y cargarlos como tablas separadas en BigQuery, una tabla por cada año.

El propósito principal de este DAG es automatizar la carga de datos desde GCS hacia BigQuery, procesando todos los años disponibles de manera automática. Cada año se carga en una tabla separada con el formato "idi_raw_data_territorio_{año}", permitiendo mantener un historial de datos por año y facilitando el procesamiento posterior.

## 2. Estructura del DAG

El DAG está compuesto por un grupo de tareas llamado "bronze" que contiene todas las operaciones relacionadas con la carga de datos a la capa bronze. Este grupo incluye dos tareas principales. La primera tarea es "ensure_dataset" que asegura que el dataset de bronze exista en BigQuery. La segunda tarea es "load_all_years_to_bigquery" que busca todas las carpetas de años en GCS, descarga los archivos CSV y los carga en BigQuery. Después del grupo "bronze", el DAG incluye un "TriggerDagRunOperator" llamado "trigger_transform_dag" que dispara el DAG de transformación "src_planeacion_transf_idi" sin esperar a que termine. Finalmente, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "GCS_BUCKET_NAME" obtiene su valor de "DEFAULT_BUCKET_NAME" importado desde "modules.config", que representa el nombre del bucket de GCS. La variable "GCS_BASE_FOLDER" obtiene su valor de "CONF.idi.gcs_base_folder", que define la carpeta base en GCS donde se encuentran los archivos organizados por año. La variable "TABLE_PREFIX" obtiene su valor de "CONF.idi.tables.bronze_prefix", que define el prefijo para los nombres de las tablas en BigQuery. La variable "DATASET_ID_BRONZE" se importa directamente desde "modules.config" y representa el identificador del dataset de bronze en BigQuery.

## 4. Estructura de Archivos en GCS

Los archivos CSV del IDI se almacenan en GCS organizados por año en la siguiente estructura. La carpeta base está definida por "GCS_BASE_FOLDER", típicamente "data_staging/dpt_planeacion_municipal/idi". Dentro de esta carpeta, cada año tiene su propia subcarpeta, por ejemplo "2024", "2023", "2022", etc. Dentro de cada carpeta de año, se encuentra un archivo CSV con el formato "resultados_{año}.csv". Esta estructura permite que el DAG identifique automáticamente todos los años disponibles y procese cada uno de manera independiente.

## 5. Estructura de Tablas en BigQuery

Cada año se carga en una tabla separada en el dataset de bronze con el formato "idi_raw_data_territorio_{año}". Por ejemplo, el año 2024 se carga en la tabla "idi_raw_data_territorio_2024", el año 2023 en "idi_raw_data_territorio_2023", y así sucesivamente. Todas las tablas se crean en el mismo dataset de bronze, permitiendo mantener un historial completo de datos por año y facilitando consultas y transformaciones posteriores.

## 6. Función _ensure_dataset_bronze_task

La función "_ensure_dataset_bronze_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.idi.idi_load" pasando el identificador del dataset de bronze. Esta función asegura que el dataset exista en BigQuery antes de intentar cargar datos en él, creándolo si no existe. La función imprime información de depuración indicando qué dataset se está verificando y confirmando cuando está listo.

## 7. Función _load_all_years_task

La función "_load_all_years_task" es responsable de buscar todas las carpetas de años en GCS, descargar los archivos CSV correspondientes y cargarlos en BigQuery. La función utiliza "load_all_years_idi_to_bq" del módulo "modules.idi.idi_load" para realizar la operación completa. La función pasa todos los parámetros necesarios, incluyendo el nombre del bucket, la carpeta base, el identificador del dataset y el prefijo de las tablas. La función imprime información de depuración sobre el proceso, incluyendo el bucket, la carpeta base, el dataset destino y el prefijo de tablas. Al finalizar, la función imprime un resumen con el total de años cargados y el nombre de cada tabla creada, y retorna un diccionario con los años procesados y las referencias de las tablas creadas.

## 8. Proceso de Carga

El proceso de carga ejecutado por "load_all_years_idi_to_bq" incluye varios pasos. Primero, lista todas las carpetas en la carpeta base de GCS para identificar los años disponibles. Para cada año encontrado, construye la ruta al archivo CSV esperado con el formato "resultados_{año}.csv". Luego descarga el archivo CSV desde GCS a un archivo temporal local. El archivo CSV se lee con pandas usando un delimitador de punto y coma, ya que los archivos del IDI utilizan este formato. El esquema de la tabla se detecta automáticamente basándose en los datos del CSV. Finalmente, carga el DataFrame en BigQuery creando una nueva tabla con el nombre "idi_raw_data_territorio_{año}" en el dataset de bronze. Si la tabla ya existe, se sobrescribe con los nuevos datos.

## 9. Dependencias entre Tareas

Dentro del grupo "bronze", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "load_all_years_to_bigquery". Después del grupo "bronze", el flujo continúa con "trigger_transform_dag" y luego "end". La tarea "start" precede a todo el grupo "bronze".

## 10. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_load_idi" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:carga", "fuente:idi" y "ejecución:manual". El "TriggerDagRunOperator" tiene el parámetro "reset_dag_run" establecido en "True", lo que permite re-ejecutar el DAG de transformación incluso si ya está en ejecución.

## 11. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos del IDI, siendo disparado automáticamente por el DAG de ingesta una vez que todos los archivos CSV se encuentran en GCS organizados por año. Una vez que los datos se cargan en BigQuery como tablas separadas por año, el DAG dispara automáticamente el DAG de transformación, creando un flujo automatizado que continúa con el procesamiento de los datos. El uso de "TriggerDagRunOperator" con "wait_for_completion=False" permite que el DAG actual se marque como exitoso inmediatamente después de disparar el siguiente DAG.

## 12. Notas Importantes

Es importante asegurarse de que exista al menos una carpeta de año en la carpeta base configurada en GCS antes de ejecutar el DAG, ya que la función "load_all_years_idi_to_bq" buscará carpetas que representen años. Cada carpeta de año debe contener un archivo CSV con el formato "resultados_{año}.csv" para que el proceso funcione correctamente. El DAG está diseñado para procesar todos los años disponibles automáticamente, por lo que si se agregan nuevos años a GCS, serán procesados en la siguiente ejecución del DAG. Los archivos CSV deben usar punto y coma como delimitador, ya que este es el formato esperado por el proceso de carga. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde GCS y escribir en BigQuery. Si algún año falla durante el proceso de carga, el DAG puede continuar procesando los años restantes dependiendo de la implementación de "load_all_years_idi_to_bq", pero es importante revisar los logs para identificar cualquier problema. Las tablas se crean o sobrescriben en cada ejecución, por lo que es importante asegurarse de que los datos en GCS sean correctos antes de ejecutar el DAG.

