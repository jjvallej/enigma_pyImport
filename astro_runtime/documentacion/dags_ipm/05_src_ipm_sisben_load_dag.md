# Documentación: dags_ipm/src_ipm_sisben_load_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_load_ipm_sisben" que se encarga de crear una tabla externa de BigQuery para los datos de IPM SISBEN. La tabla externa apunta directamente a los archivos CSV almacenados en Google Cloud Storage, lo que permite consultar los datos sin necesidad de cargarlos primero en BigQuery, optimizando el uso de almacenamiento y mejorando la eficiencia del proceso.

El propósito principal de este DAG es crear una estructura de tabla externa en BigQuery que referencie los archivos CSV en GCS, permitiendo que BigQuery consulte los datos directamente desde el almacenamiento sin necesidad de duplicar la información. Una vez creada la tabla externa, el DAG dispara automáticamente el DAG de transformación para continuar con el procesamiento de los datos.

## 2. Estructura del DAG

El DAG está compuesto por cinco tareas principales conectadas en secuencia. La primera tarea es un "EmptyOperator" con el identificador "start" que marca el inicio del flujo. La segunda tarea es un "BigQueryCreateEmptyDatasetOperator" llamado "ensure_dataset" que asegura que el dataset de bronze exista en BigQuery. La tercera tarea es un "BigQueryInsertJobOperator" llamado "create_external_table" que ejecuta el SQL para crear la tabla externa. La cuarta tarea es un "TriggerDagRunOperator" llamado "trigger_transform_dag" que dispara el DAG de transformación "src_planeacion_transform_ipm_sisben" sin esperar a que termine. La quinta y última tarea es otro "EmptyOperator" con el identificador "end" que marca el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "TABLE_NAME" obtiene su valor de "CONF.ipm_sisben.tables.bronze", que define el nombre de la tabla externa en BigQuery. La variable "GCS_BUCKET_NAME" obtiene su valor de "DEFAULT_BUCKET_NAME" importado desde "modules.config", que representa el nombre del bucket de GCS. La variable "GCS_PATH" obtiene su valor de "CONF.ipm_sisben.external_table.gcs_path", que define la ruta dentro del bucket donde se encuentran los archivos CSV. La variable "SKIP_LEADING_ROWS" obtiene su valor de "CONF.ipm_sisben.external_table.skip_leading_rows", que indica cuántas filas se deben omitir al inicio del archivo CSV. La variable "FIELD_DELIMITER" obtiene su valor de "CONF.ipm_sisben.external_table.field_delimiter", que define el delimitador de campos en el archivo CSV. La variable "ALLOW_QUOTED_NEWLINES" obtiene su valor de "CONF.ipm_sisben.external_table.allow_quoted_newlines", que indica si se permiten saltos de línea dentro de campos entre comillas. Las variables "PROJECT_ID", "LOCATION" y "DATASET_ID_BRONZE" se importan directamente desde "modules.config" y representan el identificador del proyecto de GCP, la ubicación del dataset y el identificador del dataset de bronze en BigQuery.

## 4. Función get_create_external_table_sql

La función "get_create_external_table_sql" genera dinámicamente el SQL necesario para crear la tabla externa en BigQuery. La función construye el URI de GCS combinando "GCS_BUCKET_NAME" y "GCS_PATH" en el formato "gs://bucket_name/path". La función genera una declaración "CREATE EXTERNAL TABLE IF NOT EXISTS" que define la estructura de la tabla con todas las columnas necesarias, todas definidas como tipo "STRING" para mantener la flexibilidad en el procesamiento posterior. La declaración incluye una sección "OPTIONS" que especifica el formato del archivo como "CSV", el URI o URIs de los archivos, el número de filas iniciales a omitir, el delimitador de campos y si se permiten saltos de línea entre comillas. La función retorna el SQL completo como una cadena de texto, que será ejecutado por el operador "BigQueryInsertJobOperator".

## 5. Tarea ensure_dataset

La tarea "ensure_dataset" utiliza "BigQueryCreateEmptyDatasetOperator" para asegurar que el dataset de bronze exista en BigQuery antes de intentar crear la tabla externa. El operador está configurado con el identificador del dataset, el identificador del proyecto y la ubicación del dataset. El parámetro "exists_ok" está establecido en "True", lo que significa que el operador no fallará si el dataset ya existe, permitiendo ejecuciones idempotentes del DAG.

## 6. Tarea create_external_table

La tarea "create_external_table" utiliza "BigQueryInsertJobOperator" para ejecutar el SQL generado por la función "get_create_external_table_sql". El operador está configurado con una configuración de consulta que incluye el SQL generado y el parámetro "useLegacySql" establecido en "False" para usar la sintaxis SQL estándar de BigQuery. El operador también está configurado con la ubicación y el identificador del proyecto para asegurar que la operación se realice en el contexto correcto.

## 7. Tarea trigger_transform_dag

La tarea "trigger_transform_dag" utiliza "TriggerDagRunOperator" para disparar el DAG de transformación "src_planeacion_transform_ipm_sisben" una vez que la tabla externa se ha creado exitosamente. El operador está configurado con "wait_for_completion" establecido en "False", lo que permite que el DAG actual se marque como exitoso inmediatamente después de disparar el DAG de transformación, sin esperar a que este termine. El parámetro "reset_dag_run" está establecido en "True", lo que permite re-ejecutar el DAG de transformación incluso si ya está en ejecución, útil para casos donde se necesita reprocesar los datos.

## 8. Dependencias entre Tareas

Las tareas están conectadas en una secuencia lineal donde "start" precede a "ensure_dataset", que precede a "create_external_table", que precede a "trigger_transform_dag", y finalmente "trigger_transform_dag" precede a "end". Esta estructura garantiza que cada tarea se ejecute en el orden correcto, asegurando que el dataset exista antes de crear la tabla externa, y que la tabla externa se cree antes de disparar el DAG de transformación.

## 9. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_load_ipm_sisben" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:carga", "fuente:ipm_sisben" y "ejecución:manual".

## 10. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos de IPM SISBEN, siendo ejecutado después de que los archivos CSV se encuentran en GCS. El DAG crea una tabla externa que permite a BigQuery consultar los datos directamente desde GCS, optimizando el uso de almacenamiento. Una vez creada la tabla externa, el DAG dispara automáticamente el DAG de transformación para continuar con el procesamiento de los datos.

## 11. Notas Importantes

Es importante asegurarse de que los archivos CSV existan en la ruta configurada en "CONF.ipm_sisben.external_table.gcs_path" antes de ejecutar el DAG, ya que aunque BigQuery permite crear tablas externas incluso si los archivos no existen, la tabla estará vacía y las consultas fallarán. La estructura de columnas definida en la función "get_create_external_table_sql" debe coincidir con la estructura real de los archivos CSV, ya que cualquier discrepancia puede causar errores al consultar la tabla. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para crear datasets y tablas en BigQuery, así como para acceder a los archivos en GCS. El uso de tablas externas es eficiente en términos de almacenamiento, pero puede tener un impacto en el rendimiento de las consultas comparado con tablas nativas de BigQuery, especialmente para consultas complejas o grandes volúmenes de datos. La configuración de "skip_leading_rows", "field_delimiter" y "allow_quoted_newlines" debe coincidir con el formato real de los archivos CSV para asegurar que los datos se lean correctamente.
