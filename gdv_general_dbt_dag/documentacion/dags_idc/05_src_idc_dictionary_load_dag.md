# Documentación: dags_idc/src_idc_dictionary_load_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_load_idc_dictionary" que se encarga de extraer datos del archivo CSV del diccionario IDC desde Google Cloud Storage, transformarlos mínimamente y cargarlos directamente en la capa gold de BigQuery. El DAG está diseñado para buscar automáticamente el archivo CSV más reciente en la carpeta configurada de GCS, procesarlo y cargarlo como la tabla "dim_idc" en el dataset de gold.

El propósito principal de este DAG es automatizar la carga del diccionario de indicadores desde GCS hacia BigQuery, creando la tabla dimensional "dim_idc" que contiene la estructura jerárquica de factores, pilares, indicadores y subindicadores. Esta tabla es esencial para el proceso de transformación del IDC, ya que se une con las tablas transformadas para crear la tabla final "fact_idc" en la capa gold.

## 2. Estructura del DAG

El DAG está compuesto por un grupo de tareas llamado "gold" que contiene todas las operaciones relacionadas con la carga del diccionario a la capa gold. Este grupo incluye cuatro tareas principales. La primera tarea es "ensure_dataset" que asegura que el dataset de gold exista en BigQuery. La segunda tarea es "download_csv" que descarga el archivo CSV más reciente desde GCS. La tercera tarea es "load_csv" que carga el archivo CSV a BigQuery como la tabla "dim_idc". La cuarta tarea es "cleanup_temp_files" que elimina los archivos temporales creados durante el proceso. Después del grupo "gold", el DAG incluye un "TriggerDagRunOperator" llamado "trigger_ingest_idc" que dispara el DAG de ingesta "src_planeacion_ingest_idc" sin esperar a que termine. Finalmente, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "GCS_BUCKET_NAME" obtiene su valor de "DEFAULT_BUCKET_NAME" importado desde "modules.config", que representa el nombre del bucket de GCS. La variable "GCS_FOLDER_PATH" obtiene su valor de "CONF.idc.gcs_folder", que define la carpeta en GCS donde se buscan los archivos CSV. La variable "TABLE_NAME" obtiene su valor de "CONF.idc.tables.dictionary", que define el nombre de la tabla en BigQuery donde se cargará el diccionario, típicamente "dim_idc". La variable "DATASET_ID_GOLD" se importa directamente desde "modules.config" y representa el identificador del dataset de gold en BigQuery.

## 4. Estructura del Diccionario

El archivo CSV del diccionario contiene la estructura jerárquica de indicadores con las siguientes columnas. La columna "ID_FACTOR" contiene el identificador del factor. La columna "ID_PILAR" contiene el identificador del pilar. La columna "ID_INDICADOR" contiene el identificador del indicador. La columna "ID_SUBINDICADOR" contiene el identificador del subindicador. La columna "NOM_FACTOR" contiene el nombre del factor. La columna "NOM_PILAR" contiene el nombre del pilar. La columna "NOM_INDICADOR" contiene el nombre del indicador. La columna "NOM_SUBINDICADOR" contiene el nombre del subindicador. Esta estructura permite crear relaciones jerárquicas entre los diferentes niveles de indicadores.

## 5. Función _ensure_dataset_gold_task

La función "_ensure_dataset_gold_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.idc.idc_dictionary_load" pasando el identificador del dataset de gold. Esta función asegura que el dataset exista en BigQuery antes de intentar cargar datos en él, creándolo si no existe.

## 6. Función _download_csv_task

La función "_download_csv_task" es responsable de buscar y descargar el archivo CSV más reciente desde GCS. La función intenta primero obtener la configuración desde Variables de Airflow usando "Variable.get", pero si estas no existen, utiliza los valores por defecto definidos en el código. La función utiliza "get_latest_csv_from_gcs_folder" para encontrar el archivo CSV más reciente en la carpeta especificada, y luego utiliza "download_csv_from_gcs" para descargarlo a un archivo temporal local. La función retorna la ruta del archivo descargado, que se almacena en XCom para ser utilizada por las tareas siguientes.

## 7. Función _load_csv_task

La función "_load_csv_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la ruta del archivo CSV descargado por la tarea anterior. La función utiliza "load_csv_to_bq" del módulo "modules.idc.idc_dictionary_load" para cargar el archivo CSV en BigQuery, especificando el dataset de gold y el nombre de la tabla destino. La función procesa el CSV transformando todas las columnas a tipo STRING para mantener la flexibilidad, y agrega una columna "fecha_lectura" con la fecha y hora actual para rastrear cuándo se cargó el diccionario. La función retorna el nombre de la tabla creada.

## 8. Función _cleanup_temp_files_task

La función "_cleanup_temp_files_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la ruta del archivo CSV creado durante el proceso. La función utiliza "cleanup_temp_paths" del módulo "modules.idc.idc_dictionary_load" para eliminar el archivo temporal, liberando espacio en el sistema de archivos. Esta tarea está configurada con "TriggerRule.ALL_DONE", lo que significa que se ejecutará independientemente del resultado de las tareas anteriores, asegurando que los archivos temporales siempre se eliminen.

## 9. Dependencias entre Tareas

Dentro del grupo "gold", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "download_csv", que a su vez precede a "load_csv", y finalmente "load_csv" precede a "cleanup_temp_files". La tarea "cleanup_temp_files" tiene la regla de activación "ALL_DONE", lo que garantiza que se ejecute incluso si alguna tarea anterior falla. Después del grupo "gold", el flujo continúa con "end" y luego "trigger_ingest_idc". La tarea "start" precede a todo el grupo "gold".

## 10. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_load_idc_dictionary" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:carga", "fuente:idc_dictionary" y "ejecución:manual".

## 11. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento del diccionario IDC, siendo disparado automáticamente por el DAG de ingesta una vez que el archivo CSV se encuentra en GCS. Una vez que el diccionario se carga en BigQuery como la tabla "dim_idc" en la capa gold, el DAG dispara automáticamente el DAG de ingesta del IDC principal, creando un flujo automatizado que continúa con el procesamiento de los datos. El uso de "TriggerDagRunOperator" con "wait_for_completion=False" permite que el DAG actual se marque como exitoso inmediatamente después de disparar el siguiente DAG.

## 12. Notas Importantes

Es importante asegurarse de que exista al menos un archivo CSV en la carpeta configurada de GCS antes de ejecutar el DAG, ya que la función "get_latest_csv_from_gcs_folder" fallará si no encuentra ningún archivo. El DAG está diseñado para procesar un archivo CSV con una estructura específica que incluye las columnas ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, NOM_FACTOR, NOM_PILAR, NOM_INDICADOR y NOM_SUBINDICADOR. Si el archivo tiene una estructura diferente, el proceso puede fallar o producir resultados inesperados. El diccionario se carga directamente en la capa gold porque es una tabla dimensional que no requiere transformaciones adicionales y es necesaria para el proceso de transformación del IDC principal. Los archivos temporales se eliminan automáticamente al finalizar el proceso, pero si el DAG falla antes de llegar a la tarea de limpieza, estos archivos pueden quedar en el sistema. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde GCS y escribir en BigQuery. La tabla "dim_idc" debe existir en la capa gold antes de ejecutar el DAG de transformación del IDC, ya que esta tabla se une con las tablas transformadas para crear la tabla final "fact_idc".

