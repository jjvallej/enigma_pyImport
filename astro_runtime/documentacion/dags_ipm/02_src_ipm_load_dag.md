# Documentación: dags_ipm/src_ipm_load_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_load_ipm" que se encarga de extraer datos del archivo Excel IPM desde Google Cloud Storage, transformarlos mínimamente y cargarlos en la capa bronze de BigQuery. El DAG está diseñado para buscar automáticamente el archivo Excel más reciente en la carpeta configurada de GCS, procesarlo y cargarlo en BigQuery.

El propósito principal de este DAG es automatizar la carga de datos desde GCS hacia BigQuery, realizando transformaciones mínimas necesarias para que los datos sean compatibles con BigQuery, manteniendo la mayor parte de la información en formato de cadena de texto para preservar la integridad de los datos originales.

## 2. Estructura del DAG

El DAG está compuesto por un grupo de tareas llamado "bronze" que contiene todas las operaciones relacionadas con la carga de datos a la capa bronze. Este grupo incluye cinco tareas principales. La primera tarea es "ensure_dataset" que asegura que el dataset de bronze exista en BigQuery. La segunda tarea es "download_excel" que descarga el archivo Excel más reciente desde GCS. La tercera tarea es "transform_dataframe" que transforma el archivo Excel descargado en un DataFrame de pandas y lo serializa en un archivo pickle temporal. La cuarta tarea es "load_to_bq" que carga el DataFrame desde el archivo pickle a BigQuery. La quinta tarea es "cleanup_temp_files" que elimina los archivos temporales creados durante el proceso. Después del grupo "bronze", el DAG incluye un "TriggerDagRunOperator" llamado "trigger_transf_ipm" que dispara el DAG de transformación "src_planeacion_transf_ipm" sin esperar a que termine. Finalmente, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "GCS_BUCKET_NAME" obtiene su valor de "DEFAULT_BUCKET_NAME" importado desde "modules.config", que representa el nombre del bucket de GCS. La variable "GCS_FOLDER_PATH" obtiene su valor de "CONF.ipm.gcs_folder", que define la carpeta en GCS donde se buscan los archivos Excel. La variable "TABLE_NAME_BRONZE" obtiene su valor de "CONF.ipm.tables.bronze", que define el nombre de la tabla en BigQuery donde se cargarán los datos. La variable "DATASET_ID_BRONZE" se importa directamente desde "modules.config" y representa el identificador del dataset de bronze en BigQuery. La constante "SHEET_INDEX" está establecida en 0, indicando que se procesará la primera hoja del archivo Excel.

## 4. Función _ensure_dataset_bronze_task

La función "_ensure_dataset_bronze_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.ipm.ipm_load" pasando el identificador del dataset de bronze. Esta función asegura que el dataset exista en BigQuery antes de intentar cargar datos en él, creándolo si no existe.

## 5. Función _download_excel_task

La función "_download_excel_task" es responsable de buscar y descargar el archivo Excel más reciente desde GCS. La función intenta primero obtener la configuración desde Variables de Airflow usando "Variable.get", pero si estas no existen, utiliza los valores por defecto definidos en el código. La función utiliza "get_latest_excel_from_gcs_folder" para encontrar el archivo Excel más reciente en la carpeta especificada, y luego utiliza "download_excel_from_gcs" para descargarlo a un archivo temporal local. La función retorna la ruta del archivo descargado, que se almacena en XCom para ser utilizada por las tareas siguientes.

## 6. Función _transform_task

La función "_transform_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la ruta del archivo Excel descargado por la tarea anterior. La función utiliza "transform_excel" del módulo "modules.ipm.ipm_load" para transformar el archivo Excel en un DataFrame de pandas, especificando el índice de la hoja a procesar. El DataFrame resultante se serializa en un archivo pickle temporal usando "tempfile.NamedTemporaryFile", y la ruta de este archivo se retorna y almacena en XCom para ser utilizada por la tarea de carga.

## 7. Función _load_task

La función "_load_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la ruta del archivo pickle creado por la tarea de transformación. La función carga el DataFrame desde el archivo pickle usando "pd.read_pickle" y luego utiliza "load_dataframe_to_bq" del módulo "modules.ipm.ipm_load" para cargar el DataFrame en BigQuery, especificando el dataset y el nombre de la tabla destino.

## 8. Función _cleanup_temp_files_task

La función "_cleanup_temp_files_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener las rutas de los archivos temporales creados durante el proceso, tanto el archivo Excel como el archivo pickle. La función utiliza "cleanup_temp_paths" del módulo "modules.ipm.ipm_load" para eliminar estos archivos temporales, liberando espacio en el sistema de archivos. Esta tarea está configurada con "TriggerRule.ALL_DONE", lo que significa que se ejecutará independientemente del resultado de las tareas anteriores, asegurando que los archivos temporales siempre se eliminen.

## 9. Dependencias entre Tareas

Dentro del grupo "bronze", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "download_excel", que a su vez precede a "transform_dataframe", que precede a "load_to_bq", y finalmente "load_to_bq" precede a "cleanup_temp_files". La tarea "cleanup_temp_files" tiene la regla de activación "ALL_DONE", lo que garantiza que se ejecute incluso si alguna tarea anterior falla. Después del grupo "bronze", el flujo continúa con "trigger_transf_dag" y luego "end". La tarea "start" precede a todo el grupo "bronze".

## 10. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_load_ipm" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:carga", "fuente:ipm" y "ejecución:manual".

## 11. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos del IPM, siendo disparado automáticamente por el DAG de ingesta una vez que el archivo se encuentra en GCS. Una vez que los datos se cargan en BigQuery, el DAG dispara automáticamente el DAG de transformación, creando un flujo automatizado que continúa con el procesamiento de los datos. El uso de "TriggerDagRunOperator" con "wait_for_completion=False" permite que el DAG actual se marque como exitoso inmediatamente después de disparar el siguiente DAG.

## 12. Notas Importantes

Es importante asegurarse de que exista al menos un archivo Excel en la carpeta configurada de GCS antes de ejecutar el DAG, ya que la función "get_latest_excel_from_gcs_folder" fallará si no encuentra ningún archivo. El DAG está diseñado para procesar solo la primera hoja del archivo Excel, por lo que si el archivo contiene múltiples hojas y se requiere procesar otras, será necesario modificar el código. Los archivos temporales se eliminan automáticamente al finalizar el proceso, pero si el DAG falla antes de llegar a la tarea de limpieza, estos archivos pueden quedar en el sistema. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde GCS y escribir en BigQuery.
