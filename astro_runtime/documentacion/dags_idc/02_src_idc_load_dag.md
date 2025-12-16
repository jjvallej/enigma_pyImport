# Documentación: dags_idc/src_idc_load_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_load_idc" que se encarga de extraer datos del archivo Excel IDC desde Google Cloud Storage, transformarlos mínimamente y cargarlos en la capa bronze de BigQuery. El DAG está diseñado para buscar automáticamente el archivo Excel más reciente en la carpeta configurada de GCS, procesarlo y cargar todas sus hojas como tablas separadas en BigQuery.

El propósito principal de este DAG es automatizar la carga de datos desde GCS hacia BigQuery, procesando un archivo Excel que siempre contiene 4 hojas. La primera hoja se elimina automáticamente ya que contiene estructura o metadatos, y las 3 hojas restantes se cargan como tablas separadas en BigQuery. Cada hoja se procesa eliminando la primera fila que contiene metadatos y usando la segunda fila como encabezados.

## 2. Estructura del DAG

El DAG está compuesto por un grupo de tareas llamado "bronze" que contiene todas las operaciones relacionadas con la carga de datos a la capa bronze. Este grupo incluye cuatro tareas principales. La primera tarea es "ensure_dataset" que asegura que el dataset de bronze exista en BigQuery. La segunda tarea es "download_excel" que descarga el archivo Excel más reciente desde GCS. La tercera tarea es "load_all_sheets" que procesa todas las hojas del Excel y las carga como tablas separadas en BigQuery. La cuarta tarea es "cleanup_temp_files" que elimina los archivos temporales creados durante el proceso. Después del grupo "bronze", el DAG incluye un "TriggerDagRunOperator" llamado "trigger_transf_idc" que dispara el DAG de transformación "src_planeacion_transf_idc" sin esperar a que termine. Finalmente, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "GCS_BUCKET_NAME" obtiene su valor de "DEFAULT_BUCKET_NAME" importado desde "modules.config", que representa el nombre del bucket de GCS. La variable "GCS_FOLDER_PATH" obtiene su valor de "CONF.idc.gcs_folder", que define la carpeta en GCS donde se buscan los archivos Excel. La variable "DATASET_ID_BRONZE" se importa directamente desde "modules.config" y representa el identificador del dataset de bronze en BigQuery. La variable "SHEET_TO_TABLE_MAPPING" se construye a partir de "CONF.idc.tables.raw_data_mapping" y define el mapeo entre los índices de las hojas del Excel y los nombres de las tablas en BigQuery.

## 4. Procesamiento de Hojas del Excel

El archivo Excel del IDC siempre tiene 4 hojas. La primera hoja con índice 0 contiene estructura o metadatos y se elimina automáticamente durante el procesamiento. Las 3 hojas restantes se procesan de la siguiente manera. La primera hoja restante con índice 0 se mapea a la tabla "idc_raw_data_dato_original". La segunda hoja restante con índice 1 se mapea a la tabla "idc_raw_data_valor_normalizado". La tercera hoja restante con índice 2 se mapea a la tabla "idc_raw_data_valor_ranking". En cada hoja, se elimina automáticamente la primera fila que contiene metadatos o estructura, y la segunda fila se usa como encabezados para los nombres de las columnas.

## 5. Función _ensure_dataset_bronze_task

La función "_ensure_dataset_bronze_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.idc.idc_load" pasando el identificador del dataset de bronze. Esta función asegura que el dataset exista en BigQuery antes de intentar cargar datos en él, creándolo si no existe.

## 6. Función _download_excel_task

La función "_download_excel_task" es responsable de buscar y descargar el archivo Excel más reciente desde GCS. La función intenta primero obtener la configuración desde Variables de Airflow usando "Variable.get", pero si estas no existen, utiliza los valores por defecto definidos en el código. La función utiliza "get_latest_excel_from_gcs_folder" para encontrar el archivo Excel más reciente en la carpeta especificada, y luego utiliza "download_excel_from_gcs" para descargarlo a un archivo temporal local. La función retorna la ruta del archivo descargado, que se almacena en XCom para ser utilizada por las tareas siguientes.

## 7. Función _load_all_sheets_task

La función "_load_all_sheets_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la ruta del archivo Excel descargado por la tarea anterior. La función utiliza "load_all_sheets_to_bq" del módulo "modules.idc.idc_load" para procesar todas las hojas del Excel y cargarlas como tablas separadas en BigQuery. La función pasa el mapeo de hojas a tablas definido en "SHEET_TO_TABLE_MAPPING", que especifica cómo se mapean los índices de las hojas a los nombres de las tablas. La función retorna un diccionario con los resultados del proceso, indicando qué hojas se cargaron en qué tablas.

## 8. Función _cleanup_temp_files_task

La función "_cleanup_temp_files_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la ruta del archivo Excel creado durante el proceso. La función utiliza "cleanup_temp_paths" del módulo "modules.idc.idc_load" para eliminar el archivo temporal, liberando espacio en el sistema de archivos. Esta tarea está configurada con "TriggerRule.ALL_DONE", lo que significa que se ejecutará independientemente del resultado de las tareas anteriores, asegurando que los archivos temporales siempre se eliminen.

## 9. Dependencias entre Tareas

Dentro del grupo "bronze", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "download_excel", que a su vez precede a "load_all_sheets", y finalmente "load_all_sheets" precede a "cleanup_temp_files". La tarea "cleanup_temp_files" tiene la regla de activación "ALL_DONE", lo que garantiza que se ejecute incluso si alguna tarea anterior falla. Después del grupo "bronze", el flujo continúa con "trigger_transf_dag" y luego "end". La tarea "start" precede a todo el grupo "bronze".

## 10. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_load_idc" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:carga", "fuente:idc" y "ejecución:manual".

## 11. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos del IDC, siendo disparado automáticamente por el DAG de ingesta una vez que el archivo se encuentra en GCS. Una vez que los datos se cargan en BigQuery como tres tablas separadas, el DAG dispara automáticamente el DAG de transformación, creando un flujo automatizado que continúa con el procesamiento de los datos. El uso de "TriggerDagRunOperator" con "wait_for_completion=False" permite que el DAG actual se marque como exitoso inmediatamente después de disparar el siguiente DAG.

## 12. Notas Importantes

Es importante asegurarse de que exista al menos un archivo Excel en la carpeta configurada de GCS antes de ejecutar el DAG, ya que la función "get_latest_excel_from_gcs_folder" fallará si no encuentra ningún archivo. El DAG está diseñado para procesar un archivo Excel con exactamente 4 hojas, donde la primera hoja se elimina automáticamente y las 3 hojas restantes se cargan como tablas separadas. Si el archivo tiene una estructura diferente, el proceso puede fallar o producir resultados inesperados. El mapeo de hojas a tablas está definido en "CONF.idc.tables.raw_data_mapping" y debe coincidir con la estructura real del archivo Excel. Los archivos temporales se eliminan automáticamente al finalizar el proceso, pero si el DAG falla antes de llegar a la tarea de limpieza, estos archivos pueden quedar en el sistema. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde GCS y escribir en BigQuery.

