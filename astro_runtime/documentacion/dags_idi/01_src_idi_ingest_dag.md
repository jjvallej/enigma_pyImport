# Documentación: dags_idi/src_idi_ingest_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_ingest_idi" que se encarga de ingerir archivos Excel del IDI desde la página web de Función Pública hacia Google Cloud Storage. El DAG está diseñado para leer una configuración desde un archivo Excel en Google Drive que contiene información sobre los años y las URLs de descarga, y luego procesar automáticamente todos los años encontrados.

El propósito principal de este DAG es automatizar la transferencia de archivos Excel del IDI desde la web hacia GCS, procesando todos los años disponibles de manera automática. El proceso incluye web scraping para encontrar los enlaces de descarga, descarga de los archivos Excel, transformación de los datos eliminando filas iniciales y columnas específicas, y finalmente carga de los archivos transformados como CSV en GCS organizados por año.

## 2. Estructura del DAG

El DAG está compuesto por cuatro tareas principales conectadas en secuencia. La primera tarea es un "EmptyOperator" con el identificador "start" que marca el inicio del flujo. La segunda tarea es un "PythonOperator" llamado "ingest_idi_all_years" que ejecuta la función "_ingest_idi" para realizar la ingesta de todos los años. La tercera tarea es un "TriggerDagRunOperator" con el identificador "trigger_load_dag" que dispara el DAG de carga "src_planeacion_load_idi" con el parámetro "wait_for_completion" establecido en "False", lo que permite que el DAG actual termine exitosamente sin esperar la finalización del DAG disparado. La cuarta y última tarea es otro "EmptyOperator" con el identificador "end" que marca el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "CONFIG_DRIVE_URL" obtiene su valor de "CONF.idi.config_drive_url", que contiene la URL pública del archivo Excel de configuración en Google Drive. La variable "LINK_NAME_KEYWORDS" obtiene su valor de "CONF.idi.link_name_keywords", que es una lista de palabras clave utilizadas para identificar los enlaces de descarga en las páginas web. La variable "BUCKET_NAME" obtiene su valor de "DEFAULT_BUCKET_NAME" importado desde "modules.config", que representa el nombre del bucket de GCS. La variable "BASE_FOLDER_NAME" obtiene su valor de "CONF.idi.gcs_base_folder", que define la carpeta base en GCS donde se almacenarán los archivos organizados por año. La variable "SKIP_ROWS" obtiene su valor de "CONF.idi.transform_config.skip_rows", que indica cuántas filas se deben omitir al inicio de cada archivo Excel. La variable "SHEET_NAME" obtiene su valor de "CONF.idi.transform_config.sheet_name" si está disponible, o "None" si no está definida, indicando qué hoja del Excel procesar. La variable "COLUMNS_TO_DROP" obtiene su valor de "CONF.idi.transform_config.columns_to_drop" si está disponible, o "None" si no está definida, indicando qué columnas eliminar durante la transformación.

## 4. Función _ingest_idi

La función "_ingest_idi" es la encargada de ejecutar la ingesta de todos los años del IDI. Esta función utiliza la función "ingest_all_years_idi" del módulo "modules.idi.idi_ingest" para realizar la operación completa. La función pasa todos los parámetros de configuración necesarios, incluyendo la URL del archivo de configuración, las palabras clave para buscar enlaces, el bucket y carpeta de destino, y los parámetros de transformación. La función imprime información de depuración sobre el proceso, incluyendo las palabras clave de búsqueda y el destino base en GCS. Al finalizar, la función calcula un resumen con el total de años procesados, cuántos fueron exitosos y cuántos tuvieron errores, y retorna una lista de resultados con el estado de cada año procesado.

## 5. Proceso de Ingesta

El proceso de ingesta ejecutado por "ingest_all_years_idi" incluye varios pasos. Primero, descarga el archivo Excel de configuración desde Google Drive usando la URL configurada. Luego, lee el archivo de configuración para obtener información sobre los años y las URLs de las páginas web donde se encuentran los archivos. Para cada año configurado, realiza web scraping en la URL correspondiente para encontrar el enlace de descarga que coincida con las palabras clave especificadas. Una vez encontrado el enlace, descarga el archivo Excel desde la web. Luego transforma el archivo eliminando las filas iniciales especificadas en "SKIP_ROWS", seleccionando la hoja especificada en "SHEET_NAME" si está definida, y eliminando las columnas especificadas en "COLUMNS_TO_DROP" si están definidas. Finalmente, normaliza los nombres de columnas y guarda el resultado como CSV en GCS en una carpeta específica para cada año, con el formato "resultados_{año}.csv".

## 6. Dependencias entre Tareas

Las tareas están conectadas en una secuencia lineal donde "start" precede a "ingest_idi_all_years", que a su vez precede a "trigger_load_dag", y finalmente "trigger_load_dag" precede a "end". Esta estructura garantiza que cada tarea se ejecute en el orden correcto, asegurando que la ingesta de todos los años se complete antes de disparar el DAG de carga.

## 7. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_ingest_idi" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente y no tiene un horario programado. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:carga", "fuente:idi" y "ejecución:manual" para facilitar su identificación y filtrado en la interfaz de Airflow. El "TriggerDagRunOperator" tiene el parámetro "reset_dag_run" establecido en "True", lo que permite re-ejecutar el DAG de carga incluso si ya está en ejecución.

## 8. Uso en el Código

Este DAG se utiliza como punto de entrada para el proceso de ingesta de datos del IDI. Una vez que todos los archivos se encuentran en GCS organizados por año, el DAG dispara automáticamente el DAG de carga, creando un flujo automatizado que continúa con el procesamiento de los datos sin intervención manual adicional. El uso de "TriggerDagRunOperator" con "wait_for_completion=False" permite que el DAG actual se marque como exitoso inmediatamente después de disparar el siguiente DAG, sin esperar a que este termine, lo que mejora la eficiencia del proceso general.

## 9. Notas Importantes

Es importante asegurarse de que la URL del archivo de configuración en Google Drive configurada en "CONF.idi.config_drive_url" sea un enlace público y que el archivo esté accesible sin autenticación. El archivo de configuración debe tener una estructura específica que indique los años y las URLs de las páginas web donde se encuentran los archivos. Las palabras clave configuradas en "CONF.idi.link_name_keywords" deben ser lo suficientemente específicas para identificar correctamente los enlaces de descarga en las páginas web, pero no tan restrictivas que impidan encontrar los enlaces. El proceso de web scraping depende de la estructura de las páginas web, por lo que si la estructura cambia, puede ser necesario actualizar las palabras clave o el código de scraping. La carpeta destino en GCS se crea automáticamente si no existe, pero es necesario que el bucket especificado en "BUCKET_NAME" exista y que las credenciales de GCP tengan los permisos necesarios para escribir en él. El proceso transforma los archivos Excel a CSV antes de guardarlos en GCS, por lo que es importante verificar que las transformaciones configuradas sean apropiadas para los datos. Si algún año falla durante el proceso, el DAG continuará procesando los años restantes y reportará un resumen al final con los éxitos y errores.

