# Documentación: dags_idc/src_idc_dictionary_ingest_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_ingest_idc_dictionary" que se encarga de ingerir el archivo CSV del diccionario IDC desde Google Drive hacia Google Cloud Storage. El DAG está diseñado para trabajar con enlaces públicos de Google Drive, lo que significa que no requiere autenticación adicional para acceder a los archivos.

El propósito principal de este DAG es automatizar la transferencia del archivo CSV del diccionario desde Google Drive a GCS, específicamente a la carpeta "idc" dentro de la ruta "data_staging/dpt_planeacion_municipal/". Una vez completada la transferencia, el DAG dispara automáticamente el DAG de carga "src_planeacion_load_idc_dictionary" sin esperar a que este termine su ejecución.

## 2. Estructura del DAG

El DAG está compuesto por cuatro tareas principales conectadas en secuencia. La primera tarea es un "EmptyOperator" con el identificador "start" que marca el inicio del flujo. La segunda tarea es un "PythonOperator" llamado "upload_file_from_drive_to_gcs" que ejecuta la función "_upload_file_task" para realizar la transferencia del archivo CSV. La tercera tarea es un "TriggerDagRunOperator" con el identificador "trigger_load_idc_dictionary" que dispara el DAG de carga "src_planeacion_load_idc_dictionary" con el parámetro "wait_for_completion" establecido en "False", lo que permite que el DAG actual termine exitosamente sin esperar la finalización del DAG disparado. La cuarta y última tarea es otro "EmptyOperator" con el identificador "end" que marca el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "DEFAULT_FOLDER_NAME" obtiene su valor de "CONF.idc.gcs_folder", que define la carpeta base en GCS donde se almacenará el archivo. La variable "DRIVE_URL" obtiene su valor de "CONF.idc.dictionary_drive_url", que contiene la URL pública del archivo CSV del diccionario en Google Drive. La variable "DEFAULT_BUCKET_NAME" se importa directamente desde "modules.config" y representa el nombre del bucket de GCS donde se almacenará el archivo.

## 4. Función _upload_file_task

La función "_upload_file_task" es la encargada de ejecutar la transferencia del archivo CSV desde Google Drive a GCS. Esta función utiliza la función "move_file_from_drive_to_gcs" del módulo "modules.idc.idc_dictionary_ingest" para realizar la operación. La función construye la ruta de destino usando "DEFAULT_FOLDER_NAME", resultando en una ruta como "data_staging/dpt_planeacion_municipal/idc". El parámetro "destination_file_name" se establece en "None", lo que permite que el nombre del archivo se extraiga automáticamente del archivo original en Google Drive. La función imprime información de depuración sobre la operación, incluyendo la URL de Drive, el bucket destino, la carpeta destino y el método utilizado. Al finalizar, retorna el URI de GCS donde se almacenó el archivo.

## 5. Dependencias entre Tareas

Las tareas están conectadas en una secuencia lineal donde "start" precede a "upload_file_from_drive_to_gcs", que a su vez precede a "trigger_load_idc_dictionary", y finalmente "trigger_load_idc_dictionary" precede a "end". Esta estructura garantiza que cada tarea se ejecute en el orden correcto, asegurando que la transferencia del archivo se complete antes de disparar el DAG de carga.

## 6. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_ingest_idc_dictionary" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente y no tiene un horario programado. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:ingesta", "fuente:idc_dictionary" y "ejecución:manual" para facilitar su identificación y filtrado en la interfaz de Airflow.

## 7. Uso en el Código

Este DAG se utiliza como punto de entrada para el proceso de ingesta del diccionario IDC. Una vez que el archivo CSV se encuentra en GCS, el DAG dispara automáticamente el DAG de carga, creando un flujo automatizado que continúa con el procesamiento de los datos sin intervención manual adicional. El uso de "TriggerDagRunOperator" con "wait_for_completion=False" permite que el DAG actual se marque como exitoso inmediatamente después de disparar el siguiente DAG, sin esperar a que este termine, lo que mejora la eficiencia del proceso general.

## 8. Notas Importantes

Es importante asegurarse de que la URL de Google Drive configurada en "CONF.idc.dictionary_drive_url" sea un enlace público y que el archivo esté accesible sin autenticación. Si el archivo requiere permisos especiales, el proceso fallará. La carpeta destino en GCS se crea automáticamente si no existe, pero es necesario que el bucket especificado en "DEFAULT_BUCKET_NAME" exista y que las credenciales de GCP tengan los permisos necesarios para escribir en él. El DAG está diseñado para trabajar con archivos CSV, por lo que es importante verificar que el archivo en Google Drive sea del formato correcto antes de ejecutar el DAG. El archivo CSV del diccionario debe tener una estructura específica con columnas ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, NOM_FACTOR, NOM_PILAR, NOM_INDICADOR y NOM_SUBINDICADOR, por lo que es importante verificar que el archivo tenga el formato esperado antes de ejecutar este DAG. Este diccionario es esencial para el proceso de transformación del IDC, ya que se une con las tablas transformadas para crear la tabla final "fact_idc" en la capa gold.

