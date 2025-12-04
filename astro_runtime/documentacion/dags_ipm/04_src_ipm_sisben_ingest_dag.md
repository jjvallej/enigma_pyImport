# Documentación: dags_ipm/src_ipm_sisben_ingest_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_ingest_ipm_sisben" que se encarga de mover archivos Excel desde una carpeta temporal a la carpeta destino en Google Cloud Storage para el procesamiento de datos de IPM SISBEN. El DAG está diseñado para trabajar con archivos que ya se encuentran en una carpeta temporal dentro del bucket de GCS, moviéndolos a la ubicación final donde serán procesados.

El propósito principal de este DAG es automatizar la organización de archivos dentro de GCS, moviendo los archivos desde la carpeta temporal hacia la carpeta destino configurada para IPM SISBEN. Este proceso es útil cuando los archivos se cargan manualmente o mediante otro proceso a la carpeta temporal y luego necesitan ser movidos a su ubicación final para el procesamiento.

## 2. Estructura del DAG

El DAG está compuesto por tres tareas principales conectadas en secuencia. La primera tarea es un "EmptyOperator" con el identificador "start" que marca el inicio del flujo. La segunda tarea es un "PythonOperator" llamado "move_file_within_gcs" que ejecuta la función "_move_file_task" para realizar el movimiento del archivo dentro de GCS. La tercera y última tarea es otro "EmptyOperator" con el identificador "end" que marca el final del flujo.

## 3. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "SOURCE_FOLDER" obtiene su valor de "CONF.ipm_sisben.gcs_temp_folder", que define la carpeta temporal en GCS donde se encuentran los archivos a mover. La variable "DESTINATION_FOLDER" obtiene su valor de "CONF.ipm_sisben.gcs_folder", que define la carpeta destino en GCS donde se moverán los archivos. La variable "DEFAULT_BUCKET_NAME" se importa directamente desde "modules.config" y representa el nombre del bucket de GCS donde se realizará la operación.

## 4. Función _move_file_task

La función "_move_file_task" es la encargada de ejecutar el movimiento del archivo desde la carpeta temporal a la carpeta destino dentro de GCS. La función utiliza la función "move_file_within_gcs" del módulo "modules.ipm.ipm_sisben_ingest" para realizar la operación. La función construye los parámetros necesarios usando "DEFAULT_BUCKET_NAME" para el bucket, "SOURCE_FOLDER" para la carpeta origen y "DESTINATION_FOLDER" para la carpeta destino. El parámetro "file_pattern" se establece en "*.xlsx" para indicar que solo se moverán archivos con extensión Excel. La función imprime información de depuración sobre la operación, incluyendo el bucket, la carpeta origen y la carpeta destino. Al finalizar, retorna el URI de GCS donde se movió el archivo.

## 5. Dependencias entre Tareas

Las tareas están conectadas en una secuencia lineal donde "start" precede a "move_file_within_gcs", que a su vez precede a "end". Esta estructura garantiza que cada tarea se ejecute en el orden correcto, asegurando que el movimiento del archivo se complete antes de finalizar el DAG.

## 6. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_ingest_ipm_sisben" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente y no tiene un horario programado. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:ingesta", "fuente:ipm_sisben" y "ejecución:manual" para facilitar su identificación y filtrado en la interfaz de Airflow.

## 7. Uso en el Código

Este DAG se utiliza como punto de entrada para el proceso de ingesta de datos de IPM SISBEN cuando los archivos ya se encuentran en una carpeta temporal dentro de GCS. El DAG mueve los archivos a su ubicación final, preparándolos para el procesamiento posterior. A diferencia del DAG de ingesta de IPM regular, este DAG no dispara automáticamente el DAG de carga, por lo que el proceso de carga debe iniciarse manualmente o mediante otro mecanismo.

## 8. Notas Importantes

Es importante asegurarse de que exista al menos un archivo Excel en la carpeta temporal configurada en "CONF.ipm_sisben.gcs_temp_folder" antes de ejecutar el DAG, ya que la función "move_file_within_gcs" buscará archivos que coincidan con el patrón "*.xlsx". Si no se encuentra ningún archivo, el DAG fallará. La carpeta destino debe estar correctamente configurada en "CONF.ipm_sisben.gcs_folder" y debe ser una ruta válida dentro del bucket de GCS. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde la carpeta temporal y escribir en la carpeta destino dentro del bucket de GCS. El DAG está diseñado para trabajar con archivos Excel, por lo que es importante verificar que los archivos en la carpeta temporal sean del formato correcto antes de ejecutar el DAG. Si hay múltiples archivos Excel en la carpeta temporal, el DAG moverá todos los que coincidan con el patrón, por lo que es importante asegurarse de que solo haya un archivo o que todos los archivos sean válidos para el procesamiento.
