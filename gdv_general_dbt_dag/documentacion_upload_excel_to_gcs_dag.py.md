# DOCUMENTACIÓN DETALLADA DEL DAG upload_excel_to_gcs_dag.py

Este documento describe de forma detallada y continua el DAG de Airflow `scr_planeacion_inges_ipm` definido en el archivo `upload_excel_to_gcs_dag.py`. El DAG automatiza la transferencia de un archivo Excel del IPM desde Google Drive hacia Google Cloud Storage GCS utilizando el método más simple: enlace público de Google Drive, sin autenticación adicional.

## Propósito general

Facilitar la ingesta de archivos fuente del IPM alojados en Google Drive, moviéndolos a un bucket de GCS bajo una carpeta estándar. Este movimiento habilita el procesamiento posterior por otros DAGs, como el de transformación a bronze y capas superiores en BigQuery.

## Configuración principal

Constantes por defecto dentro del DAG:
- `DEFAULT_BUCKET_NAME`: bucket de destino en GCS. Por defecto `datalake_gdv`.
- `DEFAULT_FOLDER_NAME`: carpeta destino dentro del bucket. Por defecto `data_staging/dpt_planeacion_municipal/ipm`.

Parámetros de ejecución del DAG:
- `params.drive_url` (obligatorio): URL pública de Google Drive o File ID del archivo a transferir. Si no se provee, la ejecución falla con un mensaje indicando el parámetro requerido.

El DAG no utiliza Variables de Airflow, ya que asume el caso de uso más simple. Cualquier ajuste de bucket o carpeta se realiza modificando las constantes anteriores.

## Estructura del DAG

El DAG `scr_planeacion_inges_ipm` se ejecuta manualmente (`schedule_interval=None`) y no hace catchup. Consta de tres tareas secuenciales:

1. `start` (`EmptyOperator`): marcador de inicio.
2. `upload_file_from_drive_to_gcs` (`PythonOperator`): tarea principal que descarga desde Drive y sube a GCS.
3. `end` (`EmptyOperator`): marcador de fin.

Dependencias: `start -> upload_file_from_drive_to_gcs -> end`.

## Lógica de la tarea principal

La función `_upload_file_task` implementa la lógica de negocio:

- Lee `drive_url_or_id` desde `context['params']['drive_url']`. Si está vacío, levanta un `ValueError` explicando cómo pasar el parámetro.
- Define parámetros operativos con valores por defecto:
  - `bucket_name = DEFAULT_BUCKET_NAME`
  - `folder_name = DEFAULT_FOLDER_NAME`
  - `use_public_link = True` para descargar usando enlace público
  - `destination_file_name = None` para usar el nombre original del archivo de Drive
- Emite logs informativos con el origen y el destino configurados.
- Llama a `move_file_from_drive_to_gcs` del módulo `modules.upload_excel_to_gcs`, que:
  - Descarga el archivo desde Google Drive (enlace público) a un archivo temporal local, preservando el nombre original cuando es posible.
  - Construye la ruta de destino en GCS como `folder_name/destination_file_name`.
  - Sube el archivo al bucket indicado y devuelve la URI final `gs://...`.
- Devuelve la URI de GCS como resultado de la tarea y la registra en logs.

## Interacción con el módulo de utilidades

`modules.upload_excel_to_gcs.move_file_from_drive_to_gcs` encapsula los detalles técnicos:
- Soporta distintos formatos de URL o IDs de Drive y extrae el File ID.
- Maneja la advertencia de descarga de archivos grandes de Google Drive y descarga por chunks.
- Determina el nombre original del archivo y asegura una extensión válida `.xlsx`.
- Sube el archivo a GCS, creando la “carpeta” de destino si no existe y sobrescribiendo si es necesario.
- Elimina siempre el archivo temporal local al finalizar, incluso ante errores.

## Manejo de errores

- La validación del parámetro `drive_url` evita ejecuciones inválidas. Si falta, la tarea falla explícitamente con un mensaje guía.
- Cualquier error en la descarga desde Drive o la subida a GCS propagará una excepción que marcará la tarea como `FAILED`.

## Idempotencia y convenciones

- La subida a GCS sobrescribe un archivo existente con el mismo nombre dentro de la carpeta por defecto. Este comportamiento es consistente con el módulo de utilidades y simplifica reintentos.
- El DAG estandariza la ubicación de archivos fuente en `data_staging/dpt_planeacion_municipal/ipm`, facilitando su descubrimiento por los siguientes procesos.

## Ejecución del DAG

1. Abrir el DAG `scr_planeacion_inges_ipm` en la UI de Airflow.
2. Hacer clic en `Trigger DAG` o `Run`, y pasar el parámetro `drive_url` en `Conf` o `Params` con alguno de estos formatos:
   - URL pública: `https://drive.google.com/file/d/FILE_ID/view?usp=sharing`
   - ID directo: `FILE_ID`
3. Verificar en los logs la URI final de GCS retornada por la tarea principal, por ejemplo: `gs://datalake_gdv/data_staging/dpt_planeacion_municipal/ipm/archivo.xlsx`.

## Requisitos previos

- La cuenta de servicio configurada en el entorno de Airflow debe tener permisos de escritura en el bucket de GCS `DEFAULT_BUCKET_NAME`.
- El archivo en Google Drive debe ser accesible públicamente mediante enlace. Si no es público, este DAG no aplica; en su lugar debe usarse el flujo con Service Account en el módulo, extendiendo el DAG para `use_public_link=False`.

## Resumen

El DAG mueve un archivo Excel del IPM desde un enlace público de Google Drive a una ubicación estandarizada en GCS, listo para que otros procesos como `scr_planeacion_transf_ipm_manual` lo consuman. Es simple, manual, y centrado en parametrizar únicamente la URL del archivo de origen.
