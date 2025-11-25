DOCUMENTACION DETALLADA DEL DAG src_ipm_extract_dag.py

Este documento describe de forma detallada y continua el DAG de Airflow src_planeacion_extrac_ipm definido en el archivo src_ipm_extract_dag.py. El DAG automatiza la transferencia del archivo Excel del IPM desde Google Drive hacia Google Cloud Storage GCS utilizando el metodo mas simple: enlace publico de Google Drive, sin autenticacion adicional. La URL del archivo esta hardcodeada en el codigo, por lo que no requiere parametros de ejecucion.

PROPOSITO GENERAL

Facilitar la extraccion (descarga) del archivo fuente del IPM alojado en Google Drive, moviendolo a un bucket de GCS bajo una carpeta estandar. Este movimiento habilita el procesamiento posterior por otros DAGs, como el de ingesta a bronze y capas superiores en BigQuery. El DAG esta disenado para trabajar con un archivo especifico cuya URL esta definida directamente en el codigo como constante.

CONFIGURACION PRINCIPAL

El archivo comienza importando las librerias necesarias de Airflow y el modulo local ipm_extract que contiene las funciones de utilidad para descargar desde Google Drive y subir a GCS.

Constantes dentro del DAG:
- DEFAULT_BUCKET_NAME: bucket de destino en GCS. Por defecto "datalake_gdv_dev". Esta constante define donde se almacenara el archivo descargado.
- DEFAULT_FOLDER_NAME: carpeta destino dentro del bucket. Por defecto "data_staging/dpt_planeacion_municipal/ipm". Esta ruta estandariza la ubicacion de los archivos fuente del IPM.
- DRIVE_URL: URL fija del archivo IPM en Google Drive. Esta URL esta hardcodeada en el codigo y apunta al archivo especifico del IPM que se procesa. La URL completa es: https://docs.google.com/spreadsheets/d/1uXHTK64SVXmsV-nKGXT8Vz7u_b4gXvYs/edit?usp=drive_link&ouid=109263228047844968910&rtpof=true&sd=true

El DAG no requiere parametros de ejecucion ni utiliza Variables de Airflow. La URL del archivo esta definida directamente en el codigo como constante DRIVE_URL. Cualquier ajuste de bucket, carpeta o URL se realiza modificando las constantes en el codigo fuente. Esto simplifica la ejecucion ya que no es necesario proporcionar parametros al ejecutar el DAG.

ESTRUCTURA DEL DAG

El DAG src_planeacion_extrac_ipm se define con las siguientes caracteristicas:
- dag_id: "src_planeacion_extrac_ipm" identificador unico del DAG
- start_date: Fecha de inicio, 1 de enero de 2024
- schedule_interval: None, lo que significa que el DAG no se ejecuta automaticamente, solo manualmente
- catchup: False, no ejecuta ejecuciones pasadas
- tags: Etiquetas para categorizar el DAG: secretaria:planeacion, actividad:extraccion, fuente:ipm, ejecucion:manual
- description: Descripcion detallada del proposito del DAG que incluye la descarga desde Google Drive, subida a GCS y ejecucion del DAG de ingesta

El DAG consta de cuatro tareas secuenciales:

1. start (EmptyOperator): Marcador de inicio que no realiza ninguna accion. Sirve como punto de entrada del flujo.

2. upload_file_from_drive_to_gcs (PythonOperator): Tarea principal que ejecuta la funcion _upload_file_task. Esta tarea descarga el archivo desde Google Drive y lo sube a GCS. No requiere contexto adicional ya que no lee parametros, por lo que no se configura provide_context=True.

3. trigger_load_ipm (TriggerDagRunOperator): Tarea que ejecuta el DAG de carga src_planeacion_load_ipm. Esta tarea espera a que el DAG de carga termine completamente antes de continuar (wait_for_completion=True). Esto asegura que todo el proceso de carga y transformacion se complete antes de finalizar el DAG principal.

4. end (EmptyOperator): Marcador de fin que no realiza ninguna accion. Sirve como punto de salida del flujo.

Dependencias: start ejecuta primero, luego upload_file_from_drive_to_gcs, despues trigger_load_ipm, y finalmente end. La secuencia es simple y lineal sin paralelismo. El DAG de carga se ejecuta como parte del flujo principal y el DAG principal espera su finalizacion antes de terminar. A su vez, el DAG de carga ejecuta automaticamente el DAG de transformacion src_planeacion_transf_ipm al finalizar exitosamente.

LOGICA DE LAS TAREAS

La funcion _upload_file_task implementa la logica de descarga y subida del archivo. Esta funcion no recibe parametros porque toda la configuracion esta definida como constantes en el codigo.

El proceso funciona de la siguiente manera:

Primero, la funcion asigna los valores de configuracion desde las constantes definidas en el archivo. bucket_name se establece en DEFAULT_BUCKET_NAME, folder_name se establece en DEFAULT_FOLDER_NAME, y destination_file_name se establece en None para que el modulo extraiga automaticamente el nombre original del archivo desde Google Drive.

Segundo, emite mensajes informativos usando print que se registraran en los logs de Airflow. Estos mensajes incluyen: un mensaje de inicio de transferencia, la URL de Drive que se esta utilizando (la constante DRIVE_URL), el bucket destino configurado, la carpeta destino configurada, el metodo de descarga que se usara (enlace publico), y una nota de que el nombre del archivo se extraera automaticamente.

Tercero, llama a la funcion move_file_from_drive_to_gcs del modulo modules.ipm_extract pasando todos los parametros configurados. Esta funcion encapsula todo el proceso de descarga desde Google Drive y subida a GCS.

Cuarto, una vez que move_file_from_drive_to_gcs completa exitosamente, imprime un mensaje de confirmacion con la URI completa del archivo en GCS en formato gs://bucket/ruta/archivo.xlsx.

Finalmente, retorna la URI de GCS como resultado de la tarea. Aunque esta URI no se usa por otras tareas en este DAG, se puede consultar en los logs o en la interfaz de Airflow para verificar donde se almaceno el archivo.

La tarea trigger_load_ipm utiliza el operador TriggerDagRunOperator de Airflow para ejecutar el DAG src_planeacion_load_ipm. Esta tarea tiene configurado wait_for_completion=True, lo que significa que el DAG principal esperara a que el DAG de carga termine completamente (ya sea exitosamente o con error) antes de continuar. A su vez, el DAG de carga ejecuta automaticamente el DAG de transformacion src_planeacion_transf_ipm al finalizar exitosamente. Si alguno de los DAGs falla, la tarea trigger_load_ipm tambien fallara, lo que causara que el DAG principal falle. Esto asegura que el flujo completo se ejecute de manera atomica: o todo el proceso (extraccion, carga y transformacion) se completa exitosamente, o el DAG principal falla indicando que hubo un problema en algun punto del pipeline.

INTERACCION CON EL MODULO DE UTILIDADES

La funcion move_file_from_drive_to_gcs del modulo modules.ipm_extract encapsula todos los detalles tecnicos de la transferencia. Esta funcion es la que realmente realiza el trabajo pesado.

El proceso que realiza esta funcion es el siguiente: primero extrae el File ID de la URL proporcionada usando la funcion extract_file_id_from_url, que puede manejar diferentes formatos de URLs de Google Drive. Segundo, descarga el archivo desde Google Drive usando download_file_from_public_link, que maneja casos especiales como archivos grandes que muestran una pagina de advertencia antes de la descarga real. La descarga se hace en chunks para manejar archivos grandes eficientemente. Tercero, determina el nombre original del archivo desde los metadatos de Google Drive o desde los headers HTTP, y asegura que tenga una extension valida .xlsx. Cuarto, sube el archivo a GCS usando upload_file_to_gcs, que crea automaticamente la estructura de carpetas si no existe y puede sobrescribir archivos existentes si es necesario. Quinto, elimina siempre el archivo temporal local al finalizar, incluso si ocurre un error durante el proceso, garantizando que no queden archivos temporales acumulandose en el sistema.

Esta separacion de responsabilidades permite que el DAG sea simple y claro, mientras que la complejidad tecnica se maneja en el modulo de utilidades.

MANEJO DE ERRORES

Como la URL esta hardcodeada en el codigo como constante DRIVE_URL, no hay validacion de parametros requerida. La URL siempre esta disponible cuando se ejecuta la funcion, por lo que no hay riesgo de que falte un parametro.

Sin embargo, pueden ocurrir otros tipos de errores durante la ejecucion:

Si el archivo en Google Drive no es publico o la URL es invalida, la funcion download_file_from_public_link del modulo lanzara una excepcion HTTPError o ValueError. Esta excepcion se propagara hasta la tarea de Airflow, que marcara la tarea como FAILED.

Si hay un error al acceder al bucket de GCS o al subir el archivo, la funcion upload_file_to_gcs lanzara una excepcion que tambien marcara la tarea como FAILED.

En todos los casos, el modulo de utilidades garantiza que los archivos temporales se eliminen incluso si ocurre un error, por lo que no habra acumulacion de archivos temporales en el sistema.

Los logs de Airflow mostraran el mensaje de error especifico, lo que facilita la depuracion. Por ejemplo, si el archivo no es publico, se vera un error HTTP 403 o un mensaje indicando que no se pudo acceder al archivo.

IDEMPOTENCIA Y CONVENCIONES

El DAG tiene caracteristicas de idempotencia y sigue convenciones estandar:

La subida a GCS sobrescribe un archivo existente con el mismo nombre dentro de la carpeta por defecto. Esto significa que si se ejecuta el DAG multiples veces, siempre reemplazara el archivo anterior con el nuevo. Este comportamiento es consistente con el modulo de utilidades que usa overwrite=True por defecto, y simplifica los reintentos ya que no es necesario eliminar manualmente archivos anteriores.

El DAG estandariza la ubicacion de archivos fuente en data_staging/dpt_planeacion_municipal/ipm. Esta ruta es conocida por otros procesos, especialmente por el DAG src_planeacion_load_ipm que busca automaticamente el archivo mas reciente en esta carpeta. Esta convencion facilita la integracion entre DAGs y asegura que los archivos se encuentren donde se esperan.

La URL del archivo esta hardcodeada, lo que significa que siempre descargara el mismo archivo desde la misma ubicacion en Google Drive. Esto es apropiado cuando hay un archivo fuente unico y oficial que se actualiza periodicamente en la misma ubicacion.

EJECUCION DEL DAG

El DAG se ejecuta de manera muy simple ya que no requiere configuracion adicional:

1. Abrir el DAG src_planeacion_extrac_ipm en la interfaz web de Airflow.

2. Hacer clic en el boton Trigger DAG o Run. No es necesario proporcionar ningun parametro en la seccion de configuracion, ya que la URL del archivo esta hardcodeada en el codigo como constante DRIVE_URL.

3. Monitorear la ejecucion en la interfaz de Airflow. El DAG ejecutara las siguientes tareas en secuencia:
   - La tarea upload_file_from_drive_to_gcs mostrara su estado como running mientras descarga y sube el archivo.
   - La tarea trigger_load_ipm ejecutara el DAG src_planeacion_load_ipm que extraera los datos a bronze y luego ejecutara automaticamente el DAG src_planeacion_transf_ipm para transformar los datos a silver y gold. Esta tarea puede tardar varios minutos dependiendo del tamano del archivo y la complejidad de las transformaciones.
   - Finalmente, la tarea end marcara el final del flujo.

4. Verificar en los logs de la tarea upload_file_from_drive_to_gcs la URI final de GCS retornada. Los logs mostraran mensajes informativos sobre el proceso, incluyendo la URL de Drive que se esta usando, el bucket y carpeta destino, y finalmente un mensaje de confirmacion con la URI completa del archivo en GCS, por ejemplo: gs://datalake_gdv_dev/data_staging/dpt_planeacion_municipal/ipm/archivo.xlsx.

5. Una vez completada exitosamente toda la ejecucion, el archivo estara disponible en GCS, habra sido procesado y transformado a traves de las capas bronze, silver y gold en BigQuery, y el DAG principal habra finalizado correctamente.

REQUISITOS PREVIOS

Para que el DAG funcione correctamente, se deben cumplir los siguientes requisitos:

La cuenta de servicio configurada en el entorno de Airflow debe tener permisos de escritura en el bucket de GCS especificado en DEFAULT_BUCKET_NAME. Esto significa que la cuenta de servicio debe tener el rol Storage Object Admin o Storage Object Creator en el bucket datalake_gdv_dev, o al menos permisos para crear y escribir objetos en la carpeta data_staging/dpt_planeacion_municipal/ipm.

El archivo en Google Drive debe ser accesible publicamente mediante enlace. Esto significa que el archivo debe estar configurado como "Cualquier persona con el enlace puede ver" en la configuracion de compartir de Google Drive. Si el archivo no es publico, este DAG no funcionara porque utiliza exclusivamente enlaces publicos. Para que funcione, es necesario que el archivo tenga permisos publicos en Google Drive.

El archivo debe ser un archivo Excel valido con extension .xlsx o .xls. El modulo puede manejar diferentes formatos, pero el procesamiento posterior asume que es un archivo Excel.

La URL hardcodeada DRIVE_URL debe ser valida y apuntar al archivo correcto. Si el archivo se mueve o se elimina en Google Drive, la URL dejara de funcionar y el DAG fallara.

RESUMEN

El DAG src_planeacion_extrac_ipm es el primer componente del pipeline de datos del IPM. Su funcion es mover el archivo Excel del IPM desde un enlace publico fijo de Google Drive (hardcodeado en el codigo como constante DRIVE_URL) a una ubicacion estandarizada en GCS (gs://datalake_gdv_dev/data_staging/dpt_planeacion_municipal/ipm/), y luego ejecutar automaticamente el DAG de carga src_planeacion_load_ipm que a su vez ejecutara el DAG de transformacion src_planeacion_transf_ipm, procesando el archivo a traves de las capas bronze, silver y gold en BigQuery.

El DAG consta de cuatro tareas secuenciales: una tarea de inicio, la descarga y subida del archivo a GCS, la ejecucion del DAG de ingesta (que espera su finalizacion), y una tarea de fin. No requiere parametros de ejecucion ya que la URL del archivo esta definida directamente en el codigo como constante. Esto simplifica su uso pero significa que siempre descargara el mismo archivo desde la misma ubicacion.

La ejecucion es completamente manual, lo que permite controlar cuando se actualiza el archivo fuente en GCS y se ejecuta todo el pipeline de transformacion. Una vez ejecutado exitosamente, el archivo queda disponible en GCS, ha sido procesado completamente a traves de todas las capas de transformacion, y los datos finales estan disponibles en BigQuery para consumo.

El diseno del DAG sigue el principio de simplicidad: toda la complejidad tecnica de descargar desde Google Drive y subir a GCS esta encapsulada en el modulo de utilidades, mientras que el DAG orquesta la ejecucion de manera clara y directa, incluyendo la ejecucion automatica del DAG de ingesta como parte del flujo principal.
