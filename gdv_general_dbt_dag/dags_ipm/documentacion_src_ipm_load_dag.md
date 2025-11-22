DOCUMENTACION DETALLADA DEL DAG src_ipm_load_dag.py

Este documento describe de forma detallada y continua el DAG de Airflow src_planeacion_load_ipm definido en el archivo src_ipm_load_dag.py. El DAG orquesta la extraccion de un archivo Excel del IPM desde Google Cloud Storage GCS, su transformacion minima en Python para la capa bronze, y la carga a BigQuery.

PROPOSITO GENERAL

Automatizar el flujo de extraccion de datos del IPM a partir de un Excel subido previamente a GCS. El DAG:
- Asegura la existencia del dataset bronze en BigQuery.
- Localiza y descarga automaticamente el archivo Excel mas reciente en una carpeta de GCS.
- Realiza transformaciones minimas en pandas manteniendo los datos en formato texto para preservarlos en bronze.
- Carga los datos transformados a la tabla ipm_raw_data en el dataset bronze.
- Ejecuta automaticamente el DAG de transformacion src_planeacion_transf_ipm al finalizar.

CONFIGURACION PRINCIPAL

El archivo comienza importando las librerias necesarias de Airflow y el modulo local ipm_load que contiene las funciones de utilidad para extraer desde GCS, transformar y cargar a BigQuery.

Constantes dentro del DAG:
- GCS_BUCKET_NAME: bucket de origen en GCS. Por defecto "datalake_gdv". Esta constante define donde se buscara el archivo.
- GCS_FOLDER_PATH: carpeta con los Excel. Por defecto "data_staging/dpt_planeacion_municipal/ipm". Esta ruta estandariza la ubicacion de los archivos fuente del IPM.
- DATASET_ID_BRONZE: dataset de bronze en BigQuery, "bronze_dpt_planeacion_municipal_dev".
- TABLE_NAME_BRONZE: nombre de la tabla bronze, "ipm_raw_data".
- SHEET_INDEX: indice de hoja de Excel a procesar. Por defecto 0.

Variables de Airflow opcionales que sobrescriben las constantes de GCS:
- ipm_gcs_bucket: nombre del bucket de GCS.
- ipm_gcs_folder: ruta de la carpeta en GCS.

Si existen, el DAG las utilizara; si no, empleara los valores por defecto.

ESTRUCTURA DEL DAG

El DAG src_planeacion_load_ipm se define con las siguientes caracteristicas:
- dag_id: "src_planeacion_load_ipm" identificador unico del DAG
- start_date: Fecha de inicio, 1 de enero de 2024
- schedule_interval: None, lo que significa que el DAG no se ejecuta automaticamente, solo manualmente
- catchup: False, no ejecuta ejecuciones pasadas
- tags: Etiquetas para categorizar el DAG: secretaria:planeacion, actividad:ingesta, fuente:ipm, ejecucion:manual
- description: Descripcion detallada del proposito del DAG que incluye la extraccion desde GCS, transformacion minima y carga a bronze, y ejecucion del DAG de transformacion

El DAG consta de un TaskGroup "bronze" que contiene todas las tareas de extraccion y carga, seguido de una tarea que ejecuta el DAG de transformacion:

1. start (EmptyOperator): Marcador de inicio que no realiza ninguna accion. Sirve como punto de entrada del flujo.

2. TaskGroup "bronze": Grupo de tareas que extrae desde GCS y carga a bronze:
   - ensure_dataset (PythonOperator): Asegura que el dataset bronze exista en BigQuery.
   - download_excel (PythonOperator): Busca y descarga el archivo Excel mas reciente desde GCS.
   - transform_dataframe (PythonOperator): Transforma el Excel descargado, preservando datos como texto.
   - load_to_bq (PythonOperator): Carga el DataFrame transformado a BigQuery en la tabla bronze.
   - cleanup_temp_files (PythonOperator): Elimina archivos temporales (TriggerRule.ALL_DONE).

3. trigger_transf_ipm (TriggerDagRunOperator): Tarea que ejecuta el DAG de transformacion src_planeacion_transf_ipm. Esta tarea espera a que el DAG de transformacion termine completamente antes de continuar (wait_for_completion=True).

4. end (EmptyOperator): Marcador de fin que no realiza ninguna accion. Sirve como punto de salida del flujo.

Dependencias: start ejecuta primero, luego el TaskGroup "bronze" con sus tareas internas en secuencia, despues trigger_transf_ipm, y finalmente end. La secuencia es: start -> bronze (ensure_dataset -> download_excel -> transform_dataframe -> load_to_bq -> cleanup_temp_files) -> trigger_transf_ipm -> end.

LOGICA DE LAS TAREAS

La funcion _ensure_dataset_bronze_task simplemente llama a ensure_dataset del modulo ipm_load para asegurar que el dataset bronze exista.

La funcion _download_excel_task resuelve primero el bucket y carpeta desde Variables de Airflow o usa los valores por defecto. Luego busca en GCS el archivo .xlsx mas reciente en la carpeta indicada usando get_latest_excel_from_gcs_folder, y finalmente descarga el archivo usando download_excel_from_gcs, retornando la ruta local del archivo descargado via XCom.

La funcion _transform_task recibe la ruta del Excel desde XCom (task bronze.download_excel), transforma el Excel usando transform_excel del modulo ipm_load, serializa el DataFrame en un archivo .pkl temporal y retorna su ruta via XCom.

La funcion _load_task recibe la ruta del .pkl desde XCom (task bronze.transform_dataframe), deserializa el DataFrame y lo carga a BigQuery usando load_dataframe_to_bq del modulo ipm_load.

La funcion _cleanup_temp_files_task recibe ambas rutas (Excel y .pkl) desde XCom y las elimina usando cleanup_temp_paths del modulo ipm_load.

La tarea trigger_transf_ipm utiliza el operador TriggerDagRunOperator de Airflow para ejecutar el DAG src_planeacion_transf_ipm. Esta tarea tiene configurado wait_for_completion=True, lo que significa que el DAG principal esperara a que el DAG de transformacion termine completamente antes de continuar.

INTERACCION CON EL MODULO DE UTILIDADES

Las funciones del modulo ipm_load encapsulan todos los detalles tecnicos de la extraccion, transformacion y carga. El modulo maneja la busqueda del archivo mas reciente en GCS, la descarga, la transformacion minima preservando los datos como texto, y la carga a BigQuery con el esquema apropiado.

Esta separacion de responsabilidades permite que el DAG sea simple y claro, mientras que la complejidad tecnica se maneja en el modulo de utilidades.

MANEJO DE ERRORES

Si no se encuentra ningun archivo Excel en la carpeta de GCS, la funcion get_latest_excel_from_gcs_folder lanzara una excepcion ValueError. Esta excepcion se propagara hasta la tarea de Airflow, que marcara la tarea como FAILED.

Si hay un error al acceder al bucket de GCS o al descargar el archivo, las funciones del modulo lanzaran excepciones que tambien marcaran las tareas como FAILED.

Si hay un error al cargar datos a BigQuery, la funcion load_dataframe_to_bq lanzara una excepcion que marcara la tarea como FAILED.

La tarea cleanup_temp_files usa TriggerRule.ALL_DONE, lo que garantiza que siempre se ejecute, incluso si fallan tareas previas, asegurando la limpieza de archivos temporales.

Si el DAG de transformacion falla, la tarea trigger_transf_ipm tambien fallara, asegurando que todo el pipeline falle de manera controlada.

Los logs de Airflow mostraran el mensaje de error especifico, lo que facilita la depuracion.

IDEMPOTENCIA Y CONVENCIONES

El DAG tiene caracteristicas de idempotencia y sigue convenciones estandar:

La carga a BigQuery usa WRITE_TRUNCATE, lo que significa que si la tabla ya existe, se reemplazara completamente con los nuevos datos. Esto significa que si se ejecuta el DAG multiples veces, siempre reemplazara los datos anteriores con los nuevos. Este comportamiento simplifica los reintentos.

El DAG busca automaticamente el archivo mas reciente en la carpeta configurada, por lo que siempre procesara la version mas actualizada disponible.

La limpieza de archivos temporales siempre se ejecuta, incluso si ocurre un error, evitando acumulacion de archivos temporales en el sistema.

EJECUCION DEL DAG

El DAG se ejecuta de manera simple ya que puede usar Variables de Airflow opcionales o valores por defecto:

1. Abrir el DAG src_planeacion_load_ipm en la interfaz web de Airflow.

2. Hacer clic en el boton Trigger DAG o Run. No es necesario proporcionar parametros si se usan los valores por defecto. Si se desea cambiar el bucket o carpeta de GCS, se pueden configurar Variables de Airflow antes de ejecutar.

3. Monitorear la ejecucion en la interfaz de Airflow. El DAG ejecutara las siguientes tareas en secuencia:
   - La tarea ensure_dataset mostrara su estado como running mientras verifica o crea el dataset.
   - La tarea download_excel mostrara su estado como running mientras busca y descarga el archivo mas reciente desde GCS.
   - La tarea transform_dataframe mostrara su estado como running mientras transforma el Excel.
   - La tarea load_to_bq mostrara su estado como running mientras carga los datos a BigQuery.
   - La tarea cleanup_temp_files eliminara los archivos temporales.
   - La tarea trigger_transf_ipm ejecutara el DAG src_planeacion_transf_ipm que transformara los datos a silver y gold. Esta tarea puede tardar varios minutos dependiendo de la complejidad de las transformaciones.
   - Finalmente, la tarea end marcara el final del flujo.

4. Verificar en los logs de las tareas la ejecucion del proceso. Los logs mostraran mensajes informativos sobre cada paso, incluyendo el archivo encontrado en GCS, el numero de filas transformadas, y la confirmacion de carga en BigQuery.

5. Una vez completada exitosamente toda la ejecucion, los datos estaran disponibles en bronze, habran sido transformados a traves de las capas silver y gold en BigQuery, y el DAG principal habra finalizado correctamente.

REQUISITOS PREVIOS

Para que el DAG funcione correctamente, se deben cumplir los siguientes requisitos:

La cuenta de servicio configurada en el entorno de Airflow debe tener permisos de lectura en el bucket de GCS especificado y permisos de lectura/escritura en BigQuery. Esto significa que la cuenta de servicio debe tener los roles apropiados para acceder a GCS y BigQuery.

Los datos deben estar disponibles en GCS en la carpeta configurada (normalmente subidos por el DAG src_planeacion_extrac_ipm que descarga desde Google Drive). Al menos un archivo Excel (.xlsx) debe existir en la carpeta para que el DAG funcione correctamente.

El archivo Excel debe tener el formato esperado con las columnas del IPM. Si el formato no es correcto, la transformacion fallara con un error descriptivo.

RESUMEN

El DAG src_planeacion_load_ipm es el segundo componente del pipeline de datos del IPM. Su funcion es extraer el archivo Excel mas reciente desde GCS, aplicar transformaciones minimas preservando los datos como texto, y cargarlo a BigQuery en la capa bronze (bronze_dpt_planeacion_municipal_dev.ipm_raw_data), luego ejecutar automaticamente el DAG de transformacion src_planeacion_transf_ipm que procesa los datos a traves de las capas silver y gold.

El DAG consta de un TaskGroup "bronze" con tareas secuenciales para asegurar el dataset, descargar el Excel, transformar los datos, cargar a BigQuery, y limpiar archivos temporales, seguido de una tarea que ejecuta el DAG de transformacion. Puede usar Variables de Airflow para configurar el bucket y carpeta de GCS, o usar valores por defecto hardcodeados.

La ejecucion es completamente manual, lo que permite controlar cuando se procesan los datos y se ejecuta todo el pipeline de transformacion. Una vez ejecutado exitosamente, los datos quedan disponibles en bronze, han sido procesados completamente a traves de todas las capas de transformacion, y los datos finales estan disponibles en BigQuery para consumo.

El diseno del DAG sigue el principio de simplicidad: toda la complejidad tecnica de extraccion, transformacion y carga esta encapsulada en el modulo de utilidades, mientras que el DAG orquesta la ejecucion de manera clara y directa, incluyendo la ejecucion automatica del DAG de transformacion como parte del flujo principal.
