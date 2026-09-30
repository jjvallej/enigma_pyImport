# Documentación: dags_evaplan/src_evaplan_ingest_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_ingest_evaplan" que se encarga de ingerir datos desde la API de Evaplan y almacenar las respuestas JSON en Google Cloud Storage. El DAG está diseñado para autenticarse con la API, obtener la lista de periodos disponibles, y luego obtener datos de múltiples endpoints para todos los periodos, almacenando cada respuesta como un archivo JSON separado en GCS.

El propósito principal de este DAG es automatizar la extracción de datos desde la API de Evaplan, procesando todos los periodos disponibles de manera automática y organizando los archivos JSON en carpetas específicas según el tipo de dato. El proceso incluye autenticación con la API, obtención de periodos, y extracción de datos de múltiples tipos de avances diferentes para cada periodo, incluyendo AvanceMR, AvanceMP, AvanceXSubprograma, AvanceGeneral, AvanceSubprogramas, AvanceProgramas y SectorMP.

## 2. Estructura del DAG

El DAG está compuesto por múltiples tareas organizadas en un flujo secuencial y paralelo. La primera tarea es un "EmptyOperator" con el identificador "start" que marca el inicio del flujo. Luego sigue una secuencia de tareas de autenticación y obtención de periodos. Después, cuatro grupos de tareas se ejecutan en paralelo para obtener y guardar los diferentes tipos de avances. Finalmente, hay un "TriggerDagRunOperator" que dispara el DAG de carga y una tarea "end" que marca el final del flujo.

## 3. Flujo de Tareas

El flujo comienza con la tarea "authenticate" que obtiene un token de autenticación de la API. Luego sigue "get_periodos" que obtiene la lista de periodos usando el token. La tarea "save_periodos_to_gcs" guarda la respuesta de periodos en GCS. La tarea "read_periodos_from_gcs" lee el JSON más reciente de periodos desde GCS para obtener todos los periodos disponibles. Después de leer los periodos, múltiples grupos de tareas se ejecutan en paralelo. Cada grupo incluye una tarea para obtener datos de un tipo de avance y otra para guardar esos datos en GCS. Los grupos incluyen "get_avance_mr" y "save_avance_mr_to_gcs", "get_avance_mp" y "save_avance_mp_to_gcs", "get_avance_x_subprograma" y "save_avance_x_subprograma_to_gcs", "get_avance_general" y "save_avance_general_to_gcs", "get_avance_subprogramas" y "save_avance_subprogramas_to_gcs", "get_avance_programas" y "save_avance_programas_to_gcs", y "get_sector_mp" y "save_sector_mp_to_gcs". Finalmente, después de que todas las tareas de guardado terminen, se dispara el DAG de carga.

## 4. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "DEFAULT_BUCKET_NAME" se importa directamente desde "modules.config" y representa el nombre del bucket de GCS donde se almacenarán los archivos. Las rutas de las carpetas se construyen usando "CONF.evaplan.gcs_base_folder" y las carpetas específicas definidas en "CONF.evaplan.gcs_folders" para cada tipo de dato, incluyendo "periodos", "avance_mr", "avance_mp", "avance_x_subprograma", "avance_general", "avance_subprogramas", "avance_programas" y "sector_mp".

## 5. Función _authenticate_task

La función "_authenticate_task" es responsable de autenticarse con la API de Evaplan y obtener un token Bearer. La función utiliza la función "authenticate" del módulo "modules.evaplan.evaplan_ingest" que lee las credenciales desde "config.yaml" y realiza una petición de autenticación a la API. La función retorna el token obtenido, que se almacena en XCom para ser utilizado por las tareas siguientes que requieren autenticación.

## 6. Función _get_periodos_task

La función "_get_periodos_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener el token de autenticación de la tarea anterior. La función utiliza "get_periodos" del módulo "modules.evaplan.evaplan_ingest" para obtener la lista de periodos desde la API usando el token. La función retorna los datos de periodos, que se almacenan en XCom para ser utilizados por las tareas siguientes.

## 7. Función _save_periodos_to_gcs_task

La función "_save_periodos_to_gcs_task" recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener los datos de periodos de la tarea anterior. La función construye la ruta de la carpeta destino combinando "CONF.evaplan.gcs_base_folder" con "CONF.evaplan.gcs_folders.periodos". La función utiliza "save_periodos_to_gcs" del módulo "modules.evaplan.evaplan_ingest" para guardar los datos en un archivo JSON en GCS con un nombre que incluye la fecha y hora actual. La función retorna el URI de GCS donde se guardó el archivo.

## 8. Función _read_periodos_from_gcs_task

La función "_read_periodos_from_gcs_task" lee el archivo JSON más reciente de periodos desde GCS para obtener todos los periodos disponibles. La función utiliza "read_latest_periodos_json_from_gcs" del módulo "modules.evaplan.evaplan_ingest" para leer el archivo más reciente de la fecha actual. Luego utiliza "get_all_periodos_from_json" para extraer la lista de periodos del JSON. La función retorna la lista de periodos, que se almacena en XCom para ser utilizada por las tareas que obtienen los avances.

## 9. Funciones de Obtención de Avances

Las funciones "_get_avance_mr_task", "_get_avance_mp_task", "_get_avance_x_subprograma_task", "_get_avance_general_task", "_get_avance_subprogramas_task", "_get_avance_programas_task" y "_get_sector_mp_task" siguen un patrón similar. Cada función recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener el token de autenticación y la lista de periodos. Cada función itera sobre todos los periodos y llama a la función correspondiente del módulo "modules.evaplan.evaplan_ingest" para obtener los datos de avance para cada periodo. Las funciones retornan una lista de diccionarios, cada uno conteniendo el periodo y los datos de avance correspondientes.

## 10. Funciones de Guardado de Avances

Las funciones "_save_avance_mr_to_gcs_task", "_save_avance_mp_to_gcs_task", "_save_avance_x_subprograma_to_gcs_task", "_save_avance_general_to_gcs_task", "_save_avance_subprogramas_to_gcs_task", "_save_avance_programas_to_gcs_task" y "_save_sector_mp_to_gcs_task" siguen un patrón similar. Cada función recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la lista de avances de la tarea correspondiente de obtención. Cada función itera sobre todos los avances y utiliza "save_avance_to_gcs" del módulo "modules.evaplan.evaplan_ingest" para guardar cada avance en un archivo JSON separado en GCS. Los archivos se guardan en carpetas específicas según el tipo de avance, y cada archivo incluye el "peri_idp" en su nombre para facilitar la identificación. Las funciones retornan una lista de URIs de GCS donde se guardaron los archivos. La función "_save_sector_mp_to_gcs_task" maneja el caso especial donde los datos están en "data.AvanceMP" aunque el endpoint sea SectorMP.

## 11. Dependencias entre Tareas

Las tareas están conectadas en un flujo donde "start" precede a "authenticate", que precede a "get_periodos", que precede a "save_periodos_to_gcs", que precede a "read_periodos_from_gcs". Después de "read_periodos_from_gcs", múltiples tareas de obtención de avances se ejecutan en paralelo, cada una dependiendo del token y de los periodos. Estas incluyen "get_avance_mr", "get_avance_mp", "get_avance_x_subprograma", "get_avance_general", "get_avance_subprogramas", "get_avance_programas" y "get_sector_mp". Después de cada tarea de obtención, sigue la tarea correspondiente de guardado. Finalmente, después de que todas las tareas de guardado terminen, se ejecuta "trigger_load_dag" y luego "end".

## 12. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_ingest_evaplan" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:ingesta", "fuente:evaplan" y "ejecución:manual". El "TriggerDagRunOperator" tiene el parámetro "wait_for_completion" establecido en "False", lo que permite que el DAG actual se marque como exitoso inmediatamente después de disparar el DAG de carga.

## 13. Uso en el Código

Este DAG se utiliza como punto de entrada para el proceso de ingesta de datos de Evaplan. Una vez que todos los archivos JSON se encuentran en GCS organizados por tipo de dato y periodo, el DAG dispara automáticamente el DAG de carga, creando un flujo automatizado que continúa con el procesamiento de los datos sin intervención manual adicional. El uso de ejecución en paralelo para los diferentes tipos de avances mejora la eficiencia del proceso, permitiendo que múltiples llamadas a la API se realicen simultáneamente.

## 14. Notas Importantes

Es importante asegurarse de que las credenciales de autenticación estén correctamente configuradas en "config.yaml" en la sección "evaplan", ya que el proceso de autenticación es esencial para acceder a la API. El proceso obtiene datos para todos los periodos disponibles, por lo que el tiempo de ejecución puede variar dependiendo de cuántos periodos existan. Si algún periodo falla durante el proceso, el DAG puede continuar procesando los periodos restantes, pero es importante revisar los logs para identificar cualquier problema. Los archivos JSON se guardan con nombres que incluyen la fecha y hora actual, así como el "peri_idp" para facilitar la identificación y el procesamiento posterior. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para escribir en el bucket de GCS especificado. El proceso utiliza XCom para pasar datos entre tareas, por lo que es importante asegurarse de que los datos no excedan los límites de tamaño de XCom en Airflow.

