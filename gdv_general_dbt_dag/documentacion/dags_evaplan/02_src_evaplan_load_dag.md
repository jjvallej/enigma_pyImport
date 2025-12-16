# Documentación: dags_evaplan/src_evaplan_load_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_load_evaplan" que se encarga de extraer datos JSON desde Google Cloud Storage y cargarlos en la capa bronze de BigQuery. El DAG está diseñado para buscar solo archivos JSON de la fecha actual en cada carpeta de fuente, procesar todos los JSON encontrados, unir los registros, agregar metadatos y cargar los datos en BigQuery con una estrategia de carga idempotente.

El propósito principal de este DAG es automatizar la carga de datos desde GCS hacia BigQuery, procesando múltiples fuentes de datos de Evaplan de manera automática y paralela. Cada fuente se carga en una tabla separada con el formato "evaplan_api_{fuente}_raw_data", permitiendo mantener los datos organizados por tipo de fuente y facilitando el procesamiento posterior.

## 2. Estructura del DAG

El DAG está compuesto por un grupo de tareas llamado "bronze" que contiene todas las operaciones relacionadas con la carga de datos a la capa bronze. Este grupo incluye una tarea "ensure_dataset" que asegura que el dataset de bronze exista, y múltiples grupos de tareas, uno por cada fuente, que procesan y cargan los datos en paralelo. Después del grupo "bronze", el DAG incluye un "TriggerDagRunOperator" que dispara el DAG de transformación. Finalmente, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Procesamiento de Fuentes

El DAG procesa múltiples fuentes de datos definidas en "CONF.evaplan.fuentes", que típicamente incluyen "periodos", "avance_mr", "avance_mp", "avance_x_subprograma" y "avance_general". Para cada fuente, el DAG busca todos los archivos JSON de la fecha actual en la carpeta correspondiente de GCS, lee y procesa todos los JSON encontrados, extrae los registros de cada JSON, une todos los registros en un solo DataFrame, agrega columnas de metadatos como "fecha_lectura" y "peri_idp", y carga los datos en BigQuery con una estrategia idempotente que elimina registros duplicados antes de cargar.

## 4. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "DEFAULT_BUCKET_NAME" se importa directamente desde "modules.config" y representa el nombre del bucket de GCS. La variable "DATASET_ID_BRONZE" se importa directamente desde "modules.config" y representa el identificador del dataset de bronze en BigQuery. La variable "FUENTES" obtiene su valor de "CONF.evaplan.fuentes", que es una lista de nombres de fuentes a procesar.

## 5. Función _ensure_dataset_bronze_task

La función "_ensure_dataset_bronze_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.evaplan.evaplan_load" pasando el identificador del dataset de bronze. Esta función asegura que el dataset exista en BigQuery antes de intentar cargar datos en él, creándolo si no existe.

## 6. Función _load_fuente_task

La función "_load_fuente_task" es una función de orden superior que retorna una función de tarea para una fuente específica. La función retornada utiliza "get_table_name_for_fuente" del módulo "modules.evaplan.evaplan_load" para obtener el nombre de la tabla correspondiente a la fuente, típicamente con el formato "evaplan_api_{fuente}_raw_data". Luego utiliza "load_json_files_to_bq" del mismo módulo para realizar toda la lógica de carga, incluyendo la búsqueda de archivos JSON de la fecha actual, el procesamiento de los JSON, la unión de registros, la agregación de metadatos y la carga idempotente en BigQuery.

## 7. Proceso de Carga

El proceso de carga ejecutado por "load_json_files_to_bq" incluye varios pasos. Primero, busca todos los archivos JSON de la fecha actual en la carpeta correspondiente a la fuente en GCS. Luego, descarga y lee cada archivo JSON, extrayendo los registros según el tipo de fuente. Para cada registro, extrae el "peri_idp" del nivel raíz del JSON o del nombre del archivo. Une todos los registros de todos los JSON de la fecha actual en un solo DataFrame. Agrega una columna "fecha_lectura" con la fecha y hora actual, y asegura que la columna "peri_idp" esté presente en cada registro. Si la tabla existe en BigQuery y hay registros del mismo día y mismo "peri_idp", los elimina antes de cargar para evitar duplicados. Finalmente, carga el DataFrame en BigQuery creando o actualizando la tabla correspondiente.

## 8. Estrategia de Carga Idempotente

La estrategia de carga idempotente asegura que si el DAG se ejecuta múltiples veces en el mismo día, no se crearán registros duplicados. Antes de cargar nuevos datos, el proceso identifica todos los "peri_idp" únicos en los datos a cargar. Luego, para cada "peri_idp", elimina cualquier registro existente en la tabla que tenga la misma fecha de lectura y el mismo "peri_idp". Esto garantiza que solo los datos más recientes de cada periodo se mantengan en la tabla, evitando duplicados y asegurando la consistencia de los datos.

## 9. Dependencias entre Tareas

Dentro del grupo "bronze", la tarea "ensure_dataset" precede a todos los grupos de carga de fuentes. Todos los grupos de carga de fuentes se ejecutan en paralelo, ya que procesan fuentes diferentes y no dependen entre sí. Después del grupo "bronze", el flujo continúa con "trigger_transform_dag" y luego "end". La tarea "start" precede a todo el grupo "bronze".

## 10. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_load_evaplan" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:carga", "fuente:evaplan" y "ejecución:manual". El "TriggerDagRunOperator" tiene el parámetro "wait_for_completion" establecido en "False", lo que permite que el DAG actual se marque como exitoso inmediatamente después de disparar el DAG de transformación.

## 11. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos de Evaplan, siendo disparado automáticamente por el DAG de ingesta una vez que todos los archivos JSON se encuentran en GCS organizados por fuente y fecha. Una vez que los datos se cargan en BigQuery como tablas separadas por fuente, el DAG dispara automáticamente el DAG de transformación, creando un flujo automatizado que continúa con el procesamiento de los datos. El uso de ejecución en paralelo para las diferentes fuentes mejora la eficiencia del proceso, permitiendo que múltiples fuentes se procesen simultáneamente.

## 12. Notas Importantes

Es importante asegurarse de que existan archivos JSON de la fecha actual en las carpetas correspondientes de GCS antes de ejecutar el DAG, ya que el proceso busca específicamente archivos de la fecha actual. Si no se encuentran archivos, el proceso puede crear tablas vacías o fallar dependiendo de la implementación. El proceso utiliza una estrategia de carga idempotente que elimina registros duplicados antes de cargar, lo que es importante para evitar datos duplicados si el DAG se ejecuta múltiples veces. El proceso extrae el "peri_idp" de cada JSON, ya sea del nivel raíz del JSON o del nombre del archivo, por lo que es importante asegurarse de que los archivos tengan el formato esperado. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde GCS y escribir en BigQuery. El proceso agrega una columna "fecha_lectura" a cada registro, lo que permite rastrear cuándo se cargaron los datos y facilita la identificación de datos duplicados. La estrategia de carga idempotente asegura que solo los datos más recientes de cada periodo se mantengan en la tabla, lo que es importante para mantener la consistencia de los datos.

