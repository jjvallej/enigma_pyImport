# Documentación: dags_evaplan/src_evaplan_transform_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_transform_evaplan" que se encarga de transformar los datos de Evaplan desde la capa bronze hacia las capas silver y gold. El DAG está diseñado para obtener los periodos únicos de las tablas bronze de la fecha actual, eliminar registros existentes con esos periodos de las tablas silver, copiar los nuevos datos transformados de bronze a silver, y crear vistas en gold que unen las tablas de periodos con las tablas de avances.

El propósito principal de este DAG es automatizar el proceso de transformación de datos de Evaplan, aplicando normalizaciones de nombres de columnas, manteniendo solo los datos más recientes de cada periodo en silver, y creando vistas estructuradas en gold que facilitan el análisis y la consulta de los datos.

## 2. Estructura del DAG

El DAG está compuesto por tres grupos principales de tareas. El primer grupo obtiene los periodos únicos de las tablas bronze. El segundo grupo "silver" contiene tareas que transforman los datos de bronze a silver para cada fuente. El tercer grupo "gold" contiene tareas que crean vistas en gold usando dbt. Además, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Obtención de Periodos

La tarea "get_peri_idps" obtiene todos los "peri_idp" únicos de las tablas bronze de la fecha actual. Esta tarea utiliza "get_all_peri_idps_from_bronze" del módulo "modules.evaplan.evaplan_transform" que consulta todas las tablas bronze de Evaplan y extrae los "peri_idp" únicos que tienen una fecha de lectura del día actual. La lista de periodos se almacena en XCom para ser utilizada por las tareas de transformación.

## 4. Grupo silver

El grupo "silver" comienza con una tarea "ensure_dataset" que asegura que el dataset de silver exista en BigQuery. Luego, para cada fuente definida en "CONF.evaplan.fuentes", se crea un grupo de tareas que transforma los datos de esa fuente de bronze a silver. Todas las transformaciones de fuentes se ejecutan en paralelo ya que procesan fuentes diferentes y no dependen entre sí.

## 5. Transformación de Fuentes

Para cada fuente, la transformación ejecutada por "transform_fuente_to_silver" incluye varios pasos. Primero, obtiene los "peri_idp" únicos de la tarea "get_peri_idps" mediante XCom. Luego, si la tabla silver existe, elimina todos los registros que tengan esos "peri_idp", asegurando que solo los datos más recientes de esos periodos se mantengan. Después, lee los datos de la tabla bronze correspondiente, transforma los nombres de columnas a minúsculas y formato snake_case, y carga los datos transformados en la tabla silver correspondiente. Este proceso asegura que las tablas silver siempre contengan los últimos datos de los endpoints para cada periodo que se está procesando.

## 6. Grupo gold

El grupo "gold" comienza con una tarea "ensure_dataset" que asegura que el dataset de gold exista en BigQuery. Luego, se ejecutan cuatro tareas dbt en paralelo que crean vistas en gold. Cada vista une la tabla de periodos de silver con una tabla de avance específica mediante un JOIN por "peri_idp". Las vistas creadas son "evaplan_api_avance_mr_processed_data", "evaplan_api_avance_mp_processed_data", "evaplan_api_avance_x_subprograma_processed_data" y "evaplan_api_avance_general_processed_data".

## 7. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". Las variables "DATASET_ID_SILVER" y "DATASET_ID_GOLD" se importan directamente desde "modules.config" y representan los identificadores de los datasets de silver y gold en BigQuery. La variable "DBT_PROJECT_DIR" se calcula dinámicamente encontrando la raíz del proyecto y construyendo la ruta a la carpeta "dbt". La función "get_dbt_command" se importa desde "modules.config" y se utiliza para generar los comandos dbt con las variables de entorno y parámetros necesarios. La variable "FUENTES" obtiene su valor de "CONF.evaplan.fuentes", que es una lista de nombres de fuentes a procesar.

## 8. Función _ensure_dataset_silver_task

La función "_ensure_dataset_silver_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.evaplan.evaplan_transform" pasando el identificador del dataset de silver. Esta función asegura que el dataset exista en BigQuery antes de intentar crear tablas en él, creándolo si no existe.

## 9. Función _ensure_dataset_gold_task

La función "_ensure_dataset_gold_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.evaplan.evaplan_transform" pasando el identificador del dataset de gold. Esta función asegura que el dataset exista en BigQuery antes de intentar crear vistas en él, creándolo si no existe.

## 10. Función _get_peri_idps_task

La función "_get_peri_idps_task" obtiene todos los "peri_idp" únicos de las tablas bronze de la fecha actual. La función utiliza "get_all_peri_idps_from_bronze" del módulo "modules.evaplan.evaplan_transform" que consulta todas las tablas bronze de Evaplan y extrae los "peri_idp" únicos. La función retorna una lista ordenada de periodos, que se almacena en XCom para ser utilizada por las tareas de transformación.

## 11. Función _transform_fuente_task

La función "_transform_fuente_task" es una función de orden superior que retorna una función de tarea para una fuente específica. La función retornada recibe el contexto de la tarea a través del parámetro "ti" y utiliza "xcom_pull" para obtener la lista de "peri_idp" de la tarea "get_peri_idps". Luego utiliza "transform_fuente_to_silver" del módulo "modules.evaplan.evaplan_transform" para realizar toda la lógica de transformación, incluyendo la eliminación de registros existentes con esos periodos, la lectura de datos de bronze, la transformación de nombres de columnas y la carga en silver.

## 12. Transformaciones en gold

Las transformaciones en gold se ejecutan utilizando modelos dbt. Cada modelo crea una vista que une la tabla de periodos de silver con una tabla de avance específica mediante un JOIN por "peri_idp". Las vistas facilitan el acceso a los datos combinados sin necesidad de realizar JOINs manuales en cada consulta. Las vistas no duplican datos, sino que proporcionan un punto de acceso simplificado para análisis y reportes.

## 13. Dependencias entre Tareas

La tarea "start" precede a "get_peri_idps_task", que precede al grupo "silver". Dentro del grupo "silver", la tarea "ensure_dataset" precede a todas las transformaciones de fuentes, que se ejecutan en paralelo. El grupo "silver" precede al grupo "gold". Dentro del grupo "gold", la tarea "ensure_dataset" precede a todas las tareas dbt, que se ejecutan en paralelo. Finalmente, el grupo "gold" precede a "end".

## 14. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_transform_evaplan" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:transformacion", "fuente:evaplan" y "ejecución:manual".

## 15. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos de Evaplan, siendo disparado automáticamente por el DAG de carga una vez que los datos se encuentran en la capa bronze de BigQuery como tablas separadas por fuente. El DAG ejecuta transformaciones que normalizan los datos y mantienen solo los datos más recientes de cada periodo en silver, y crea vistas estructuradas en gold que facilitan el análisis y la consulta de los datos.

## 16. Notas Importantes

Es importante asegurarse de que los datos ya estén cargados en la capa bronze antes de ejecutar este DAG, ya que las transformaciones dependen de la existencia de las tablas bronze de Evaplan. El DAG está diseñado para procesar solo los periodos que tienen datos de la fecha actual en bronze, lo que asegura que solo se transformen los datos más recientes. El proceso de eliminación de registros existentes en silver antes de cargar nuevos datos asegura que las tablas silver siempre contengan los últimos datos de cada periodo, evitando duplicados y manteniendo la consistencia. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde el dataset de bronze y escribir en los datasets de silver y gold. Los modelos dbt deben estar correctamente configurados en el proyecto dbt y deben referenciar correctamente las tablas y variables definidas en "config.yaml". Las vistas en gold no duplican datos, lo que es eficiente en términos de almacenamiento, pero significa que cualquier cambio en las tablas de silver se reflejará automáticamente en las vistas de gold. El proceso de transformación normaliza los nombres de columnas a minúsculas y formato snake_case, lo que mejora la consistencia y facilita el trabajo con los datos.

