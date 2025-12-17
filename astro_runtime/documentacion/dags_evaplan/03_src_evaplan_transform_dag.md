# Documentación: dags_evaplan/src_evaplan_transform_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_transform_evaplan" que se encarga de transformar los datos de Evaplan desde la capa bronze hacia las capas silver y gold. El DAG está diseñado para obtener los periodos únicos de las tablas bronze de la fecha actual, eliminar registros existentes con esos periodos de las tablas silver, copiar los nuevos datos transformados de bronze a silver, crear vistas/tablas processed en gold y, finalmente, ejecutar los modelos FACT de gold (FACT_ENTIDAD, FACT_PROGRAMA y FACT_RESUMEN).

El propósito principal de este DAG es automatizar el proceso de transformación de datos de Evaplan, aplicando normalizaciones de nombres de columnas, manteniendo solo los datos más recientes de cada periodo en silver, creando vistas estructuradas en gold y materializando tablas de hechos listas para consumo analítico.

## 2. Estructura del DAG

El DAG está compuesto por cuatro bloques principales. El primer bloque obtiene los periodos únicos de las tablas bronze. El segundo bloque "silver" contiene tareas que transforman los datos de bronze a silver para cada fuente. El tercer bloque "gold" crea los modelos processed_data en gold usando dbt. El cuarto bloque "gold_facts" ejecuta los tres modelos FACT en gold (FACT_ENTIDAD, FACT_PROGRAMA, FACT_RESUMEN) también mediante dbt. Además, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Obtención de Periodos

La tarea "get_peri_idps" obtiene todos los "peri_idp" únicos de las tablas bronze de la fecha actual. Esta tarea utiliza "get_all_peri_idps_from_bronze" del módulo "modules.evaplan.evaplan_transform" que consulta todas las tablas bronze de Evaplan y extrae los "peri_idp" únicos que tienen una fecha de lectura del día actual. La lista de periodos se almacena en XCom para ser utilizada por las tareas de transformación.

## 4. Grupo silver

El grupo "silver" comienza con una tarea "ensure_dataset" que asegura que el dataset de silver exista en BigQuery. Luego, para cada fuente definida en "CONF.evaplan.fuentes", se crea un grupo de tareas que transforma los datos de esa fuente de bronze a silver. Todas las transformaciones de fuentes se ejecutan en paralelo ya que procesan fuentes diferentes y no dependen entre sí.

## 5. Transformación de Fuentes

Para cada fuente, la transformación ejecutada por "transform_fuente_to_silver" incluye varios pasos. Primero, obtiene los "peri_idp" únicos de la tarea "get_peri_idps" mediante XCom. Luego, si la tabla silver existe, elimina todos los registros que tengan esos "peri_idp", asegurando que solo los datos más recientes de esos periodos se mantengan. Después, lee los datos de la tabla bronze correspondiente, transforma los nombres de columnas a minúsculas y formato snake_case, y carga los datos transformados en la tabla silver correspondiente. Este proceso asegura que las tablas silver siempre contengan los últimos datos de los endpoints para cada periodo que se está procesando.

## 6. Grupo gold

El grupo "gold" comienza con una tarea "ensure_dataset" que asegura que el dataset de gold exista en BigQuery. Luego, se ejecutan siete tareas dbt en secuencia que crean las tablas/vistas processed_data en gold: "evaplan_api_avance_mr_processed_data", "evaplan_api_avance_mp_processed_data", "evaplan_api_avance_x_subprograma_processed_data", "evaplan_api_avance_general_processed_data", "evaplan_api_avance_subprogramas_processed_data", "evaplan_api_avance_programas_processed_data" y "evaplan_api_sector_mp_processed_data". Estas tareas enriquecen los avances con información de periodos y preparan los datos para los hechos.

## 7. Grupo gold_facts

El bloque "gold_facts" corre después del grupo gold y ejecuta cuatro tareas dbt secuenciales dentro de un TaskGroup dedicado: "dbt_fact_entidad", "dbt_fact_programa", "dbt_fact_resumen" y "dbt_fact_sector". Estas tareas materializan las tablas de hechos FACT_ENTIDAD, FACT_PROGRAMA, FACT_RESUMEN y FACT_SECTOR en la capa gold. El flujo de dependencias es: start → get_peri_idps → silver → gold → gold_facts → end.

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

La tarea "start" precede a "get_peri_idps_task", que precede al grupo "silver". Dentro del grupo "silver", la tarea "ensure_dataset" precede a todas las transformaciones de fuentes, que se ejecutan en paralelo. El grupo "silver" precede al grupo "gold". Dentro del grupo "gold", la tarea "ensure_dataset" precede a las seis tareas dbt processed_data, que se ejecutan en paralelo. El grupo "gold" precede al grupo "gold_facts". Dentro de "gold_facts" se ejecutan las tres tareas dbt para FACT_ENTIDAD, FACT_PROGRAMA y FACT_RESUMEN. Finalmente, "gold_facts" precede a "end".

## 14. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_transform_evaplan" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:transformacion", "fuente:evaplan" y "ejecución:manual".

## 15. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos de Evaplan, siendo disparado automáticamente por el DAG de carga una vez que los datos se encuentran en la capa bronze de BigQuery como tablas separadas por fuente. El DAG ejecuta transformaciones que normalizan los datos y mantienen solo los datos más recientes de cada periodo en silver, y crea vistas estructuradas en gold que facilitan el análisis y la consulta de los datos.

## 16. Notas Importantes

Es importante asegurarse de que los datos ya estén cargados en la capa bronze antes de ejecutar este DAG, ya que las transformaciones dependen de la existencia de las tablas bronze de Evaplan. El DAG está diseñado para procesar solo los periodos que tienen datos de la fecha actual en bronze, lo que asegura que solo se transformen los datos más recientes. El proceso de eliminación de registros existentes en silver antes de cargar nuevos datos asegura que las tablas silver siempre contengan los últimos datos de cada periodo, evitando duplicados y manteniendo la consistencia. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde el dataset de bronze y escribir en los datasets de silver y gold. Los modelos dbt deben estar correctamente configurados en el proyecto dbt y deben referenciar correctamente las tablas y variables definidas en "config.yaml". Las vistas en gold no duplican datos, lo que es eficiente en términos de almacenamiento, pero significa que cualquier cambio en las tablas de silver se reflejará automáticamente en las vistas de gold. El proceso de transformación normaliza los nombres de columnas a minúsculas y formato snake_case, lo que mejora la consistencia y facilita el trabajo con los datos.

---

Última actualización: 2025-12-17  
Archivo documentado: "dags_evaplan/src_evaplan_transform_dag.py"  
Versión del archivo: 3.1

