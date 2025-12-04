# Documentación: dags_ipm/src_ipm_sisben_transform_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_transform_ipm_sisben" que se encarga de transformar los datos de IPM SISBEN desde la capa bronze hacia las capas silver y gold utilizando modelos dbt. El DAG está diseñado para ejecutar transformaciones que procesan los datos desde la tabla externa creada en la capa bronze, aplicando reglas de negocio y validaciones para crear las estructuras finales en las capas silver y gold.

El propósito principal de este DAG es automatizar el proceso de transformación de datos de IPM SISBEN utilizando dbt como motor de transformación, aplicando transformaciones que incluyen la adición de columnas de descripción para códigos numéricos y la creación de vistas finales en la capa gold que facilitan el análisis y la consulta de los datos.

## 2. Estructura del DAG

El DAG está compuesto por dos grupos de tareas principales, "silver" y "gold", que representan las dos capas de transformación. El grupo "silver" contiene dos tareas relacionadas con la transformación de datos a la capa silver. El grupo "gold" contiene dos tareas relacionadas con la creación de la capa gold. Además, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Grupo silver

El grupo "silver" comienza con una tarea "ensure_dataset" que asegura que el dataset de silver exista en BigQuery. Luego sigue una tarea dbt "dbt_ipm_sisben_stg" que ejecuta el modelo dbt "ipm_sisben_stg" para transformar los datos desde la tabla externa de bronze hacia la capa silver. Esta transformación incluye el procesamiento de los datos y la aplicación de reglas de negocio iniciales.

## 4. Grupo gold

El grupo "gold" comienza con una tarea "ensure_dataset" que asegura que el dataset de gold exista en BigQuery. Luego sigue una tarea dbt "dbt_ipm_sisben_processed_data" que ejecuta el modelo dbt "ipm_sisben_processed_data" para crear la vista final en la capa gold. Esta vista incluye columnas de descripción para códigos numéricos, facilitando el análisis y la interpretación de los datos.

## 5. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". Las variables "DATASET_ID_SILVER" y "DATASET_ID_GOLD" se importan directamente desde "modules.config" y representan los identificadores de los datasets de silver y gold en BigQuery. La variable "DBT_PROJECT_DIR" se calcula dinámicamente encontrando la raíz del proyecto y construyendo la ruta a la carpeta "dbt". La función "get_dbt_command" se importa desde "modules.config" y se utiliza para generar los comandos dbt con las variables de entorno y parámetros necesarios. Las constantes "DBT_MODEL_SILVER" y "DBT_MODEL_GOLD" están definidas en el código con los valores "ipm_sisben_stg" e "ipm_sisben_processed_data" respectivamente, representando los nombres de los modelos dbt a ejecutar.

## 6. Función _ensure_dataset_silver_task

La función "_ensure_dataset_silver_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.ipm.ipm_sisben_load" pasando el identificador del dataset de silver. Esta función asegura que el dataset exista en BigQuery antes de intentar crear tablas o vistas en él, creándolo si no existe.

## 7. Función _ensure_dataset_gold_task

La función "_ensure_dataset_gold_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.ipm.ipm_sisben_load" pasando el identificador del dataset de gold. Esta función asegura que el dataset exista en BigQuery antes de intentar crear vistas en él, creándolo si no existe.

## 8. Tareas dbt en silver

La tarea dbt en el grupo silver utiliza "BashOperator" para ejecutar el comando dbt. La tarea utiliza la función "get_dbt_command" para generar el comando completo con las variables de entorno y parámetros necesarios. La tarea ejecuta el modelo dbt "ipm_sisben_stg" usando la sintaxis "dbt run --select ipm_sisben_stg". La tarea tiene el parámetro "append_env" establecido en "True" para asegurar que las variables de entorno se pasen correctamente al proceso dbt.

## 9. Tareas dbt en gold

La tarea dbt en el grupo gold sigue el mismo patrón que la del grupo silver, utilizando "BashOperator" y "get_dbt_command" para ejecutar el comando dbt. La tarea ejecuta el modelo dbt "ipm_sisben_processed_data" que crea la vista final en la capa gold con las columnas de descripción para códigos numéricos.

## 10. Dependencias entre Tareas

Dentro del grupo "silver", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "dbt_ipm_sisben_stg". Dentro del grupo "gold", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "dbt_ipm_sisben_processed_data". A nivel del DAG completo, "start" precede al grupo "silver", que precede al grupo "gold", que precede a "end". Esta estructura garantiza que las transformaciones se ejecuten en el orden correcto, asegurando que la capa silver se cree antes de la capa gold.

## 11. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_transform_ipm_sisben" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:transformacion", "fuente:ipm_sisben" y "ejecución:manual".

## 12. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos de IPM SISBEN, siendo disparado automáticamente por el DAG de carga una vez que la tabla externa se ha creado en la capa bronze de BigQuery. El DAG ejecuta transformaciones utilizando dbt, aplicando reglas de negocio y creando las estructuras finales en las capas silver y gold que facilitan el análisis y la consulta de los datos.

## 13. Notas Importantes

Es importante asegurarse de que la tabla externa ya esté creada en la capa bronze antes de ejecutar este DAG, ya que los modelos dbt dependen de la existencia de esta tabla. El DAG está diseñado para ejecutar las transformaciones en un orden específico, primero en la capa silver y luego en la capa gold, por lo que es importante no modificar las dependencias entre tareas sin entender completamente el impacto en el proceso de transformación. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde el dataset de bronze y escribir en los datasets de silver y gold. Los modelos dbt deben estar correctamente configurados en el proyecto dbt y deben referenciar correctamente las tablas y variables definidas en "config.yaml". La vista final en la capa gold incluye columnas de descripción para códigos numéricos, lo que facilita el análisis pero puede aumentar el tamaño de la vista, por lo que es importante considerar el impacto en el rendimiento de las consultas. El uso de tablas externas en la capa bronze puede tener un impacto en el rendimiento de las transformaciones comparado con tablas nativas de BigQuery, especialmente para transformaciones complejas o grandes volúmenes de datos.
