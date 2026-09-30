# Documentación: dags_idi/src_idi_transform_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_transf_idi" que se encarga de transformar los datos del IDI desde la capa bronze hacia las capas silver y gold utilizando modelos dbt. El DAG está diseñado para procesar múltiples años de datos, transformarlos individualmente, consolidarlos en una tabla única en silver, y crear una vista final en gold para análisis y reportes.

El propósito principal de este DAG es automatizar el proceso de transformación de datos del IDI utilizando dbt como motor de transformación, aplicando normalizaciones de columnas, agregación de información de año, normalización de departamentos y reemplazo de valores nulos antes de crear las estructuras finales en las capas silver y gold.

## 2. Estructura del DAG

El DAG está compuesto por dos grupos de tareas principales, "silver" y "gold", que representan las dos capas de transformación. El grupo "silver" contiene cuatro tareas relacionadas con la transformación de datos a la capa silver. El grupo "gold" contiene dos tareas relacionadas con la creación de la capa gold. Además, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Grupo silver

El grupo "silver" comienza con una tarea "ensure_dataset" que asegura que el dataset de silver exista en BigQuery. Luego sigue una secuencia de tareas dbt que procesan los años individualmente y luego los consolidan. La primera tarea dbt es "dbt_idi_2023" que ejecuta el modelo "idi_transformed_data_2023" para transformar los datos del año 2023. La segunda tarea dbt es "dbt_idi_2024" que ejecuta el modelo "idi_transformed_data_2024" para transformar los datos del año 2024. Estas dos tareas se ejecutan en paralelo ya que procesan años diferentes. La tercera tarea dbt es "dbt_idi_consolidated" que ejecuta el modelo "idi_transformed_data_consolidated" para unir las tablas transformadas de 2023 y 2024 en una tabla consolidada con una columna adicional de año.

## 4. Grupo gold

El grupo "gold" comienza con una tarea "ensure_dataset" que asegura que el dataset de gold exista en BigQuery. Luego sigue una tarea dbt "dbt_idi_processed_data" que ejecuta el modelo "idi_processed_data" para crear una vista en la capa gold que apunta a la tabla consolidada de silver. Esta vista facilita el acceso a los datos para análisis y reportes sin necesidad de duplicar los datos.

## 5. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". Las variables "DATASET_ID_SILVER" y "DATASET_ID_GOLD" se importan directamente desde "modules.config" y representan los identificadores de los datasets de silver y gold en BigQuery. La variable "DBT_PROJECT_DIR" se calcula dinámicamente encontrando la raíz del proyecto y construyendo la ruta a la carpeta "dbt". La función "get_dbt_command" se importa desde "modules.config" y se utiliza para generar los comandos dbt con las variables de entorno y parámetros necesarios. Las constantes "DBT_MODEL_2023", "DBT_MODEL_2024", "DBT_MODEL_CONSOLIDATED" y "DBT_MODEL_GOLD" están definidas en el código con los nombres de los modelos dbt a ejecutar.

## 6. Función _ensure_dataset_silver_task

La función "_ensure_dataset_silver_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.idi.idi_load" pasando el identificador del dataset de silver. Esta función asegura que el dataset exista en BigQuery antes de intentar crear tablas o vistas en él, creándolo si no existe.

## 7. Función _ensure_dataset_gold_task

La función "_ensure_dataset_gold_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.idi.idi_load" pasando el identificador del dataset de gold. Esta función asegura que el dataset exista en BigQuery antes de intentar crear vistas en él, creándolo si no existe.

## 8. Transformaciones en silver

Las transformaciones en silver se ejecutan utilizando modelos dbt. El modelo "idi_transformed_data_2023" procesa los datos del año 2023 desde la tabla "idi_raw_data_territorio_2023" en bronze, aplicando normalizaciones de nombres de columnas, agregando una columna de año, normalizando el departamento o entidad a mayúsculas sin tildes, y reemplazando valores NULL o NaN por 0 en columnas numéricas. El modelo "idi_transformed_data_2024" realiza las mismas transformaciones para el año 2024 desde la tabla "idi_raw_data_territorio_2024". El modelo "idi_transformed_data_consolidated" une las dos tablas transformadas de 2023 y 2024 en una tabla única, manteniendo la columna de año para identificar el origen de cada registro. Esta tabla consolidada facilita el análisis de datos históricos y comparaciones entre años.

## 9. Transformación en gold

La transformación en gold utiliza el modelo dbt "idi_processed_data" para crear una vista que apunta a la tabla consolidada de silver. Esta vista no duplica los datos, sino que proporciona un punto de acceso simplificado para análisis y reportes. La vista mantiene toda la estructura y datos de la tabla consolidada, permitiendo consultas directas desde la capa gold sin necesidad de acceder a silver.

## 10. Dependencias entre Tareas

Dentro del grupo "silver", las tareas están conectadas de manera que primero se ejecuta "ensure_dataset", luego las tareas "dbt_idi_2023" y "dbt_idi_2024" se ejecutan en paralelo ya que procesan años diferentes, y finalmente "dbt_idi_consolidated" se ejecuta después de que ambas tareas de años individuales se completen. Dentro del grupo "gold", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "dbt_idi_processed_data". A nivel del DAG completo, "start" precede al grupo "silver", que precede al grupo "gold", que precede a "end".

## 11. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_transf_idi" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:transformacion", "fuente:idi" y "ejecución:manual".

## 12. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos del IDI, siendo disparado automáticamente por el DAG de carga una vez que los datos se encuentran en la capa bronze de BigQuery como tablas separadas por año. El DAG ejecuta transformaciones utilizando dbt, aplicando reglas de negocio y validaciones para asegurar la calidad de los datos antes de crear las estructuras finales en las capas silver y gold. La tabla consolidada en silver permite análisis históricos y comparaciones entre años, mientras que la vista en gold facilita el acceso a los datos para reportes y análisis.

## 13. Notas Importantes

Es importante asegurarse de que los datos ya estén cargados en la capa bronze antes de ejecutar este DAG, ya que los modelos dbt dependen de la existencia de las tablas "idi_raw_data_territorio_2023" e "idi_raw_data_territorio_2024" en el dataset de bronze. El DAG está diseñado para procesar específicamente los años 2023 y 2024, por lo que si se agregan nuevos años, será necesario actualizar el DAG para incluir modelos y tareas adicionales. El DAG está diseñado para ejecutar las transformaciones en un orden específico, primero procesando los años individualmente en paralelo y luego consolidándolos, por lo que es importante no modificar las dependencias entre tareas sin entender completamente el impacto en el proceso de transformación. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde el dataset de bronze y escribir en los datasets de silver y gold. Los modelos dbt deben estar correctamente configurados en el proyecto dbt y deben referenciar correctamente las tablas y variables definidas en "config.yaml". La tabla consolidada en silver incluye una columna de año que identifica el origen de cada registro, lo que es esencial para análisis históricos y comparaciones entre años. La vista en gold no duplica datos, lo que es eficiente en términos de almacenamiento, pero significa que cualquier cambio en la tabla consolidada de silver se reflejará automáticamente en la vista de gold.

