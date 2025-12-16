# Documentación: dags_ipm/src_ipm_transform_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_transf_ipm" que se encarga de transformar los datos del IPM desde la capa bronze hacia las capas silver y gold utilizando modelos dbt. El DAG está diseñado para ejecutar una serie de transformaciones secuenciales que limpian, normalizan y validan los datos antes de crear las vistas y tablas finales en las capas silver y gold.

El propósito principal de este DAG es automatizar el proceso de transformación de datos utilizando dbt como motor de transformación, aplicando una serie de pasos que incluyen normalización de texto, transformación de tipos de datos, limpieza de números, detección de valores negativos, aplicación de validaciones y finalmente la creación de las estructuras de datos finales.

## 2. Estructura del DAG

El DAG está compuesto por dos grupos de tareas principales, "silver" y "gold", que representan las dos capas de transformación. El grupo "silver" contiene nueve tareas relacionadas con la transformación de datos a la capa silver. El grupo "gold" contiene tres tareas relacionadas con la creación de la capa gold. Además, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Grupo silver

El grupo "silver" comienza con una tarea "ensure_dataset" que asegura que el dataset de silver exista en BigQuery. Luego sigue una secuencia de tareas dbt que ejecutan diferentes modelos de transformación. La primera tarea dbt es "dbt_run_stg" que ejecuta el modelo "ipm_transform_stg". La segunda tarea es "dbt_run_normalize_text" que ejecuta el modelo "ipm_transform_normalize_text". La tercera tarea es "dbt_run_transform_types" que ejecuta el modelo "ipm_transform_transform_types" y tiene un timeout de 15 minutos debido a la complejidad de la transformación. La cuarta tarea es "dbt_run_clean_numbers" que ejecuta el modelo "ipm_transform_clean_numbers". La quinta tarea es "dbt_run_detect_negatives" que ejecuta el modelo "ipm_transform_detect_negatives". La sexta tarea es "dbt_run_apply_validations" que ejecuta el modelo "ipm_transform_apply_validations". La séptima tarea es "dbt_run_clean" que ejecuta el modelo "ipm_transform_clean". La octava tarea es "dbt_test" que ejecuta las pruebas dbt sobre el modelo "ipm_transform_clean" para validar la calidad de los datos transformados.

## 4. Grupo gold

El grupo "gold" comienza con una tarea "ensure_dataset" que asegura que el dataset de gold exista en BigQuery. Luego sigue una tarea dbt "dbt_run_gold" que ejecuta el modelo "ipm_processed_data" para crear la tabla final en la capa gold. Finalmente, hay una tarea "dbt_test" que ejecuta las pruebas dbt sobre el modelo "ipm_processed_data" para validar la calidad de los datos finales.

## 5. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". La variable "TABLE_NAME_SILVER" obtiene su valor de "CONF.ipm.tables.silver", que define el nombre de la tabla o vista en la capa silver. La variable "TABLE_NAME_GOLD" obtiene su valor de "CONF.ipm.tables.gold", que define el nombre de la tabla en la capa gold. Las variables "DATASET_ID_SILVER" y "DATASET_ID_GOLD" se importan directamente desde "modules.config" y representan los identificadores de los datasets de silver y gold en BigQuery. La variable "DBT_PROJECT_DIR" se calcula dinámicamente encontrando la raíz del proyecto y construyendo la ruta a la carpeta "dbt". La función "get_dbt_command" se importa desde "modules.config" y se utiliza para generar los comandos dbt con las variables de entorno y parámetros necesarios.

## 6. Función _ensure_dataset_silver_task

La función "_ensure_dataset_silver_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.ipm.ipm_transform" pasando el identificador del dataset de silver. Esta función asegura que el dataset exista en BigQuery antes de intentar crear tablas o vistas en él.

## 7. Función _ensure_dataset_gold_task

La función "_ensure_dataset_gold_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.ipm.ipm_transform" pasando el identificador del dataset de gold. Esta función asegura que el dataset exista en BigQuery antes de intentar crear tablas en él.

## 8. Tareas dbt en silver

Cada tarea dbt en el grupo silver utiliza "BashOperator" para ejecutar comandos dbt. Las tareas utilizan la función "get_dbt_command" para generar los comandos completos con las variables de entorno y parámetros necesarios. Cada tarea ejecuta un modelo dbt específico usando la sintaxis "dbt run --select nombre_modelo". La tarea "dbt_run_transform_types" tiene un timeout de 15 minutos configurado mediante el parámetro "execution_timeout" debido a la complejidad de la transformación de tipos de datos. Todas las tareas tienen el parámetro "append_env" establecido en "True" para asegurar que las variables de entorno se pasen correctamente al proceso dbt.

## 9. Tareas dbt en gold

Las tareas dbt en el grupo gold siguen el mismo patrón que las del grupo silver, utilizando "BashOperator" y "get_dbt_command" para ejecutar los comandos dbt. La tarea "dbt_run_gold" ejecuta el modelo "ipm_processed_data" que crea la tabla final en la capa gold. La tarea "dbt_test" ejecuta las pruebas dbt sobre el modelo final para validar la calidad de los datos.

## 10. Dependencias entre Tareas

Dentro del grupo "silver", las tareas están conectadas en una secuencia específica. La tarea "ensure_dataset" precede a "dbt_run_stg", que precede a "dbt_run_normalize_text". Después de "dbt_run_normalize_text", las tareas "dbt_run_transform_types" y "dbt_run_clean_numbers" se ejecutan en paralelo, ya que ambas dependen de "dbt_run_normalize_text" pero no dependen entre sí. Después de estas tareas paralelas, la secuencia continúa con "dbt_run_detect_negatives", seguida de "dbt_run_apply_validations", luego "dbt_run_clean", y finalmente "dbt_test". Dentro del grupo "gold", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "dbt_run_gold", que precede a "dbt_test". A nivel del DAG completo, "start" precede al grupo "silver", que precede al grupo "gold".

## 11. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_transf_ipm" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:transformacion", "fuente:ipm" y "ejecución:manual".

## 12. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos del IPM, siendo disparado automáticamente por el DAG de carga una vez que los datos se encuentran en la capa bronze de BigQuery. El DAG ejecuta una serie de transformaciones secuenciales utilizando dbt, aplicando reglas de negocio y validaciones para asegurar la calidad de los datos antes de crear las estructuras finales en las capas silver y gold.

## 13. Notas Importantes

Es importante asegurarse de que los datos ya estén cargados en la capa bronze antes de ejecutar este DAG, ya que los modelos dbt dependen de la existencia de la tabla "ipm_raw_data" en el dataset de bronze. El DAG está diseñado para ejecutar las transformaciones en un orden específico, por lo que es importante no modificar las dependencias entre tareas sin entender completamente el impacto en el proceso de transformación. La tarea "dbt_run_transform_types" tiene un timeout de 15 minutos debido a la complejidad de la transformación, pero si los datos son muy grandes, puede ser necesario aumentar este timeout. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde el dataset de bronze y escribir en los datasets de silver y gold. Los modelos dbt deben estar correctamente configurados en el proyecto dbt y deben referenciar correctamente las tablas y variables definidas en "config.yaml".
