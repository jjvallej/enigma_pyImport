# Documentación: dags_idc/src_idc_transform_dag.py

## 1. Información General

Este archivo define el DAG de Airflow "src_planeacion_transf_idc" que se encarga de transformar los datos del IDC desde la capa bronze hacia las capas silver y gold. El DAG está diseñado para ejecutar una serie de transformaciones secuenciales que normalizan los nombres de columnas, convierten valores a minúsculas, normalizan departamentos, reemplazan valores nulos y redondean o convierten columnas numéricas antes de crear las estructuras finales en las capas silver y gold.

El propósito principal de este DAG es automatizar el proceso de transformación de datos utilizando Python y BigQuery directamente para las transformaciones en la capa silver, y dbt para crear la tabla final en la capa gold que une las tres tablas transformadas con el diccionario de indicadores.

## 2. Estructura del DAG

El DAG está compuesto por dos grupos de tareas principales, "silver" y "gold", que representan las dos capas de transformación. El grupo "silver" contiene múltiples tareas que procesan las tres tablas del IDC en paralelo para cada paso de transformación. El grupo "gold" contiene tareas que crean la tabla final uniendo las tres tablas transformadas con el diccionario. Además, hay tareas "start" y "end" que marcan el inicio y el final del flujo.

## 3. Grupo silver

El grupo "silver" procesa las tres tablas del IDC, "dato_original", "valor_normalizado" y "valor_ranking", aplicando transformaciones en paralelo para cada tabla. El proceso incluye cuatro pasos principales. El primer paso normaliza los nombres de columnas a snake_case usando la función "normalize_columns_step". El segundo paso normaliza el departamento a mayúsculas sin acentos usando la función "uppercase_departamento_step". El tercer paso reemplaza valores NULL o NaN por 0 en columnas numéricas usando la función "fill_nulls_step". El cuarto paso crea las tablas finales con todas las transformaciones aplicadas usando la función "transform_table_complete", que redondea decimales para "dato_original" y "valor_normalizado", y convierte a enteros para "valor_ranking".

## 4. Grupo gold

El grupo "gold" comienza con una tarea "ensure_dataset" que asegura que el dataset de gold exista en BigQuery. Luego sigue una tarea dbt "dbt_fact_idc" que ejecuta el modelo dbt "idc_processed_data" para crear la tabla final "fact_idc" que une las tres tablas transformadas de silver con el diccionario "dim_idc" de gold. La tabla final tiene la estructura con columnas DEPARTAMENTO, ANIO, ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, VALOR_ORIGINAL, VALOR_NORMALIZADO y VALOR_RANKING.

## 5. Configuración y Parámetros

Todos los valores de configuración se leen desde el archivo "config.yaml" a través del objeto "CONF" importado desde "modules.config". Las variables "DATASET_ID_SILVER" y "DATASET_ID_GOLD" se importan directamente desde "modules.config" y representan los identificadores de los datasets de silver y gold en BigQuery. La variable "DBT_PROJECT_DIR" se calcula dinámicamente encontrando la raíz del proyecto y construyendo la ruta a la carpeta "dbt". La función "get_dbt_command" se importa desde "modules.config" y se utiliza para generar los comandos dbt con las variables de entorno y parámetros necesarios.

## 6. Función _ensure_dataset_silver_task

La función "_ensure_dataset_silver_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.idc.idc_load" pasando el identificador del dataset de silver. Esta función asegura que el dataset exista en BigQuery antes de intentar crear tablas en él.

## 7. Función _ensure_dataset_gold_task

La función "_ensure_dataset_gold_task" es una función wrapper que llama a la función "ensure_dataset" del módulo "modules.idc.idc_load" pasando el identificador del dataset de gold. Esta función asegura que el dataset exista en BigQuery antes de intentar crear tablas en él.

## 8. Transformaciones en silver

Las transformaciones en silver se ejecutan utilizando funciones del módulo "modules.idc.idc_transform". La función "normalize_columns_step" convierte los nombres de columnas a snake_case y crea vistas intermedias. La función "uppercase_departamento_step" normaliza el departamento a mayúsculas sin acentos y crea vistas intermedias. La función "fill_nulls_step" reemplaza valores NULL o NaN por 0 en columnas numéricas y crea vistas intermedias. La función "transform_table_complete" combina todas las transformaciones anteriores y crea las tablas finales en silver, aplicando redondeo de decimales para "dato_original" y "valor_normalizado", y conversión a enteros para "valor_ranking". Cada función se ejecuta para las tres tablas en paralelo, pero las funciones se ejecutan secuencialmente, es decir, primero todas las normalizaciones de columnas, luego todas las normalizaciones de departamento, luego todos los reemplazos de nulos, y finalmente todas las transformaciones completas.

## 9. Transformación en gold

La transformación en gold utiliza dbt para crear la tabla final "fact_idc" que une las tres tablas transformadas de silver con el diccionario "dim_idc" de gold. La tarea "dbt_fact_idc" ejecuta el modelo dbt "idc_processed_data" que realiza el join entre las tablas y crea la estructura final con todas las columnas necesarias. Esta transformación depende de que las tres tablas finales de silver estén completas y de que la tabla "dim_idc" exista en gold.

## 10. Dependencias entre Tareas

Dentro del grupo "silver", las tareas están conectadas de manera que primero se ejecutan todas las normalizaciones de columnas en paralelo para las tres tablas, luego todas las normalizaciones de departamento en paralelo, luego todos los reemplazos de nulos en paralelo, y finalmente todas las transformaciones completas en paralelo. Cada paso depende del anterior para cada tabla específica. Dentro del grupo "gold", las tareas están conectadas en una secuencia lineal donde "ensure_dataset" precede a "dbt_fact_idc". A nivel del DAG completo, "start" precede al grupo "silver", y las tres tablas finales de silver deben completarse antes de que el grupo "gold" comience. Finalmente, el grupo "gold" precede a "end".

## 11. Características del DAG

El DAG tiene un "dag_id" de "src_planeacion_transf_idc" y está configurado con una fecha de inicio del 1 de enero de 2024. El parámetro "schedule" está establecido en "None", lo que significa que el DAG solo se ejecuta manualmente. El parámetro "catchup" está en "False". El DAG está etiquetado con "secretaria:planeacion", "actividad:transformacion", "fuente:idc" y "ejecución:manual". Todas las tareas de transformación tienen un timeout de 30 minutos configurado mediante el parámetro "execution_timeout" debido a la complejidad de las transformaciones.

## 12. Uso en el Código

Este DAG se utiliza como parte del flujo de procesamiento de datos del IDC, siendo disparado automáticamente por el DAG de carga una vez que los datos se encuentran en la capa bronze de BigQuery como tres tablas separadas. El DAG ejecuta una serie de transformaciones secuenciales utilizando Python y BigQuery directamente para la capa silver, y dbt para la capa gold, aplicando reglas de negocio y validaciones para asegurar la calidad de los datos antes de crear las estructuras finales.

## 13. Notas Importantes

Es importante asegurarse de que los datos ya estén cargados en la capa bronze antes de ejecutar este DAG, ya que las transformaciones dependen de la existencia de las tres tablas "idc_raw_data_dato_original", "idc_raw_data_valor_normalizado" e "idc_raw_data_valor_ranking" en el dataset de bronze. El DAG también depende de que la tabla "dim_idc" exista en el dataset de gold, ya que esta tabla se une con las tablas transformadas para crear la tabla final "fact_idc". El DAG está diseñado para ejecutar las transformaciones en un orden específico, por lo que es importante no modificar las dependencias entre tareas sin entender completamente el impacto en el proceso de transformación. Todas las tareas de transformación tienen un timeout de 30 minutos, pero si los datos son muy grandes, puede ser necesario aumentar este timeout. Es importante verificar que las credenciales de GCP tengan los permisos necesarios para leer desde el dataset de bronze y escribir en los datasets de silver y gold. El modelo dbt "idc_processed_data" debe estar correctamente configurado en el proyecto dbt y debe referenciar correctamente las tablas y variables definidas en "config.yaml".

