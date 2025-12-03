# Documentación: dbt - Proyecto de Transformaciones

## 1. Información General

La carpeta "dbt" contiene el proyecto completo de dbt que se encarga de todas las transformaciones de datos desde la capa bronze hacia las capas silver y gold en BigQuery. Este proyecto utiliza dbt como motor de transformación para aplicar reglas de negocio, normalizaciones, validaciones y agregaciones que convierten los datos raw en estructuras analíticas listas para consumo.

El propósito principal de este proyecto dbt es centralizar toda la lógica de transformación de datos en un solo lugar, utilizando SQL como lenguaje de transformación y aprovechando las capacidades de dbt para gestionar dependencias, versionar transformaciones, ejecutar pruebas y documentar el proceso de transformación. El proyecto está completamente parametrizado para funcionar en diferentes ambientes y proyectos de GCP sin necesidad de modificar el código SQL.

## 2. Estructura del Proyecto

El proyecto dbt está organizado en varias carpetas principales que siguen las convenciones estándar de dbt. La carpeta "models" contiene todos los modelos SQL organizados por capas de datos. La carpeta "macros" contiene macros personalizadas que extienden las funcionalidades de dbt. La carpeta "dbt_internal_packages" contiene los paquetes internos de dbt necesarios para el funcionamiento del proyecto. La carpeta "target" contiene los artefactos generados por dbt durante la compilación y ejecución, incluyendo SQL compilado, manifiestos y resultados de ejecución. La carpeta "logs" contiene los archivos de log de dbt.

## 3. Archivos de Configuración Principales

El archivo "dbt_project.yml" es el archivo de configuración principal del proyecto dbt. Este archivo define el nombre del proyecto, la versión, los paths donde dbt debe buscar diferentes tipos de archivos, las variables globales del proyecto, y las configuraciones específicas para cada capa de modelos. Las variables definidas en este archivo incluyen "project_id", "bronze_dataset", "silver_dataset" y "gold_dataset", que se configuran automáticamente desde "config.yaml" mediante el argumento "--vars" en los comandos dbt ejecutados por los DAGs de Airflow.

El archivo "profiles.yml" define los perfiles de conexión a BigQuery para diferentes ambientes. Este archivo contiene tres perfiles principales: "dev" para desarrollo, "local" para desarrollo local, y "prod" para producción. Cada perfil especifica el tipo de adaptador, el método de autenticación, el proyecto de GCP, el dataset por defecto, el número de threads, el timeout y la ubicación. Todos los valores se configuran dinámicamente mediante variables de entorno que se establecen cuando los DAGs ejecutan comandos dbt.

## 4. Organización de Modelos por Capas

Los modelos SQL están organizados en tres capas principales dentro de la carpeta "models": "bronze", "silver" y "gold". La capa "bronze" contiene modelos que referencian las tablas raw cargadas desde GCS, aunque actualmente la mayoría de las transformaciones comienzan directamente desde las tablas bronze sin modelos intermedios. La capa "silver" contiene todos los modelos de transformación que procesan los datos desde bronze, aplicando normalizaciones, limpiezas y validaciones. La capa "gold" contiene los modelos finales que crean las estructuras analíticas listas para consumo, típicamente tablas o vistas que unen múltiples fuentes o aplican agregaciones.

## 5. Modelos en la Capa Silver

Los modelos en la capa "silver" están organizados por fuente de datos. Para IPM, existen múltiples modelos que ejecutan transformaciones secuenciales: "ipm_transform_stg" crea una vista inicial, "ipm_transform_normalize_text" normaliza texto, "ipm_transform_transform_types" transforma tipos de datos, "ipm_transform_clean_numbers" limpia números, "ipm_transform_detect_negatives" detecta valores negativos, "ipm_transform_apply_validations" aplica validaciones, y "ipm_transform_clean" crea la tabla final limpia. Para IPM SISBEN, existe el modelo "ipm_sisben_stg" que realiza las transformaciones iniciales. Para IDC, existen modelos que procesan las tres tablas en paralelo, aplicando normalizaciones de columnas, conversiones a minúsculas, normalizaciones de departamento, reemplazo de nulos y redondeo de decimales o conversión a enteros. Para IDI, existen modelos que transforman los años individualmente y luego los consolidan: "idi_transformed_data_2023", "idi_transformed_data_2024" e "idi_transformed_data_consolidated". Todos los modelos en silver están configurados como vistas mediante la configuración en "dbt_project.yml".

## 6. Modelos en la Capa Gold

Los modelos en la capa "gold" están organizados por fuente de datos y crean las estructuras finales analíticas. Para IPM, el modelo "ipm_processed_data" crea la tabla final procesada. Para IPM SISBEN, el modelo "ipm_sisben_processed_data" crea la vista final con descripciones de códigos. Para IDC, el modelo "idc_processed_data" crea la tabla "fact_idc" que une las tres tablas transformadas de silver con el diccionario "dim_idc" de gold. Para IDI, el modelo "idi_processed_data" crea una vista que apunta a la tabla consolidada de silver. Para Evaplan, existen cuatro modelos que crean vistas que unen la tabla de periodos con cada tabla de avance: "evaplan_api_avance_mr_processed_data", "evaplan_api_avance_mp_processed_data", "evaplan_api_avance_x_subprograma_processed_data" y "evaplan_api_avance_general_processed_data". Todos los modelos en gold están configurados como tablas mediante la configuración en "dbt_project.yml", excepto las vistas de Evaplan e IDI que son vistas.

## 7. Definición de Fuentes

El archivo "sources.yml" define todas las fuentes de datos que los modelos dbt pueden referenciar usando la función "source()". Este archivo organiza las fuentes por nombre lógico y especifica el schema y las tablas disponibles. Las fuentes definidas incluyen "bronze_ipmv2" con la tabla "ipm_raw_data", "bronze_idc" con las tres tablas de IDC, "bronze_evaplan" con las cinco tablas de Evaplan, "bronze_ipm_sisben" con la tabla de IPM SISBEN, "bronze_idi" con las tablas de IDI por año, "silver_evaplan" con las tablas transformadas de Evaplan, "silver_ipm_sisben" con la tabla transformada de IPM SISBEN, y "gold_idc" con la tabla dimensional "dim_idc". El uso de fuentes permite que dbt valide las dependencias y genere documentación automática sobre el linaje de datos.

## 8. Macros Personalizadas

La carpeta "macros" contiene macros personalizadas que extienden las funcionalidades de dbt. El archivo "generate_schema_name.sql" define una macro que personaliza cómo dbt genera los nombres de schemas. Esta macro permite que los modelos especifiquen schemas personalizados o usen el schema por defecto del target según sea necesario. Las macros personalizadas pueden ser reutilizadas en múltiples modelos, reduciendo la duplicación de código y facilitando el mantenimiento.

## 9. Integración con el Sistema

El proyecto dbt se integra con el resto del sistema a través de los DAGs de Airflow que ejecutan comandos dbt. Los DAGs utilizan la función "get_dbt_command" del módulo "modules.config" para generar comandos dbt completos con todas las variables de entorno y parámetros necesarios. Los comandos dbt se ejecutan mediante "BashOperator" en los DAGs, pasando variables mediante el argumento "--vars" y estableciendo variables de entorno para que dbt pueda acceder a la configuración del proyecto, datasets y ubicación de GCP. Esta integración asegura que dbt siempre use la configuración correcta según el ambiente activo, sin necesidad de modificar el código SQL.

## 10. Variables y Parametrización

El proyecto dbt está completamente parametrizado para funcionar en diferentes ambientes y proyectos de GCP. Las variables "project_id", "bronze_dataset", "silver_dataset" y "gold_dataset" se pasan desde los DAGs mediante el argumento "--vars", sobrescribiendo los valores por defecto definidos en "dbt_project.yml". Las variables de entorno "DBT_PROJECT_ID", "DBT_DATASET_BRONZE", "DBT_DATASET_SILVER", "DBT_DATASET_GOLD" y "DBT_LOCATION" se establecen cuando los DAGs ejecutan comandos dbt, permitiendo que "profiles.yml" y "dbt_project.yml" accedan a estos valores mediante "env_var()". Esta doble capa de parametrización asegura que el proyecto sea completamente portable entre diferentes ambientes y proyectos sin modificar código.

## 11. Materialización de Modelos

Los modelos en la capa "silver" están configurados para materializarse como vistas mediante la configuración "+materialized: view" en "dbt_project.yml". Esto significa que cuando dbt ejecuta estos modelos, crea vistas en BigQuery en lugar de tablas, lo que es eficiente en términos de almacenamiento y asegura que siempre reflejen los datos más recientes de las tablas bronze. Los modelos en la capa "gold" están configurados para materializarse como tablas mediante la configuración "+materialized: table" en "dbt_project.yml", excepto algunos modelos específicos como los de Evaplan e IDI que son vistas. Las tablas en gold proporcionan mejor rendimiento para consultas complejas y permiten agregaciones pre-calculadas.

## 12. Dependencias entre Modelos

Los modelos dbt definen dependencias entre sí mediante las funciones "ref()" y "source()". La función "ref()" se usa para referenciar otros modelos dbt, y dbt automáticamente construye un grafo de dependencias que determina el orden de ejecución. La función "source()" se usa para referenciar tablas definidas en "sources.yml", permitiendo que dbt valide que las tablas existen antes de ejecutar los modelos. Este sistema de dependencias asegura que los modelos se ejecuten en el orden correcto y que dbt pueda detectar problemas de dependencias antes de la ejecución.

## 13. Paquetes Internos de dbt

La carpeta "dbt_internal_packages" contiene los paquetes internos de dbt necesarios para el funcionamiento del proyecto. Estos paquetes incluyen "dbt-adapters" que proporciona funcionalidades base del adaptador, y "dbt-bigquery" que proporciona funcionalidades específicas para BigQuery, incluyendo macros de materialización, relaciones y utilidades. Estos paquetes son instalados automáticamente por dbt y no requieren mantenimiento manual, pero son necesarios para que el proyecto funcione correctamente.

## 14. Artefactos Generados

La carpeta "target" contiene todos los artefactos generados por dbt durante la compilación y ejecución. La carpeta "compiled" contiene el SQL compilado de todos los modelos, que es el SQL final que se ejecuta en BigQuery después de resolver todas las referencias, variables y macros. La carpeta "run" contiene el SQL que se ejecutó en la última ejecución. El archivo "manifest.json" contiene el manifiesto completo del proyecto, incluyendo todos los modelos, fuentes, macros y sus dependencias. El archivo "run_results.json" contiene los resultados de la última ejecución, incluyendo el estado de cada modelo. Estos artefactos pueden ser eliminados usando el comando "dbt clean" y se regeneran automáticamente en la siguiente ejecución.

## 15. Uso en el Código

El proyecto dbt se utiliza desde los DAGs de transformación de Airflow, que ejecutan comandos dbt para transformar los datos desde bronze hacia silver y gold. Los DAGs utilizan "BashOperator" para ejecutar comandos dbt generados por "get_dbt_command", que incluyen el comando específico como "dbt run --select nombre_modelo", el directorio del proyecto, y todos los argumentos y variables de entorno necesarios. Los modelos se ejecutan en un orden específico definido por las dependencias, y los DAGs pueden ejecutar modelos individuales o grupos de modelos según sea necesario.

## 16. Notas Importantes

Es importante entender que el proyecto dbt está completamente parametrizado y no contiene valores hardcodeados de proyectos, datasets o ubicaciones. Todos estos valores se pasan desde "config.yaml" a través de los DAGs, lo que permite que el mismo código funcione en diferentes ambientes sin modificaciones. Los modelos en silver están materializados como vistas, lo que significa que siempre reflejan los datos más recientes de bronze, pero puede tener un impacto en el rendimiento de consultas complejas. Los modelos en gold están materializados como tablas, lo que proporciona mejor rendimiento pero requiere que se ejecuten para actualizar los datos. El proyecto utiliza la función "source()" para referenciar tablas bronze y silver, lo que permite que dbt valide las dependencias y genere documentación automática. Los artefactos en la carpeta "target" pueden ser eliminados con "dbt clean" y se regeneran automáticamente, por lo que no es necesario versionarlos en el control de versiones. El proyecto está diseñado para ser ejecutado desde Airflow en ambientes de Cloud Composer, pero también puede ejecutarse localmente configurando las variables de entorno apropiadas.

