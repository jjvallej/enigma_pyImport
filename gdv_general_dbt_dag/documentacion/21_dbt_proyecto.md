# Documentación: dbt - Proyecto de Transformaciones

## 1. Información General

La carpeta "dbt" contiene el proyecto completo de dbt que se encarga de todas las transformaciones de datos desde la capa bronze hacia las capas silver y gold en BigQuery. Este proyecto utiliza dbt como motor de transformación para aplicar reglas de negocio, normalizaciones, validaciones y agregaciones que convierten los datos raw en estructuras analíticas listas para consumo.

El propósito principal de este proyecto dbt es centralizar toda la lógica de transformación de datos en un solo lugar, utilizando SQL como lenguaje de transformación y aprovechando las capacidades de dbt para gestionar dependencias, versionar transformaciones, ejecutar pruebas y documentar el proceso de transformación. El proyecto está completamente parametrizado para funcionar en diferentes ambientes y proyectos de GCP sin necesidad de modificar el código SQL.

## 2. Estructura del Proyecto

El proyecto dbt está organizado en varias carpetas principales que siguen las convenciones estándar de dbt. La carpeta "models" contiene todos los modelos SQL organizados por capas de datos. La carpeta "macros" contiene macros personalizadas que extienden las funcionalidades de dbt. La carpeta "dbt_internal_packages" contiene los paquetes internos de dbt necesarios para el funcionamiento del proyecto. La carpeta "target" contiene los artefactos generados por dbt durante la compilación y ejecución, incluyendo SQL compilado, manifiestos y resultados de ejecución. La carpeta "logs" contiene los archivos de log de dbt.

## 3. Archivos de Configuración Principales

El archivo "dbt_project.yml" es el archivo de configuración principal del proyecto dbt. Este archivo define el nombre del proyecto, la versión, los paths donde dbt debe buscar diferentes tipos de archivos, las variables globales del proyecto, y las configuraciones específicas para cada capa de modelos. Las variables definidas en este archivo incluyen "project_id", "bronze_dataset", "silver_dataset" y "gold_dataset", que se configuran automáticamente desde "config.yaml" mediante el argumento "--vars" en los comandos dbt ejecutados por los DAGs de Airflow. También incluye variables específicas para los nombres de tablas de Evaplan en gold, incluyendo las nuevas tablas "evaplan_gold_avance_subprogramas_table_name", "evaplan_gold_avance_programas_table_name", y las tres tablas FACT: "evaplan_gold_fact_entidad_table_name", "evaplan_gold_fact_programa_table_name", y "evaplan_gold_fact_resumen_table_name".

El archivo "profiles.yml" define los perfiles de conexión a BigQuery para diferentes ambientes. Este archivo contiene tres perfiles principales: "dev" para desarrollo, "local" para desarrollo local, y "prod" para producción. Cada perfil especifica el tipo de adaptador, el método de autenticación, el proyecto de GCP, el dataset por defecto, el número de threads, el timeout y la ubicación. Todos los valores se configuran dinámicamente mediante variables de entorno que se establecen cuando los DAGs ejecutan comandos dbt.

## 4. Organización de Modelos por Capas

Los modelos SQL están organizados en tres capas principales dentro de la carpeta "models": "bronze", "silver" y "gold". La capa "bronze" contiene modelos que referencian las tablas raw cargadas desde GCS, aunque actualmente la mayoría de las transformaciones comienzan directamente desde las tablas bronze sin modelos intermedios. La capa "silver" contiene todos los modelos de transformación que procesan los datos desde bronze, aplicando normalizaciones, limpiezas y validaciones. La capa "gold" contiene los modelos finales que crean las estructuras analíticas listas para consumo, típicamente tablas o vistas que unen múltiples fuentes o aplican agregaciones.

## 5. Modelos en la Capa Silver

Los modelos en la capa "silver" están organizados por fuente de datos. Para IPM, existen múltiples modelos que ejecutan transformaciones secuenciales: "ipm_transform_stg" crea una vista inicial, "ipm_transform_normalize_text" normaliza texto, "ipm_transform_transform_types" transforma tipos de datos, "ipm_transform_clean_numbers" limpia números, "ipm_transform_detect_negatives" detecta valores negativos, "ipm_transform_apply_validations" aplica validaciones, y "ipm_transform_clean" crea la tabla final limpia. Para IPM SISBEN, existe el modelo "ipm_sisben_stg" que realiza las transformaciones iniciales. Para IDC, existen modelos que procesan las tres tablas en paralelo, aplicando normalizaciones de columnas, conversiones a minúsculas, normalizaciones de departamento, reemplazo de nulos y redondeo de decimales o conversión a enteros. Para IDI, existen modelos que transforman los años individualmente y luego los consolidan: "idi_transformed_data_2023", "idi_transformed_data_2024" e "idi_transformed_data_consolidated". Todos los modelos en silver están configurados como vistas mediante la configuración en "dbt_project.yml".

## 6. Modelos en la Capa Gold

Los modelos en la capa "gold" están organizados por fuente de datos y crean las estructuras finales analíticas. Para IPM, el modelo "ipm_processed_data" crea la tabla final procesada. Para IPM SISBEN, el modelo "ipm_sisben_processed_data" crea la vista final con descripciones de códigos. Para IDC, el modelo "idc_processed_data" crea la tabla "FACT_IDC" que une las tres tablas transformadas de silver con el diccionario "DIM_IDC" de gold. Para IDI, el modelo "idi_processed_data" crea una vista que apunta a la tabla consolidada de silver. Para Evaplan, existen seis modelos que crean tablas que unen la tabla de periodos con cada tabla de avance: "evaplan_api_avance_mr_processed_data", "evaplan_api_avance_mp_processed_data", "evaplan_api_avance_x_subprograma_processed_data", "evaplan_api_avance_general_processed_data", "evaplan_api_avance_subprogramas_processed_data", y "evaplan_api_avance_programas_processed_data". Además, existen tres modelos FACT que crean tablas de hechos agregadas: "FACT_ENTIDAD" que agrega datos de MP y MR por entidad y año, "FACT_PROGRAMA" que agrega datos de programas y subprogramas, y "FACT_RESUMEN" que consolida métricas clave de Evaplan. Todos los modelos en gold están configurados como tablas mediante la configuración en "dbt_project.yml", excepto las vistas de IDI que son vistas.

## 7. Descripción Detallada de Modelos

Esta sección proporciona una descripción detallada de cada modelo en las capas silver y gold, organizados por fuente de datos.

### 7.1 Modelos Silver - IPM

Los modelos de IPM ejecutan transformaciones secuenciales desde la tabla bronze ipm_raw_data:

- ipm_transform_stg: Modelo staging (ephemeral) que lee de bronze y normaliza nombres de columnas a snake_case. Convierte nombres como "IPM_Pobre_Abs" a "ipm_pobre_abs" y "Municipio" a "municipio". Este es el primer paso en la cadena de transformación.

- ipm_transform_normalize_text: Modelo ephemeral que normaliza texto eliminando acentos y convirtiendo a mayúsculas. Aplica transformaciones a cod_mpio y municipio para estandarizar los nombres, eliminando caracteres especiales y acentos.

- ipm_transform_transform_types: Vista que transforma tipos de datos desde STRING (como vienen de bronze) a tipos apropiados. Convierte valores absolutos a INT64 y porcentajes a FLOAT64 con 2 decimales. Usa SAFE_CAST para manejar valores inválidos que se convertirán en NULL.

- ipm_transform_clean_numbers: Vista que limpia números, manejando valores NULL, espacios, y caracteres no numéricos. Aplica validaciones y correcciones a los valores numéricos.

- ipm_transform_detect_negatives: Vista que detecta y maneja valores negativos, aplicando lógica de negocio para identificar y corregir valores que no deberían ser negativos.

- ipm_transform_apply_validations: Vista que aplica validaciones de negocio, verificando rangos válidos, relaciones entre columnas, y reglas de integridad de datos.

- ipm_transform_clean: Tabla final que materializa los datos transformados. Convierte fecha_lectura a DATE y crea la tabla ipm_transformed_data con todos los datos limpios y validados listos para análisis.

### 7.2 Modelos Silver - IPM SISBEN

- ipm_sisben_stg: Vista que transforma los datos de IPM SISBEN desde la tabla externa bronze. Agrega columnas de descripción (DESCRIPCION) para códigos numéricos, utilizando la nomenclatura prefijada de la tabla BigQuery. Por ejemplo, convierte códigos de clase ("1", "2", "3") a descripciones ("CABECERA", "CENTRO POBLADO", "RURAL DISPERSO"). Crea la tabla ipm_sisben_transformed_data con todas las transformaciones aplicadas.

### 7.3 Modelos Silver - IDC

Los modelos de IDC procesan las tres tablas bronze en paralelo (dato_original, valor_normalizado, valor_ranking), aplicando las mismas transformaciones a cada una:

- idc_normalize_columns_dato_original, idc_normalize_columns_valor_normalizado, idc_normalize_columns_valor_ranking: Tablas que normalizan nombres de columnas a formato snake_case. Convierte columnas como "INS-1-1" a "INS_1_1" y "Año_IDC" a "ano_idc", reemplazando guiones por guiones bajos.

- idc_lowercase_columns_dato_original, idc_lowercase_columns_valor_normalizado, idc_lowercase_columns_valor_ranking: Tablas que convierten todos los nombres de columnas a minúsculas, obteniendo el esquema dinámicamente y aplicando la transformación.

- idc_uppercase_dato_original, idc_uppercase_valor_normalizado, idc_uppercase_valor_ranking: Tablas que normalizan la columna departamento a mayúsculas sin acentos ni caracteres especiales. Usa expresiones regulares para eliminar acentos y convertir a mayúsculas.

- idc_fill_nulls_dato_original, idc_fill_nulls_valor_normalizado, idc_fill_nulls_valor_ranking: Tablas que reemplazan valores NULL y NaN por cero en columnas numéricas, asegurando que no haya valores nulos en los datos.

- idc_round_decimals_dato_original, idc_round_decimals_valor_normalizado: Tablas que redondean valores decimales a 2 decimales para las tablas de dato original y valor normalizado.

- idc_round_integers_valor_ranking: Tabla que convierte valores a enteros para la tabla de valor ranking, ya que los rankings son valores enteros.

### 7.4 Modelos Silver - IDI

Los modelos de IDI procesan datos por año y luego los consolidan:

- idi_transformed_data_2023: Tabla que transforma los datos de IDI del año 2023 desde la tabla bronze idi_raw_data_territorio_2023. Aplica normalizaciones de columnas, limpieza de datos, y validaciones específicas para ese año.

- idi_transformed_data_2024: Tabla que transforma los datos de IDI del año 2024 desde la tabla bronze idi_raw_data_territorio_2024. Aplica las mismas transformaciones que el modelo de 2023 pero para los datos del año 2024.

- idi_transformed_data_consolidated: Tabla consolidada que une los datos de todos los años disponibles (2023 y 2024) usando UNION ALL. Normaliza el campo departamento eliminando acentos y convirtiendo a mayúsculas para ambos años, y agrega una columna anio para identificar el año de cada registro. Los datos se ordenan por año descendente y departamento.

### 7.5 Modelos Gold - IPM

- ipm_processed_data: Tabla final que crea la estructura analítica FACT_DANE. Consume la tabla final de silver (ipm_transform_clean) y aplica transformaciones finales: convierte nombres de columnas a mayúsculas, elimina columnas de porcentajes y sufijos "_abs", y elimina la columna fecha_lectura. La estructura final contiene solo valores absolutos en mayúsculas, optimizada para dashboards y reportes.

### 7.6 Modelos Gold - IPM SISBEN

- ipm_sisben_processed_data: Vista final que crea la estructura FACT_SISBEN. Selecciona columnas de descripción (texto) y valores numéricos de la capa silver para análisis final. Incluye identificadores básicos (ficha, códigos de departamento y municipio), descripciones de servicios (tipo de vivienda, servicios públicos, etc.), y valores numéricos del SISBEN (grupo, nivel, puntajes). Esta vista proporciona datos listos para consumo con todas las descripciones aplicadas.

### 7.7 Modelos Gold - IDC

- idc_processed_data: Tabla final que crea la estructura FACT_IDC. Une las tres tablas transformadas de silver (idc_round_decimals_dato_original, idc_round_decimals_valor_normalizado, idc_round_integers_valor_ranking) con el diccionario DIM_IDC de gold. Usa UNPIVOT para convertir las columnas de indicadores (INS-1-1, INF-1-1, etc.) en filas, creando una estructura normalizada con columnas: DEPARTAMENTO, ANIO, ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, VALOR_ORIGINAL, VALOR_NORMALIZADO, VALOR_RANKING. Hace JOIN con DIM_IDC usando ID_SUBINDICADOR para obtener las descripciones y metadatos de cada indicador.

### 7.8 Modelos Gold - IDI

- idi_processed_data: Vista que apunta directamente a la tabla consolidada de silver (idi_transformed_data_consolidated). Crea la estructura IDI_PROCESSED_DATA que simplemente selecciona todos los datos de la tabla consolidada, proporcionando una interfaz consistente en gold para acceder a los datos consolidados de IDI.

### 7.9 Modelos Gold - Evaplan (Processed Data)

Los modelos de Evaplan unen la tabla de periodos con cada tabla de avance, enriqueciendo los datos de avance con información del periodo:

- evaplan_api_avance_mr_processed_data: Tabla que une la tabla de periodos con la tabla de avance de Metas de Resultado (MR). Hace JOIN por peri_idp para tener toda la información del periodo (nombre, fechas de apertura y cierre, etc.) junto con los datos de avance MR.

- evaplan_api_avance_mp_processed_data: Tabla que une la tabla de periodos con la tabla de avance de Metas de Producto (MP). Similar al modelo anterior pero para datos de MP.

- evaplan_api_avance_x_subprograma_processed_data: Tabla que une la tabla de periodos con la tabla de avance por subprograma. Proporciona datos de avance desagregados por subprograma con información del periodo.

- evaplan_api_avance_general_processed_data: Tabla que une la tabla de periodos con la tabla de avance general. Proporciona una vista general del avance con información del periodo.

- evaplan_api_avance_subprogramas_processed_data: Tabla que une la tabla de periodos con la tabla de avance de subprogramas. Proporciona datos agregados de subprogramas con información del periodo.

- evaplan_api_avance_programas_processed_data: Tabla que une la tabla de periodos con la tabla de avance de programas. Proporciona datos agregados de programas con información del periodo.

### 7.10 Modelos Gold - Evaplan (FACT Tables)

Los modelos FACT de Evaplan crean tablas de hechos agregadas optimizadas para análisis:

- evaplan_api_processed_data_fact_entidad: Tabla FACT_ENTIDAD que agrega datos de Metas de Producto (MP) y Metas de Resultado (MR) por entidad y año. Incluye métricas de avance, eficacia, eficiencia y efectividad. Agrupa por anio, clasificacion (Plan de Acción o Período de Gobierno), y entidad_dependencia (concatenación de código y nombre de entidad). Calcula métricas agregadas como promedios de avance, eficacia, eficiencia y efectividad, y cuenta de metas programadas y ejecutadas.

- evaplan_api_processed_data_fact_programa: Tabla FACT_PROGRAMA que agrega datos de programas y subprogramas, incluyendo métricas de avance y ponderación. Selecciona datos de la tabla evaplan_api_avance_x_subprograma_processed_data y crea columnas concatenadas para PROGRAMA (código - nombre) y SUBPROGRAMA (código con formato 00 - nombre). Incluye métricas como cantidad de MP por subprograma, promedio de avance, avance ponderado, ponderación del subprograma, aporte al cumplimiento, y avance ponderado del programa.

- evaplan_api_processed_data_fact_resumen: Tabla FACT_RESUMEN que consolida y resume métricas clave de Evaplan (MP, MR, Subprogramas, Programas, Líneas Estratégicas) por año, clasificación e ítem. Usa UNION ALL para combinar datos de diferentes fuentes (MP, MR, Subprogramas, Programas, Líneas Estratégicas) y luego agrupa por anio, clasificacion, e item para calcular métricas agregadas. Incluye métricas como cantidad de registros, promedios de avance, eficacia, eficiencia y efectividad, y cuenta de registros programados y ejecutados.

## 8. Definición de Fuentes

El archivo "sources.yml" define todas las fuentes de datos que los modelos dbt pueden referenciar usando la función "source()". Este archivo organiza las fuentes por nombre lógico y especifica el schema y las tablas disponibles. Las fuentes definidas incluyen "bronze_ipmv2" con la tabla "ipm_raw_data", "bronze_idc" con las tres tablas de IDC, "bronze_evaplan" con las siete tablas de Evaplan (periodos, avance_mr, avance_mp, avance_x_subprograma, avance_general, avance_subprogramas, y avance_programas), "bronze_ipm_sisben" con la tabla de IPM SISBEN, "bronze_idi" con las tablas de IDI por año, "silver_evaplan" con las siete tablas transformadas de Evaplan, "silver_ipm_sisben" con la tabla transformada de IPM SISBEN, "gold_idc" con la tabla dimensional "DIM_IDC", y "gold_evaplan" con las seis tablas processed_data de Evaplan. El uso de fuentes permite que dbt valide las dependencias y genere documentación automática sobre el linaje de datos.

## 9. Macros Personalizadas

La carpeta "macros" contiene macros personalizadas que extienden las funcionalidades de dbt. El archivo "generate_schema_name.sql" define una macro que personaliza cómo dbt genera los nombres de schemas. Esta macro permite que los modelos especifiquen schemas personalizados o usen el schema por defecto del target según sea necesario. Las macros personalizadas pueden ser reutilizadas en múltiples modelos, reduciendo la duplicación de código y facilitando el mantenimiento.

## 10. Integración con el Sistema

El proyecto dbt se integra con el resto del sistema a través de los DAGs de Airflow que ejecutan comandos dbt. Los DAGs utilizan la función "get_dbt_command" del módulo "modules.config" para generar comandos dbt completos con todas las variables de entorno y parámetros necesarios. Los comandos dbt se ejecutan mediante "BashOperator" en los DAGs, pasando variables mediante el argumento "--vars" y estableciendo variables de entorno para que dbt pueda acceder a la configuración del proyecto, datasets y ubicación de GCP. Esta integración asegura que dbt siempre use la configuración correcta según el ambiente activo, sin necesidad de modificar el código SQL.

## 11. Variables y Parametrización

El proyecto dbt está completamente parametrizado para funcionar en diferentes ambientes y proyectos de GCP. Las variables "project_id", "bronze_dataset", "silver_dataset" y "gold_dataset" se pasan desde los DAGs mediante el argumento "--vars", sobrescribiendo los valores por defecto definidos en "dbt_project.yml". Las variables de entorno "DBT_PROJECT_ID", "DBT_DATASET_BRONZE", "DBT_DATASET_SILVER", "DBT_DATASET_GOLD" y "DBT_LOCATION" se establecen cuando los DAGs ejecutan comandos dbt, permitiendo que "profiles.yml" y "dbt_project.yml" accedan a estos valores mediante "env_var()". Esta doble capa de parametrización asegura que el proyecto sea completamente portable entre diferentes ambientes y proyectos sin modificar código.

## 12. Materialización de Modelos

Los modelos en la capa "silver" están configurados para materializarse como vistas mediante la configuración "+materialized: view" en "dbt_project.yml". Esto significa que cuando dbt ejecuta estos modelos, crea vistas en BigQuery en lugar de tablas, lo que es eficiente en términos de almacenamiento y asegura que siempre reflejen los datos más recientes de las tablas bronze. Los modelos en la capa "gold" están configurados para materializarse como tablas mediante la configuración "+materialized: table" en "dbt_project.yml", excepto algunos modelos específicos como los de Evaplan e IDI que son vistas. Las tablas en gold proporcionan mejor rendimiento para consultas complejas y permiten agregaciones pre-calculadas.

## 13. Dependencias entre Modelos

Los modelos dbt definen dependencias entre sí mediante las funciones "ref()" y "source()". La función "ref()" se usa para referenciar otros modelos dbt, y dbt automáticamente construye un grafo de dependencias que determina el orden de ejecución. La función "source()" se usa para referenciar tablas definidas en "sources.yml", permitiendo que dbt valide que las tablas existen antes de ejecutar los modelos. Este sistema de dependencias asegura que los modelos se ejecuten en el orden correcto y que dbt pueda detectar problemas de dependencias antes de la ejecución.

## 14. Paquetes Internos de dbt

La carpeta "dbt_internal_packages" contiene los paquetes internos de dbt necesarios para el funcionamiento del proyecto. Estos paquetes incluyen "dbt-adapters" que proporciona funcionalidades base del adaptador, y "dbt-bigquery" que proporciona funcionalidades específicas para BigQuery, incluyendo macros de materialización, relaciones y utilidades. Estos paquetes son instalados automáticamente por dbt y no requieren mantenimiento manual, pero son necesarios para que el proyecto funcione correctamente.

## 15. Artefactos Generados

La carpeta "target" contiene todos los artefactos generados por dbt durante la compilación y ejecución. La carpeta "compiled" contiene el SQL compilado de todos los modelos, que es el SQL final que se ejecuta en BigQuery después de resolver todas las referencias, variables y macros. La carpeta "run" contiene el SQL que se ejecutó en la última ejecución. El archivo "manifest.json" contiene el manifiesto completo del proyecto, incluyendo todos los modelos, fuentes, macros y sus dependencias. El archivo "run_results.json" contiene los resultados de la última ejecución, incluyendo el estado de cada modelo. Estos artefactos pueden ser eliminados usando el comando "dbt clean" y se regeneran automáticamente en la siguiente ejecución.

## 16. Uso en el Código

El proyecto dbt se utiliza desde los DAGs de transformación de Airflow, que ejecutan comandos dbt para transformar los datos desde bronze hacia silver y gold. Los DAGs utilizan "BashOperator" para ejecutar comandos dbt generados por "get_dbt_command", que incluyen el comando específico como "dbt run --select nombre_modelo", el directorio del proyecto, y todos los argumentos y variables de entorno necesarios. Los modelos se ejecutan en un orden específico definido por las dependencias, y los DAGs pueden ejecutar modelos individuales o grupos de modelos según sea necesario.

## 17. Notas Importantes

Es importante entender que el proyecto dbt está completamente parametrizado y no contiene valores hardcodeados de proyectos, datasets o ubicaciones. Todos estos valores se pasan desde "config.yaml" a través de los DAGs, lo que permite que el mismo código funcione en diferentes ambientes sin modificaciones. Los modelos en silver están materializados como vistas, lo que significa que siempre reflejan los datos más recientes de bronze, pero puede tener un impacto en el rendimiento de consultas complejas. Los modelos en gold están materializados como tablas, lo que proporciona mejor rendimiento pero requiere que se ejecuten para actualizar los datos. El proyecto utiliza la función "source()" para referenciar tablas bronze y silver, lo que permite que dbt valide las dependencias y genere documentación automática. Los artefactos en la carpeta "target" pueden ser eliminados con "dbt clean" y se regeneran automáticamente, por lo que no es necesario versionarlos en el control de versiones. El proyecto está diseñado para ser ejecutado desde Airflow en ambientes de Cloud Composer, pero también puede ejecutarse localmente configurando las variables de entorno apropiadas.

---

Última actualización: 2025-12-15  
Archivo documentado: "dbt/" (proyecto completo)  
Versión del archivo: 3.0

