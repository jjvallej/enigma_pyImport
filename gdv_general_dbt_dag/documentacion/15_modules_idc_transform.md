# Documentación: modules/idc/idc_transform.py

## 1. Información General

El archivo "modules/idc/idc_transform.py" es un módulo Python que se encarga de transformar datos de IDC desde la capa bronze a la capa silver usando BigQuery directamente mediante queries SQL. Este módulo reemplaza las transformaciones de dbt con queries SQL directas para mejor rendimiento y control. A diferencia de "ipm_transform.py" que delega todo a dbt, este módulo ejecuta las transformaciones directamente en BigQuery usando la API de Python, lo cual permite mayor control sobre el proceso y mejor rendimiento para transformaciones complejas. Su propósito principal es aplicar todas las transformaciones necesarias a los datos de IDC en una secuencia de pasos bien definida.

## 2. Propósito y Funcionalidad

El archivo "idc_transform.py" cumple varias funciones críticas en el proceso de transformación. En primer lugar, ejecuta queries SQL en BigQuery con logging detallado y manejo de errores. En segundo lugar, aplica una secuencia de transformaciones a cada tipo de tabla: normaliza nombres de columnas de formato con guiones a formato snake_case, convierte columnas a minúsculas, normaliza la columna departamento a mayúsculas sin acentos, reemplaza valores NULL y NaN por cero en columnas numéricas, y redondea valores a 2 decimales o los convierte a enteros según el tipo de tabla. Finalmente, combina todas las transformaciones en una sola query optimizada para mejor rendimiento usando la función "transform_table_complete".

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports, configuración, y la función "execute_query" que es el helper principal para ejecutar queries. La segunda sección contiene funciones de validación que verifican que las transformaciones funcionan sin crear tablas intermedias. La tercera sección contiene funciones de transformación paso a paso: "normalize_columns", "lowercase_columns", "uppercase_departamento", "fill_nulls", "round_decimals", y "round_integers". La cuarta sección contiene "transform_table_complete" que combina todas las transformaciones en una sola query. Finalmente, la quinta sección contiene "transform_table" que ejecuta todas las transformaciones en secuencia.

## 4. Función execute_query

La función "execute_query" ejecuta una query SQL en BigQuery y espera su completación. La función toma tres parámetros: "query" que es la query SQL, "description" que es una descripción para logging, y "timeout" que es el timeout en segundos con valor por defecto de 1800 segundos. La función obtiene el cliente de BigQuery, registra el inicio de la ejecución, configura el job con "use_legacy_sql" como "False" y el timeout especificado, ejecuta la query, espera a que termine, verifica errores y lanza excepción si los hay, y registra la completación con información sobre filas procesadas. Esta función centraliza el manejo de queries y proporciona logging consistente.

## 5. Funciones de Validación

Las funciones "normalize_columns_step", "uppercase_departamento_step", y "fill_nulls_step" son funciones de validación que verifican que las transformaciones funcionan correctamente sin crear tablas. Estas funciones ejecutan queries SELECT que aplican las transformaciones pero solo retornan un COUNT, validando que la lógica es correcta sin el overhead de crear tablas intermedias. Estas funciones son útiles para debugging y validación antes de ejecutar las transformaciones completas.

## 6. Función transform_table_complete

La función "transform_table_complete" transforma una tabla completa desde bronze a silver aplicando todas las transformaciones en una sola query optimizada. La función determina si es "valor_ranking" que usa enteros o los otros tipos que usan decimales. Construye una query SQL compleja que combina todas las transformaciones: normaliza nombres de columnas directamente en el SELECT, normaliza departamento a mayúsculas sin acentos, reemplaza NULL y NaN por cero usando expresiones condicionales, y redondea o convierte a enteros según el tipo. Ejecuta la query usando "CREATE OR REPLACE TABLE" para crear la tabla silver directamente. Esta función es más eficiente que ejecutar transformaciones paso a paso porque BigQuery puede optimizar la query completa.

## 7. Funciones de Transformación Paso a Paso

Las funciones "normalize_columns", "lowercase_columns", "uppercase_departamento", "fill_nulls", "round_decimals", y "round_integers" ejecutan transformaciones individuales creando tablas intermedias. Estas funciones son útiles cuando se necesita debugging o cuando se quiere ver el resultado de cada paso. "normalize_columns" convierte guiones a guiones bajos en nombres de columnas. "lowercase_columns" convierte todos los nombres a minúsculas obteniendo el esquema dinámicamente. "uppercase_departamento" normaliza departamento a mayúsculas sin acentos. "fill_nulls" reemplaza NULL y NaN por cero. "round_decimals" redondea a 2 decimales. "round_integers" convierte a enteros para "valor_ranking".

## 8. Función transform_table

La función "transform_table" ejecuta todas las transformaciones para una tabla específica en secuencia. La función llama a "normalize_columns", luego "lowercase_columns", luego "uppercase_departamento", luego "fill_nulls", y finalmente "round_decimals" o "round_integers" según el tipo de tabla. Esta función proporciona una interfaz simple para ejecutar todas las transformaciones, pero crea tablas intermedias que pueden ser útiles para debugging. Para producción, se recomienda usar "transform_table_complete" que es más eficiente.

## 9. Transformaciones Aplicadas

Las transformaciones aplicadas incluyen normalización de nombres de columnas de formato "INS-1-1" a "ins_1_1", conversión a minúsculas, normalización de departamento eliminando acentos y convirtiendo a mayúsculas, reemplazo de NULL y NaN por cero en columnas numéricas, y redondeo a 2 decimales para "dato_original" y "valor_normalizado" o conversión a enteros para "valor_ranking". Estas transformaciones preparan los datos para análisis mientras preservan la información esencial.

## 10. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IDC que orquestan el proceso de transformación. Los DAGs importan las funciones de transformación y las llaman para cada tipo de tabla: "dato_original", "valor_normalizado", y "valor_ranking". Se recomienda usar "transform_table_complete" para mejor rendimiento, pero "transform_table" está disponible para casos donde se necesite ver resultados intermedios. Las funciones manejan automáticamente la creación de tablas y el logging detallado.

## 11. Notas Importantes

Este módulo ejecuta transformaciones directamente en BigQuery en lugar de usar dbt, lo cual proporciona mayor control pero requiere que las queries SQL estén hardcodeadas en el código. Si la estructura de los datos cambia, las queries necesitan actualizarse manualmente. El módulo está optimizado para el esquema específico de IDC con sus muchas columnas de indicadores. La función "transform_table_complete" es preferida para producción porque es más eficiente, pero las funciones paso a paso son útiles para desarrollo y debugging. El módulo asume que las tablas bronze ya existen y tienen la estructura esperada.

---

Última actualización: 2025-01-XX  
Archivo documentado: "modules/idc/idc_transform.py"  
Versión del archivo: 3.0

