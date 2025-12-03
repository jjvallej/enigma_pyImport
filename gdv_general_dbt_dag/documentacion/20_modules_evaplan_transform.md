# Documentación: modules/evaplan/evaplan_transform.py

## 1. Información General

El archivo "modules/evaplan/evaplan_transform.py" es un módulo Python que se encarga de transformar datos de Evaplan desde la capa bronze a la capa silver. Este módulo implementa una lógica específica donde primero obtiene los "peri_idp" únicos de las tablas bronze de la fecha actual, luego elimina registros existentes con esos "peri_idp" de las tablas silver para evitar duplicados, y finalmente copia los datos transformados de bronze a silver aplicando transformación de nombres de columnas a snake_case. Su propósito principal es mantener los datos silver sincronizados con bronze, eliminando versiones anteriores de los mismos "peri_idp" antes de cargar nuevas versiones.

## 2. Propósito y Funcionalidad

El archivo "evaplan_transform.py" cumple varias funciones importantes. En primer lugar, obtiene los nombres de tablas bronze y silver desde "config.yaml" para cada fuente. En segundo lugar, obtiene los "peri_idp" únicos de las tablas bronze de la fecha actual, consultando cada tabla bronze y extrayendo los "peri_idp" distintos. En tercer lugar, para cada tabla silver, elimina registros existentes que tengan esos "peri_idp" para evitar duplicados cuando se cargan nuevos datos. En cuarto lugar, copia los datos de bronze a silver aplicando transformación de nombres de columnas a snake_case usando la función "to_snake_case", manteniendo los tipos de datos apropiados. Finalmente, maneja el caso especial de "periodos" que no tiene "peri_idp" eliminando todos los registros del mismo día.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports, configuración, y funciones para obtener nombres de tablas desde "config.yaml". La segunda sección contiene funciones helper como "ensure_dataset", "to_snake_case", y "table_exists". La tercera sección contiene funciones para obtener "peri_idp" como "get_peri_idps_from_bronze_table" y "get_all_peri_idps_from_bronze". La cuarta sección contiene funciones de transformación como "delete_records_by_peri_idp", "copy_bronze_to_silver", y "transform_fuente_to_silver". Cada función tiene un propósito específico en el proceso de transformación.

## 4. Función to_snake_case

La función "to_snake_case" convierte un nombre de columna a minúsculas y normaliza separadores, pero NO inserta guiones bajos adicionales entre letras. La función reemplaza espacios y guiones con guiones bajos, convierte todo a minúsculas, limpia guiones bajos múltiples, y elimina guiones bajos al inicio y final. Esta función es diferente de otras implementaciones de snake_case porque mantiene la estructura original del nombre, solo normalizando separadores existentes. Por ejemplo, "CODIGO_LINEA" se convierte a "codigo_linea" pero "CodigoLinea" se convierte a "codigolinea" sin insertar guiones bajos.

## 5. Función get_peri_idps_from_bronze_table

La función "get_peri_idps_from_bronze_table" obtiene los "peri_idp" únicos de una tabla bronze de la fecha actual. La función verifica que la tabla exista, verifica que tenga columna "peri_idp" porque "periodos" puede no tenerla, ejecuta una query SQL que selecciona "peri_idp" distintos de la fecha actual, y retorna un set de "peri_idp" únicos. Esta función es importante porque identifica qué "peri_idp" necesitan ser procesados y eliminados de silver antes de cargar nuevos datos.

## 6. Función delete_records_by_peri_idp

La función "delete_records_by_peri_idp" elimina registros de una tabla silver que tengan los "peri_idp" especificados. La función verifica que la tabla exista, construye una query DELETE que filtra por "peri_idp" usando IN con la lista de "peri_idp", y ejecuta la query. Esta función es importante para evitar duplicados cuando se cargan nuevos datos con los mismos "peri_idp", asegurando que solo haya una versión de cada "peri_idp" en silver.

## 7. Función copy_bronze_to_silver

La función "copy_bronze_to_silver" copia datos de una tabla bronze a su tabla silver correspondiente, transformando nombres de columnas a snake_case. La función verifica que la tabla bronze exista, construye una query SELECT que filtra por fecha actual y opcionalmente por "peri_idp" específicos, lee los datos usando "to_dataframe", transforma nombres de columnas usando "to_snake_case", verifica si la tabla silver existe y ajusta columnas si es necesario, genera esquema de BigQuery desde el DataFrame preservando tipos apropiados, y carga el DataFrame a BigQuery usando "WRITE_APPEND" si la tabla existe o "WRITE_TRUNCATE" si no existe. Esta función maneja toda la lógica de copia y transformación.

## 8. Función transform_fuente_to_silver

La función "transform_fuente_to_silver" es la función principal que transforma una fuente completa de bronze a silver. La función obtiene los "peri_idp" de bronze si no se proporcionan, elimina registros existentes con esos "peri_idp" de silver, y copia los nuevos datos de bronze a silver. Para "periodos" que no tiene "peri_idp", elimina todos los registros del mismo día antes de copiar. Esta función proporciona una interfaz simple para ejecutar todo el proceso de transformación para una fuente.

## 9. Lógica de Eliminación y Carga

El módulo implementa una lógica específica de eliminación antes de carga para evitar duplicados. Primero identifica qué "peri_idp" están en bronze de la fecha actual. Luego elimina esos "peri_idp" de silver si existen. Finalmente, copia los nuevos datos de bronze a silver. Esta lógica asegura que cada "peri_idp" tenga solo una versión en silver, la más reciente de la fecha actual. Para "periodos" que no tiene "peri_idp", se eliminan todos los registros del mismo día antes de copiar.

## 10. Manejo de Esquemas

El módulo maneja esquemas de manera inteligente. Si la tabla silver ya existe, obtiene el esquema existente y verifica que las columnas del DataFrame coincidan. Si hay diferencias, usa solo las columnas comunes para evitar errores. Si la tabla no existe, genera el esquema desde el DataFrame preservando tipos apropiados: TIMESTAMP para fechas, INT64 para "peri_idp", FLOAT64 para números decimales, BOOL para booleanos, y STRING para todo lo demás. Esta flexibilidad permite que el módulo maneje cambios en la estructura de datos.

## 11. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de Evaplan que orquestan el proceso de transformación. Los DAGs llaman a "transform_fuente_to_silver" para cada fuente, pasando el nombre de la fuente. La función maneja automáticamente la obtención de "peri_idp", la eliminación de registros existentes, y la copia de nuevos datos. Los DAGs pueden llamar a la función para cada fuente en paralelo o secuencialmente según la configuración.

## 12. Notas Importantes

El módulo asume que los datos en bronze de la fecha actual son la versión correcta y que las versiones anteriores en silver deben ser reemplazadas. Esta lógica es apropiada para datos que se actualizan diariamente pero puede no ser apropiada para datos históricos que deben acumularse. El módulo transforma nombres de columnas a snake_case pero mantiene los tipos de datos apropiados, lo cual es importante para análisis posteriores. La función "to_snake_case" no inserta guiones bajos adicionales, lo cual puede resultar en nombres como "codigolinea" en lugar de "codigo_linea" si el nombre original no tenía separadores. El módulo maneja automáticamente diferencias en esquemas entre bronze y silver, usando solo columnas comunes si hay diferencias.

---

Última actualización: 2025-01-XX  
Archivo documentado: "modules/evaplan/evaplan_transform.py"  
Versión del archivo: 3.0

