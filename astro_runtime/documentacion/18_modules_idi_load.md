# Documentación: modules/idi/idi_load.py

## 1. Información General

El archivo "modules/idi/idi_load.py" es un módulo Python que se encarga de cargar archivos CSV de IDI desde Google Cloud Storage a BigQuery. Este módulo es diferente de otros módulos de carga porque maneja múltiples años de datos, cada uno almacenado en una carpeta separada en GCS y cargado como una tabla separada en BigQuery. Los archivos CSV de IDI se organizan por año en la estructura "base_folder/año/resultados_año.csv", y el módulo detecta automáticamente todos los años disponibles y los carga todos en una sola ejecución. Su propósito principal es cargar todos los años históricos de IDI a BigQuery de manera eficiente.

## 2. Propósito y Funcionalidad

El archivo "idi_load.py" cumple varias funciones importantes. En primer lugar, asegura que el dataset bronze exista en BigQuery. En segundo lugar, detecta todos los años disponibles en GCS explorando las carpetas dentro de la carpeta base. En tercer lugar, para cada año encontrado, construye la URI del CSV esperado, descarga el CSV desde GCS, y lo carga a BigQuery como una tabla separada con nombre "idi_raw_data_territorio_{año}". Finalmente, proporciona un resumen del procesamiento mostrando qué años se cargaron exitosamente. El módulo está diseñado para procesar múltiples años en una sola ejecución, lo cual es eficiente para cargas iniciales o actualizaciones masivas.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports y configuración. La segunda sección contiene función "ensure_dataset" para crear el dataset. La tercera sección contiene "get_all_years_from_gcs" que detecta todos los años disponibles. La cuarta sección contiene "download_csv_from_gcs" que descarga CSVs desde GCS. La quinta sección contiene "load_csv_to_bq" que carga un CSV a BigQuery. La sexta sección contiene "load_all_years_idi_to_bq" que procesa todos los años. Finalmente, la séptima sección contiene "cleanup_temp_paths" para limpiar archivos temporales.

## 4. Función get_all_years_from_gcs

La función "get_all_years_from_gcs" obtiene todos los años disponibles en GCS explorando la estructura de carpetas. La función lista todos los blobs en la carpeta base, extrae el año de la primera parte del path relativo después de remover el prefijo de la carpeta base, valida que sea un año válido convirtiéndolo a entero, y retorna una lista de años ordenados de más reciente a más antiguo. Esta función es importante porque permite que el módulo procese automáticamente todos los años disponibles sin necesidad de especificarlos manualmente.

## 5. Función load_csv_to_bq

La función "load_csv_to_bq" carga un archivo CSV local a una tabla de BigQuery. La función configura el job de carga con formato CSV, delimitador punto y coma que es el formato usado por "idi_ingest.py", detección automática de esquema, y opción de "WRITE_TRUNCATE" o "WRITE_APPEND" según se especifique. Usa "allow_quoted_newlines" y "allow_jagged_rows" para manejar CSVs con formato variable. Carga el archivo usando "load_table_from_file" y espera a que termine. Retorna el nombre completo de la tabla creada. Esta función es específica para CSVs de IDI que usan punto y coma como delimitador.

## 6. Función load_all_years_idi_to_bq

La función "load_all_years_idi_to_bq" es la función principal que carga todos los años disponibles de IDI a BigQuery. La función obtiene todos los años usando "get_all_years_from_gcs", valida que haya años disponibles, y para cada año construye la URI del CSV esperado en formato "gs://bucket/base_folder/año/resultados_año.csv", descarga el CSV, construye el nombre de la tabla como "{table_prefix}_{año}", carga el CSV a BigQuery, y almacena el resultado. Al final, proporciona un resumen del procesamiento mostrando todos los años procesados. Maneja archivos temporales y los limpia al final en un bloque "finally".

## 7. Estructura de Carpetas y Archivos

Los archivos CSV de IDI se organizan en una estructura jerárquica por año. La carpeta base contiene subcarpetas, una por cada año, y cada subcarpeta contiene un archivo CSV con nombre "resultados_{año}.csv". Por ejemplo, "data_staging/dpt_planeacion_municipal/idi/2024/resultados_2024.csv". Esta estructura permite que el módulo detecte automáticamente todos los años disponibles y los procese de manera organizada.

## 8. Nombres de Tablas

Cada año se carga como una tabla separada en BigQuery con nombre "idi_raw_data_territorio_{año}". Por ejemplo, el año 2024 se carga en la tabla "idi_raw_data_territorio_2024". Este esquema de nombres permite mantener los datos históricos separados por año, lo cual es útil para análisis temporales y para evitar tablas extremadamente grandes. Las transformaciones posteriores en silver pueden unir los años si es necesario.

## 9. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IDI. Los DAGs primero aseguran que el dataset existe, luego llaman a "load_all_years_idi_to_bq" pasando el nombre del bucket desde "DEFAULT_BUCKET_NAME", la carpeta base desde "CONF.idi.gcs_base_folder", el dataset desde "DATASET_ID_BRONZE", y el prefijo de tabla desde "CONF.idi.tables.bronze_prefix". La función procesa todos los años automáticamente y retorna un diccionario con el mapeo de años a nombres de tablas creadas.

## 10. Notas Importantes

El módulo asume que los archivos CSV están organizados en la estructura esperada con carpetas por año. Si la estructura cambia, la función "get_all_years_from_gcs" puede no detectar correctamente los años. Los CSVs deben usar punto y coma como delimitador, que es el formato generado por "idi_ingest.py". El módulo procesa todos los años encontrados, por lo que si hay años antiguos que no se quieren procesar, deben eliminarse de GCS o la función debe modificarse para filtrar años. Cada año se carga como tabla separada, lo cual es eficiente para consultas por año pero puede requerir uniones en transformaciones posteriores.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/idi/idi_load.py"  
Versión del archivo: 3.0

