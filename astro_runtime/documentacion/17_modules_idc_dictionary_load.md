# Documentación: modules/idc/idc_dictionary_load.py

## 1. Información General

El archivo "modules/idc/idc_dictionary_load.py" es un módulo Python que se encarga de extraer datos del archivo CSV del diccionario IDC desde Google Cloud Storage, transformarlos mínimamente y cargarlos en BigQuery. Este módulo es similar a otros módulos de carga pero está específicamente diseñado para el diccionario IDC que se carga directamente en la capa gold en lugar de bronze, porque el diccionario es un archivo de referencia que no cambia frecuentemente y se usa para interpretar los datos principales. Su propósito principal es cargar el diccionario IDC a BigQuery para que esté disponible para consultas y transformaciones.

## 2. Propósito y Funcionalidad

El archivo "idc_dictionary_load.py" cumple funciones específicas para el diccionario IDC. En primer lugar, asegura que el dataset gold exista en BigQuery, creándolo si es necesario. En segundo lugar, obtiene el archivo CSV más reciente desde una carpeta en GCS, ordenando por fecha de creación. En tercer lugar, descarga el archivo CSV a un archivo temporal local. En cuarto lugar, lee el CSV con pandas, convierte todas las columnas a STRING para preservar valores originales, agrega una columna "fecha_lectura" con timestamp, y carga el DataFrame a BigQuery con un esquema donde todas las columnas son STRING excepto "fecha_lectura" que es TIMESTAMP. El módulo carga el diccionario directamente en gold porque es un archivo de referencia que no requiere las capas intermedias.

## 3. Estructura del Archivo

El archivo está organizado de manera simple. Contiene imports y configuración, función "ensure_dataset" que crea el dataset gold, función "get_latest_csv_from_gcs_folder" que obtiene el CSV más reciente, función "download_csv_from_gcs" que descarga el CSV, función "load_csv_to_bq" que carga el CSV a BigQuery, y función "cleanup_temp_paths" que limpia archivos temporales. El módulo es más simple que otros módulos de carga porque el diccionario no requiere transformaciones complejas.

## 4. Función get_latest_csv_from_gcs_folder

La función "get_latest_csv_from_gcs_folder" obtiene la URI del último archivo CSV subido a una carpeta en GCS. La función normaliza la ruta de la carpeta, obtiene el bucket, lista todos los blobs en la carpeta, filtra solo archivos ".csv", ordena por tiempo de creación en orden descendente, toma el más reciente, y retorna la URI completa. Esta función es similar a funciones en otros módulos pero busca archivos CSV en lugar de Excel.

## 5. Función load_csv_to_bq

La función "load_csv_to_bq" carga un archivo CSV a BigQuery como tabla. La función lee el CSV con pandas usando encoding UTF-8, convierte todas las columnas a STRING para preservar valores originales, agrega una columna "fecha_lectura" con timestamp UTC, crea un esquema donde todas las columnas son STRING excepto "fecha_lectura" que es TIMESTAMP, configura el job con "WRITE_TRUNCATE" para reemplazar la tabla si existe, y carga el DataFrame a BigQuery. Retorna el nombre de la tabla creada. Esta función es simple porque el diccionario no requiere transformaciones complejas, solo preservación de datos originales.

## 6. Carga Directa en Gold

A diferencia de otros módulos que cargan en bronze, este módulo carga el diccionario directamente en gold. Esto es porque el diccionario es un archivo de referencia que contiene metadatos y mapeos que se usan para interpretar los datos principales. No requiere las capas intermedias de bronze y silver porque no es un dato transaccional que cambie frecuentemente. El diccionario se carga una vez y se actualiza ocasionalmente cuando cambian los metadatos.

## 7. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IDC Dictionary. Los DAGs primero aseguran que el dataset gold existe, luego obtienen el CSV más reciente desde GCS usando "get_latest_csv_from_gcs_folder", lo descargan usando "download_csv_from_gcs", y lo cargan a BigQuery usando "load_csv_to_bq" especificando el dataset gold. Los archivos temporales se limpian al final del proceso.

## 8. Notas Importantes

Este módulo carga el diccionario directamente en gold, no en bronze. El dataset usado es el dataset gold definido en "config.yaml", no el dataset bronze. El esquema se infiere automáticamente del CSV, con todas las columnas como STRING para preservar valores originales. El diccionario generalmente es un archivo pequeño, por lo que no se necesitan optimizaciones para archivos grandes. La función siempre usa "WRITE_TRUNCATE" para reemplazar la tabla completa, lo cual es apropiado para un diccionario que se actualiza ocasionalmente en lugar de acumularse históricamente.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/idc/idc_dictionary_load.py"  
Versión del archivo: 3.0

