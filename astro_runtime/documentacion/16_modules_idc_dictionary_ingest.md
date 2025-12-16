# Documentación: modules/idc/idc_dictionary_ingest.py

## 1. Información General

El archivo "modules/idc/idc_dictionary_ingest.py" es un módulo Python que se encarga de ingerir archivos CSV del diccionario IDC desde Google Drive usando enlaces públicos y subirlos a Google Cloud Storage. Este módulo es similar a otros módulos de ingestión pero está específicamente diseñado para archivos CSV en lugar de archivos Excel. El diccionario IDC contiene metadatos y mapeos que se usan para interpretar y transformar los datos principales de IDC. Su propósito principal es automatizar la extracción del diccionario desde Google Drive y almacenarlo en GCS para su posterior uso en transformaciones.

## 2. Propósito y Funcionalidad

El archivo "idc_dictionary_ingest.py" cumple funciones similares a otros módulos de ingestión pero para archivos CSV. Extrae File IDs de URLs de Google Drive, genera URLs de descarga directa para archivos públicos, descarga archivos CSV desde Google Drive manejando casos especiales como páginas de advertencia, y sube los archivos a GCS en la ubicación especificada. El módulo es más simple que los módulos de Excel porque los archivos CSV no requieren detección de tipo de archivo ni métodos especiales de exportación. El módulo lee la configuración desde "config.yaml" usando "CONF.idc.dictionary_drive_url" para la URL y "CONF.idc.gcs_folder" para la carpeta de destino.

## 3. Estructura del Archivo

El archivo está organizado de manera similar a otros módulos de ingestión. Contiene imports y configuración, funciones de utilidad como "extract_file_id_from_url" y "get_public_download_url", función "download_file_from_public_link" que descarga archivos CSV, función "download_file_from_drive" que es un wrapper, función "upload_file_to_gcs" que sube archivos a GCS, y función "move_file_from_drive_to_gcs" que orquesta todo el proceso. La estructura es más simple que los módulos de Excel porque no necesita manejar diferentes tipos de archivos.

## 4. Funciones Principales

Las funciones principales son similares a las de otros módulos de ingestión. "extract_file_id_from_url" extrae File IDs de URLs usando patrones de expresiones regulares, pero con menos patrones porque los CSV generalmente usan formatos más simples. "get_public_download_url" genera URLs de descarga directa sin necesidad de detectar tipos de archivo. "download_file_from_public_link" descarga archivos CSV manejando páginas de advertencia de Google Drive y retorna una tupla con la ruta temporal y el nombre original del archivo: `(ruta_local, nombre_original)`. "upload_file_to_gcs" sube archivos a GCS creando carpetas automáticamente. "move_file_from_drive_to_gcs" orquesta todo el proceso descargando y subiendo el archivo, usando el nombre original extraído de Drive si no se proporciona un nombre de destino explícito.

## 5. Diferencias con Módulos de Excel

Las principales diferencias con módulos de ingestión de Excel son que no necesita detectar si es Google Sheet o archivo Excel, no necesita métodos especiales de exportación, y el proceso es más directo. Los archivos CSV se descargan directamente sin conversión. El módulo es más simple y robusto porque los CSV son un formato más estándar y no requieren el manejo complejo que requieren los archivos Excel y Google Sheets.

## 6. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IDC Dictionary. Los DAGs importan "move_file_from_drive_to_gcs" y la llaman pasando la URL de Google Drive desde "CONF.idc.dictionary_drive_url", el nombre del bucket desde "DEFAULT_BUCKET_NAME", y la carpeta desde "CONF.idc.gcs_folder". La función retorna la URI de GCS que se usa en pasos posteriores para cargar el diccionario a BigQuery.

## 7. Notas Importantes

Este módulo es más simple que los módulos de Excel porque los CSV no requieren manejo especial. El diccionario IDC generalmente es un archivo pequeño, por lo que no se necesitan optimizaciones para archivos grandes. El módulo asume que el archivo CSV está configurado como público en Google Drive. Si el archivo es privado, se necesitaría usar la API de Google Drive con autenticación, similar a "ipm_sisben_ingest.py", pero esto generalmente no es necesario para el diccionario que suele ser un archivo pequeño y público.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/idc/idc_dictionary_ingest.py"  
Versión del archivo: 3.0

