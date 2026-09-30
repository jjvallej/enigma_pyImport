# Documentación: modules/ipm/ipm_ingest.py

## 1. Información General

El archivo "modules/ipm/ipm_ingest.py" es un módulo Python que se encarga de ingerir archivos Excel desde Google Drive usando enlaces públicos y subirlos a Google Cloud Storage. Este módulo forma parte del proceso de ingestión de datos del IPM (Índice de Pobreza Multidimensional) y proporciona funcionalidades para descargar archivos desde Google Drive, detectar automáticamente si son archivos Excel o Google Sheets, y subirlos al Data Lake en GCS. Su propósito principal es automatizar la extracción de archivos desde Google Drive y almacenarlos en GCS para su posterior procesamiento.

## 2. Propósito y Funcionalidad

El archivo "ipm_ingest.py" cumple varias funciones críticas en el proceso de ingestión. En primer lugar, extrae el File ID de diferentes formatos de URL de Google Drive, permitiendo trabajar con enlaces en múltiples formatos como URLs completas, enlaces abreviados, o directamente con File IDs. En segundo lugar, detecta automáticamente si el archivo es un Google Sheet o un archivo Excel tradicional, y usa el método de descarga apropiado para cada tipo. En tercer lugar, maneja casos especiales como archivos grandes que muestran páginas de advertencia de Google Drive, parseando el HTML para encontrar el enlace de descarga real. Finalmente, sube los archivos descargados a Google Cloud Storage en la ubicación especificada, creando automáticamente las carpetas necesarias si no existen.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports y configuración, incluyendo la lectura de variables desde "config.yaml". La segunda sección contiene funciones helper para extraer File IDs y generar URLs de descarga. La tercera sección contiene la función principal de descarga desde enlaces públicos. La cuarta sección contiene funciones para subir archivos a GCS. Finalmente, la quinta sección contiene la función principal "move_file_from_drive_to_gcs" que orquesta todo el proceso de ingestión.

## 4. Imports y Dependencias

El archivo importa varias librerías necesarias para su funcionamiento. "google.cloud.storage" se usa para interactuar con Google Cloud Storage. "requests" se usa para hacer peticiones HTTP a Google Drive. "re" se usa para expresiones regulares al extraer File IDs de URLs. "os" y "tempfile" se usan para manejar archivos temporales. "datetime" se usa para timestamps. "typing" se usa para type hints. Desde "modules.config" se importan "PROJECT_ID", "DEFAULT_BUCKET_NAME", y "CONF" para acceder a la configuración. Desde "modules.gcp_utils" se importa "get_gcs_client" para obtener el cliente de GCS.

## 5. Función extract_file_id_from_url

La función "extract_file_id_from_url" extrae el File ID de una URL de Google Drive, soportando diferentes formatos de URL. La función primero verifica si la URL ya es solo un File ID, es decir, si tiene menos de cincuenta caracteres y no empieza con "http". En ese caso, retorna el ID directamente después de eliminar espacios. Si es una URL completa, la función intenta extraer el File ID usando varios patrones de expresiones regulares. Los patrones incluyen "/file/d/", "/spreadsheets/d/", "/document/d/", "id=", y "/folders/". Si ninguno de los patrones encuentra un match, la función lanza una excepción "ValueError" con un mensaje descriptivo.

## 6. Función get_public_download_url

La función "get_public_download_url" convierte un File ID de Google Drive a un enlace de descarga directa para archivos públicos. La función toma dos parámetros: "file_id" que es el ID del archivo, e "is_google_sheet" que indica si el archivo es un Google Sheet. Si "is_google_sheet" es "True", la función retorna una URL usando el formato de exportación de Google Sheets con el parámetro "format=xlsx". Si es "False", retorna una URL usando el formato genérico de descarga de Google Drive con el parámetro "export=download". Esta diferenciación es importante porque Google Sheets requiere un formato de exportación específico para descargarse como Excel.

## 7. Función download_file_from_public_link

La función "download_file_from_public_link" descarga un archivo desde Google Drive usando un enlace público, soportando tanto archivos Excel como Google Sheets. La función toma dos parámetros: "drive_url" que es la URL pública de Google Drive o File ID, y "file_name" que es un nombre opcional para el archivo local. La función primero extrae el File ID de la URL usando "extract_file_id_from_url". Luego detecta si es un Google Sheet verificando si la URL contiene "/spreadsheets/d/" o "/spreadsheets/". Crea una sesión de requests con headers que incluyen un User-Agent para evitar bloqueos. Intenta descargar el archivo usando el método detectado. Si obtiene un error 403 o 500 y no había detectado como Google Sheet, intenta nuevamente usando el método de Google Sheets. Si Google Drive muestra una página HTML en lugar del archivo, parsea el HTML buscando diferentes patrones de enlaces de descarga. Si encuentra un enlace, lo usa para descargar el archivo. Si no encuentra el enlace y no es Google Sheet, intenta el método de Google Sheets como último recurso. Determina el nombre del archivo desde el header "Content-Disposition" o usa un nombre por defecto basado en el File ID. Crea un archivo temporal con la extensión apropiada y descarga el contenido en chunks, mostrando progreso si está en modo debug. Retorna una tupla con la ruta del archivo temporal y el nombre original del archivo: `(ruta_local, nombre_original)`.

## 8. Función download_file_from_drive

La función "download_file_from_drive" es un wrapper que llama a "download_file_from_public_link". Esta función proporciona una interfaz más simple y permite que el código que la llama no necesite conocer los detalles internos de cómo se descarga el archivo. Toma dos parámetros: "drive_url_or_id" que es la URL pública de Google Drive o File ID, y "file_name" que es un nombre opcional para el archivo local. Retorna una tupla con la ruta del archivo temporal y el nombre original del archivo: `(ruta_local, nombre_original)`.

## 9. Función upload_file_to_gcs

La función "upload_file_to_gcs" sube un archivo local a Google Cloud Storage. La función toma cuatro parámetros: "local_file_path" que es la ruta del archivo local, "bucket_name" que es el nombre del bucket, "destination_blob_name" que es la ruta completa en GCS, y "overwrite" que indica si sobrescribir archivos existentes. La función obtiene el cliente de GCS usando "get_gcs_client". Normaliza la ruta de destino eliminando barras dobles y asegurando formato consistente. Verifica si existe algún objeto en la carpeta para logging. Obtiene el blob del bucket. Si el archivo ya existe y "overwrite" es "True", elimina el archivo existente antes de subir el nuevo. Si "overwrite" es "False", mantiene el archivo anterior. Sube el archivo usando "upload_from_filename", lo cual crea automáticamente la carpeta si no existe. Retorna la URI completa del archivo en GCS en formato "gs://bucket/path".

## 10. Función move_file_from_drive_to_gcs

La función "move_file_from_drive_to_gcs" es la función principal que orquesta todo el proceso de ingestión. Esta función toma cuatro parámetros: "drive_url_or_id" que puede ser una URL de Google Drive o un File ID, "bucket_name" que es el nombre del bucket, "folder_name" que es el nombre de la carpeta dentro del bucket con valor por defecto "IPM", y "destination_file_name" que es un nombre opcional para el archivo en GCS. La función primero descarga el archivo desde Google Drive usando "download_file_from_drive", que retorna una tupla con la ruta temporal y el nombre original: `(local_file_path, original_file_name)`. Luego determina el nombre del archivo de destino, usando el nombre original extraído de Drive si no se proporciona uno explícitamente. Construye la ruta completa en GCS combinando el nombre de la carpeta y el nombre del archivo, normalizando las rutas para eliminar barras duplicadas y espacios. Sube el archivo a GCS usando "upload_file_to_gcs". Finalmente, en un bloque "finally", elimina el archivo temporal local para limpiar recursos, manejando errores de eliminación sin detener la ejecución. Retorna la URI completa del archivo en GCS en formato "gs://bucket/path".

## 11. Manejo de Errores

El código incluye manejo de errores robusto en varios puntos. Si no se puede extraer el File ID de la URL, se lanza una excepción "ValueError" con un mensaje descriptivo. Si la descarga falla con un código HTTP específico, se proporcionan mensajes de error detallados: para 403 se indica que el archivo puede no ser público, para 404 se indica que el archivo no fue encontrado, y para 500 se indica que puede ser un error del servidor o que el archivo requiere autenticación. Si falla el método detectado, se intenta automáticamente el método alternativo. Si todos los métodos fallan, se lanza una excepción con un mensaje que explica qué se intentó. Si hay errores al eliminar archivos temporales, se imprimen advertencias pero no se detiene la ejecución.

## 12. Modo Debug

El código usa la variable "DEBUG" que se obtiene de "CONF.global_config.debug" para controlar la cantidad de información que se imprime durante la ejecución. Cuando "DEBUG" es "True", se imprimen mensajes detallados sobre el proceso de descarga, incluyendo el File ID extraído, el tipo de archivo detectado, la URL de descarga usada, el progreso de la descarga, y el estado de las operaciones. Esto es útil para diagnosticar problemas durante el desarrollo y la depuración.

## 13. Cómo se Usa en el Código

El módulo se usa principalmente desde los DAGs de Airflow que orquestan el proceso de ingestión. Los DAGs importan la función "move_file_from_drive_to_gcs" y la llaman pasando la URL de Google Drive, el nombre del bucket desde "DEFAULT_BUCKET_NAME", y la carpeta de destino desde "CONF.ipm.gcs_folder". La función maneja todos los detalles de la descarga y subida, retornando la URI de GCS que puede ser usada en pasos posteriores del pipeline.

## 14. Notas Importantes

Es importante que los archivos en Google Drive estén configurados como públicos con el permiso "Cualquier persona con el enlace puede ver" para que la descarga funcione correctamente. El código maneja automáticamente diferentes formatos de URL y tipos de archivos, pero si un archivo es muy grande o requiere autenticación especial, puede fallar. Los archivos temporales se eliminan automáticamente después de la subida, pero si hay un error durante la subida, el archivo temporal puede quedar en el sistema. El código está diseñado para ser robusto y manejar múltiples intentos con diferentes métodos de descarga, pero en casos extremos puede requerir intervención manual.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/ipm/ipm_ingest.py"  
Versión del archivo: 3.0

