# Documentación: modules/idi/idi_ingest.py

## 1. Información General

El archivo "modules/idi/idi_ingest.py" es un módulo Python que se encarga de ingerir archivos Excel de IDI desde URLs de Función Pública. Este módulo es más complejo que otros módulos de ingestión porque no descarga directamente desde Google Drive, sino que lee una configuración desde un Excel en Google Drive, hace web scraping para encontrar enlaces de descarga en páginas web, y luego descarga y transforma los archivos Excel a CSV antes de subirlos a GCS. Su propósito principal es automatizar la extracción de datos del IDI desde fuentes web públicas que requieren navegación y scraping.

## 2. Propósito y Funcionalidad

El archivo "idi_ingest.py" cumple varias funciones complejas. En primer lugar, descarga un Excel de configuración desde Google Drive que contiene información sobre años, nombres, y enlaces a páginas web. En segundo lugar, busca en el Excel de configuración la fila correspondiente a un año específico usando palabras clave. En tercer lugar, extrae la URL de la página web desde la configuración. En cuarto lugar, hace web scraping de la página web para encontrar el enlace de descarga del archivo Excel, buscando enlaces que contengan palabras clave específicas y que apunten a archivos ".xlsx". En quinto lugar, descarga el archivo Excel desde el enlace encontrado. En sexto lugar, transforma el Excel con pandas aplicando normalización de nombres de columnas, eliminando filas y columnas según configuración. Finalmente, convierte el DataFrame a CSV y lo sube a GCS.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports y configuración. La segunda sección contiene funciones helper como "normalize_column_name" para normalizar nombres de columnas, "extract_file_id_from_gdrive_url" para extraer File IDs, y "download_config_excel_from_gdrive" para descargar el Excel de configuración. La tercera sección contiene "find_download_link_in_page" que hace web scraping. La cuarta sección contiene "ingest_idi_from_config" que procesa un año específico. La quinta sección contiene "ingest_all_years_idi" que procesa todos los años disponibles.

## 4. Función normalize_column_name

La función "normalize_column_name" normaliza nombres de columnas para BigQuery eliminando tildes y caracteres especiales, convirtiendo a minúsculas, reemplazando espacios por guiones bajos, y asegurando que empiece con letra. La función usa "unicodedata" para eliminar tildes, convierte a minúsculas, elimina caracteres no alfanuméricos excepto espacios, reemplaza espacios por guiones bajos, y asegura que el nombre empiece con letra agregando un prefijo si es necesario. Limita la longitud a 128 caracteres. Esta función es importante porque los nombres de columnas en los archivos Excel del IDI pueden tener caracteres especiales y tildes que BigQuery no acepta.

## 5. Función download_config_excel_from_gdrive

La función "download_config_excel_from_gdrive" descarga el Excel de configuración desde Google Drive y lo retorna como DataFrame. La función extrae el File ID de la URL, construye la URL de descarga para Google Sheets, descarga el archivo, y lo lee con pandas usando "openpyxl" como engine, saltando la primera fila que suele ser un encabezado. Limpia los nombres de columnas eliminando espacios. Retorna el DataFrame con las columnas esperadas: "AÑO", "NOMBRE", "IDI", y "ENLACE". Esta configuración contiene la información necesaria para encontrar y descargar los archivos Excel de cada año.

## 6. Función find_download_link_in_page

La función "find_download_link_in_page" busca en una página HTML el enlace que contenga alguno de los textos especificados y que apunte a un archivo ".xlsx". La función usa "BeautifulSoup" para parsear el HTML. Busca todos los enlaces "a" en la página. Para cada enlace, verifica si el texto contiene alguna de las palabras clave proporcionadas. Si encuentra un enlace con una palabra clave, verifica que el "href" apunte a un archivo ".xlsx" o ".xls". Si el href es relativo, construye la URL absoluta usando "urljoin". Retorna la URL completa del archivo para descargar. Esta función es crítica porque las páginas web pueden tener múltiples enlaces y necesita encontrar el correcto basándose en palabras clave.

## 7. Función ingest_idi_from_config

La función "ingest_idi_from_config" es el pipeline completo para procesar un año específico. La función lee el Excel de configuración, busca la fila correspondiente al año y palabras clave, extrae la URL de la página, hace web scraping para encontrar el enlace de descarga, descarga el Excel, lo transforma con pandas aplicando normalización de columnas, eliminando filas y columnas según configuración, lo convierte a CSV con delimitador punto y coma, y lo sube a GCS. La función maneja archivos temporales y los limpia al final. Retorna la URI de GCS del archivo CSV subido.

## 8. Función ingest_all_years_idi

La función "ingest_all_years_idi" procesa todos los años encontrados en el Excel de configuración. La función lee la configuración, filtra las filas que contengan cualquiera de las palabras clave y que tengan enlace, obtiene los años únicos, los ordena de más reciente a más antiguo, y procesa cada año llamando a "ingest_idi_from_config". Para cada año, crea una carpeta en GCS con el nombre del año y un archivo CSV con el nombre "resultados_{año}.csv". Maneja errores por año sin detener el proceso completo. Retorna una lista de resultados con el estado de procesamiento de cada año.

## 9. Transformaciones Aplicadas

Las transformaciones aplicadas incluyen saltar un número configurable de filas al inicio del Excel, normalizar nombres de columnas para BigQuery, eliminar filas completamente vacías, eliminar columnas especificadas en la configuración, y manejar columnas duplicadas agregando sufijos numéricos. El archivo se guarda como CSV con delimitador punto y coma, que es el formato estándar usado en el proyecto para archivos CSV del IDI.

## 10. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IDI. Los DAGs importan "ingest_all_years_idi" y la llaman pasando la URL del Excel de configuración desde "CONF.idi.config_drive_url", las palabras clave desde "CONF.idi.link_name_keywords", el nombre del bucket, la carpeta base, y parámetros de transformación desde "CONF.idi.transform_config". La función procesa todos los años automáticamente y retorna un resumen del procesamiento.

## 11. Notas Importantes

Este módulo es más frágil que otros porque depende de la estructura de páginas web que puede cambiar. Si la estructura de la página web cambia, la función "find_download_link_in_page" puede necesitar ajustes. El módulo requiere que el Excel de configuración esté actualizado con los enlaces correctos a las páginas web. Las palabras clave deben ser específicas enough para encontrar el enlace correcto pero generales enough para funcionar con diferentes versiones de las páginas. El código maneja errores por año, por lo que si un año falla, los demás continúan procesándose. Los archivos se guardan como CSV con delimitador punto y coma, lo cual es importante para la compatibilidad con el proceso de carga posterior.

---

Última actualización: 2025-01-XX  
Archivo documentado: "modules/idi/idi_ingest.py"  
Versión del archivo: 3.0

