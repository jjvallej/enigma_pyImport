# Documentación: modules/ipm/ipm_sisben_ingest.py

## 1. Información General

El archivo "modules/ipm/ipm_sisben_ingest.py" es un módulo Python que se encarga de ingerir archivos Excel desde Google Drive para IPM SISBEN y subirlos a Google Cloud Storage. Este módulo es similar a "ipm_ingest.py" pero incluye funcionalidades adicionales para manejar archivos muy grandes que requieren autenticación con la API de Google Drive. Soporta dos métodos de descarga: enlaces públicos para archivos pequeños y la API de Google Drive con autenticación para archivos grandes de hasta 1.5GB. Su propósito principal es automatizar la extracción de archivos grandes de IPM SISBEN desde Google Drive y almacenarlos en GCS.

## 2. Propósito y Funcionalidad

El archivo "ipm_sisben_ingest.py" cumple las mismas funciones básicas que "ipm_ingest.py" pero con capacidades extendidas para archivos grandes. Extrae File IDs de URLs de Google Drive, detecta automáticamente si son Google Sheets o archivos Excel, intenta primero descargar usando la API de Google Drive con autenticación para mejor rendimiento con archivos grandes, y si falla, intenta con el método de enlace público como fallback. Maneja casos especiales como archivos que muestran páginas de advertencia, parsea HTML para encontrar enlaces de descarga, y usa timeouts más largos para archivos grandes. También proporciona funciones para mover archivos dentro de GCS entre carpetas.

## 3. Estructura del Archivo

El archivo sigue una estructura similar a "ipm_ingest.py" pero con funciones adicionales. Contiene imports que incluyen librerías de la API de Google Drive, funciones helper para extraer File IDs y generar URLs, función de descarga desde enlaces públicos con mejor manejo de archivos grandes, función de descarga usando la API de Google Drive con autenticación, función principal que intenta ambos métodos, función para subir archivos a GCS, función para mover archivos dentro de GCS, y función principal que orquesta todo el proceso.

## 4. Función download_file_from_drive_api

La función "download_file_from_drive_api" descarga un archivo desde Google Drive usando la API de Google Drive con autenticación, lo cual es útil para archivos grandes. La función obtiene credenciales usando Application Default Credentials, construye el servicio de Google Drive, obtiene metadatos del archivo incluyendo nombre y tamaño, determina si es un Google Sheet para usar el formato de exportación apropiado, crea un archivo temporal, y descarga el archivo usando "MediaIoBaseDownload" que maneja archivos grandes de manera eficiente. Muestra progreso durante la descarga si está en modo debug. Esta función es preferida para archivos grandes porque la API de Google Drive es más robusta y eficiente que los enlaces públicos.

## 5. Función download_file_from_public_link

La función "download_file_from_public_link" es similar a la de "ipm_ingest.py" pero con mejoras para archivos grandes. Usa timeouts más largos de 600 segundos en lugar de 30 segundos para dar tiempo a archivos grandes. Usa chunks más grandes de 1MB en lugar de 8KB para mejor rendimiento. Incluye más patrones de búsqueda de enlaces de descarga en el HTML para ser más robusto. Maneja mejor los casos donde Google Drive muestra páginas de advertencia para archivos grandes, intentando múltiples métodos alternativos antes de fallar, incluyendo un método alternativo específico para Google Sheets grandes que usa el formato de exportación con confirmación. Esta función se usa como fallback cuando la API de Google Drive no está disponible o falla. Retorna una tupla con la ruta temporal y el nombre original del archivo: `(ruta_local, nombre_original)`.

## 6. Función download_file_from_drive

La función "download_file_from_drive" es la función principal que intenta ambos métodos de descarga. Primero intenta usar la API de Google Drive con "download_file_from_drive_api" porque es mejor para archivos grandes. Si falla, intenta con el método de enlace público usando "download_file_from_public_link" como fallback. Si ambos métodos fallan, lanza una excepción con mensajes de error de ambos intentos. Esta estrategia de fallback asegura que el código funcione incluso si la autenticación no está disponible, pero prefiere la API para mejor rendimiento. Retorna una tupla con la ruta temporal y el nombre original del archivo: `(ruta_local, nombre_original)`.

## 7. Función move_file_within_gcs

La función "move_file_within_gcs" mueve un archivo dentro del mismo bucket de GCS desde una carpeta origen a una carpeta destino. La función busca archivos en la carpeta origen, filtra por un patrón opcional si se especifica, encuentra el archivo más reciente por tiempo de actualización, copia el archivo a la nueva ubicación, y elimina el archivo original. Esta función es útil para mover archivos desde carpetas temporales a carpetas finales después de procesamiento. Retorna la URI completa del archivo movido.

## 8. Diferencias con ipm_ingest.py

Las principales diferencias con "ipm_ingest.py" son el soporte para la API de Google Drive con autenticación, timeouts más largos para archivos grandes, chunks más grandes para mejor rendimiento, más patrones de búsqueda de enlaces en HTML, y la función adicional para mover archivos dentro de GCS. Estas mejoras están diseñadas específicamente para manejar archivos muy grandes como los de IPM SISBEN que pueden ser de 1.5GB o más.

## 9. Manejo de Archivos Grandes

El módulo está optimizado para archivos grandes de varias maneras. Usa la API de Google Drive que es más eficiente que enlaces públicos. Usa timeouts de 600 segundos para dar tiempo suficiente a la descarga. Usa chunks de 1MB para reducir el número de operaciones de red. Muestra progreso cada 10MB descargados en modo debug. Maneja mejor los casos donde Google Drive muestra páginas de advertencia para archivos grandes, parseando el HTML de manera más exhaustiva para encontrar el enlace de descarga real.

## 10. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IPM SISBEN de la misma manera que "ipm_ingest.py". Los DAGs importan "move_file_from_drive_to_gcs" y la llaman pasando la URL de Google Drive desde "CONF.ipm_sisben.drive_url", el nombre del bucket, y la carpeta desde "CONF.ipm_sisben.gcs_folder". La función maneja automáticamente la selección del mejor método de descarga y retorna la URI de GCS.

## 11. Notas Importantes

Para archivos muy grandes, es recomendable que el archivo en Google Drive esté compartido con la service account de Composer para que la API de Google Drive funcione correctamente. Si el archivo no está compartido, la API fallará y se usará el método de enlace público como fallback, pero este puede ser más lento y menos confiable para archivos grandes. Los timeouts están configurados para archivos grandes, pero si un archivo es extremadamente grande o la conexión es muy lenta, puede necesitar ajustes. El código maneja automáticamente la selección del mejor método, pero en casos extremos puede requerir configuración manual de permisos o ajustes de timeout.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/ipm/ipm_sisben_ingest.py"  
Versión del archivo: 3.0

