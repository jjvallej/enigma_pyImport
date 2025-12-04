# Documentación: modules/idc/idc_ingest.py

## 1. Información General

El archivo "modules/idc/idc_ingest.py" es un módulo Python que se encarga de ingerir archivos Excel desde Google Drive usando enlaces públicos y subirlos a Google Cloud Storage. Este módulo es muy similar a "ipm_ingest.py" en estructura y funcionalidad, pero está específicamente diseñado para los datos del IDC (Índice de Desarrollo de Capacidades). Proporciona las mismas funcionalidades de descarga desde Google Drive, detección automática de tipos de archivo, y subida a GCS, pero está configurado para usar las carpetas y configuraciones específicas del IDC definidas en "config.yaml".

## 2. Propósito y Funcionalidad

El archivo "idc_ingest.py" cumple las mismas funciones que "ipm_ingest.py" pero para datos del IDC. Extrae File IDs de URLs de Google Drive, detecta automáticamente si son Google Sheets o archivos Excel, maneja casos especiales como archivos grandes, y sube los archivos a GCS. La principal diferencia es que usa "CONF.idc.gcs_folder" para determinar la carpeta de destino en GCS en lugar de usar una carpeta hardcodeada. El módulo está diseñado para ser reutilizable y seguir el mismo patrón que otros módulos de ingestión en el proyecto.

## 3. Estructura del Archivo

El archivo sigue la misma estructura que "ipm_ingest.py". Contiene imports y configuración, funciones helper para extraer File IDs y generar URLs, función de descarga desde enlaces públicos, funciones para subir archivos a GCS, y función principal que orquesta todo el proceso. La única diferencia significativa es el valor por defecto del parámetro "folder_name" en "move_file_from_drive_to_gcs" que es "IDC" en lugar de "IPM".

## 4. Funciones Principales

Las funciones principales son idénticas a las de "ipm_ingest.py": "extract_file_id_from_url" extrae File IDs de URLs, "get_public_download_url" genera URLs de descarga, "download_file_from_public_link" descarga archivos desde Google Drive, "download_file_from_drive" es un wrapper, "upload_file_to_gcs" sube archivos a GCS, y "move_file_from_drive_to_gcs" orquesta todo el proceso. Todas estas funciones funcionan de la misma manera que en "ipm_ingest.py" y comparten la misma lógica de manejo de errores y casos especiales.

## 5. Configuración Específica de IDC

El módulo lee la configuración específica del IDC desde "config.yaml" usando "CONF.idc". Esto incluye la URL de Google Drive desde "CONF.idc.drive_url" y la carpeta de GCS desde "CONF.idc.gcs_folder". Esta configuración permite que el mismo código funcione para diferentes fuentes simplemente cambiando los valores en "config.yaml", siguiendo el principio de centralización de configuración del proyecto.

## 6. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IDC de la misma manera que "ipm_ingest.py". Los DAGs importan "move_file_from_drive_to_gcs" y la llaman pasando la URL de Google Drive desde "CONF.idc.drive_url", el nombre del bucket desde "DEFAULT_BUCKET_NAME", y la carpeta desde "CONF.idc.gcs_folder". La función retorna la URI de GCS que se usa en pasos posteriores del pipeline.

## 7. Notas Importantes

Este módulo es funcionalmente idéntico a "ipm_ingest.py" y comparte el mismo código base. Esto es intencional para mantener consistencia en el proyecto y facilitar el mantenimiento. Si se necesita hacer cambios en la lógica de ingestión, se pueden aplicar a todos los módulos de ingestión de manera similar. La única diferencia real es la configuración que se lee desde "config.yaml", lo cual permite que el mismo código funcione para diferentes fuentes.

---

Última actualización: 2025-01-XX  
Archivo documentado: "modules/idc/idc_ingest.py"  
Versión del archivo: 3.0

