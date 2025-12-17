# Documentación: modules/evaplan/evaplan_ingest.py

## 1. Información General

El archivo "modules/evaplan/evaplan_ingest.py" es un módulo Python que se encarga de ingerir datos desde la API de Evaplan. Este módulo es significativamente diferente de otros módulos de ingestión porque no descarga archivos desde Google Drive o páginas web, sino que consume endpoints de una API REST, obtiene tokens de autenticación, y almacena las respuestas JSON en Google Cloud Storage. Su propósito principal es automatizar la extracción de datos desde la API de Evaplan y almacenarlos en el Data Lake para su posterior procesamiento.

## 2. Propósito y Funcionalidad

El archivo "evaplan_ingest.py" cumple varias funciones críticas. En primer lugar, autentica con la API de Evaplan usando credenciales desde "config.yaml" y obtiene un token de acceso Bearer. En segundo lugar, obtiene la lista de periodos disponibles desde la API. En tercer lugar, identifica el periodo más reciente. En cuarto lugar, obtiene datos de diferentes endpoints de la API como AvanceMR, AvanceMP, AvanceXSubprograma, AvanceGeneral, AvanceSubprogramas, AvanceProgramas, y SectorMP, cada uno requiriendo el ID del periodo. En quinto lugar, guarda todas las respuestas JSON en GCS con nombres de archivo que incluyen fechas y IDs de periodo para trazabilidad. Finalmente, proporciona funciones para leer los JSON más recientes desde GCS para uso en procesos posteriores.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports, configuración, y construcción de endpoints completos desde "config.yaml", incluyendo los endpoints "AVANCE_SUBPROGRAMAS_ENDPOINT", "AVANCE_PROGRAMAS_ENDPOINT" y "SECTOR_MP_ENDPOINT". La segunda sección contiene funciones de autenticación como "get_auth_credentials" y "authenticate". La tercera sección contiene funciones para obtener datos de la API como "get_periodos", "get_periodo_mas_reciente", "get_avance_mr", "get_avance_mp", "get_avance_x_subprograma", "get_avance_general", "get_avance_subprogramas", "get_avance_programas", y "get_sector_mp". La cuarta sección contiene funciones para interactuar con GCS como "upload_json_to_gcs", "save_periodos_to_gcs", "read_latest_periodos_json_from_gcs", "get_all_periodos_from_json", y "save_avance_to_gcs".

## 4. Función get_auth_credentials

La función "get_auth_credentials" obtiene las credenciales de autenticación desde "config.yaml". La función accede a "CONF.evaplan.credentials" y extrae los atributos "usuario" y "password" usando "getattr". Convierte los valores a strings y limpia espacios. Valida que ambas credenciales estén presentes, lanzando una excepción "ValueError" si faltan. Retorna un diccionario con las credenciales en el formato requerido por la API. Esta función centraliza la obtención de credenciales y proporciona mensajes de error claros si no están configuradas.

## 5. Función authenticate

La función "authenticate" autentica con la API de Evaplan y obtiene un token de acceso Bearer. La función obtiene las credenciales usando "get_auth_credentials". Hace una prueba de conectividad básica usando sockets para diagnosticar problemas de red. Crea una sesión de requests y hace una petición POST al endpoint de autenticación con las credenciales en formato JSON. Usa un timeout de 240 segundos para dar tiempo a la conexión desde Composer. Valida la respuesta verificando que tenga "success" como "True" y que contenga un token en "data.token". Retorna el token de acceso. Maneja diferentes tipos de errores y proporciona mensajes descriptivos.

## 6. Funciones de Obtención de Datos

Las funciones "get_periodos", "get_avance_mr", "get_avance_mp", "get_avance_x_subprograma", "get_avance_general", "get_avance_subprogramas", "get_avance_programas", y "get_sector_mp" siguen un patrón similar. Cada función toma un token de autenticación y parámetros específicos como "peri_idp". Crea una sesión de requests con headers que incluyen el token Bearer. Hace una petición GET al endpoint correspondiente con parámetros de query si es necesario. Valida la respuesta verificando que tenga "success" como "True". Agrega "peri_idp" al nivel raíz de la respuesta para trazabilidad. Retorna el diccionario completo de la respuesta. Todas usan timeouts de 240 segundos y manejan errores de manera consistente. Las funciones "get_avance_subprogramas", "get_avance_programas" y "get_sector_mp" fueron agregadas para soportar los nuevos endpoints de la API de Evaplan. La función "get_sector_mp" maneja la estructura especial de la respuesta donde los datos están en "data.AvanceMP" aunque el endpoint sea SectorMP.

## 7. Funciones de GCS

Las funciones de GCS permiten guardar y leer JSON desde Google Cloud Storage. "upload_json_to_gcs" sube un diccionario JSON a GCS, creando carpetas automáticamente si no existen. "save_periodos_to_gcs" guarda la respuesta de periodos con un nombre de archivo que incluye la fecha de consulta. "read_latest_periodos_json_from_gcs" lee el JSON más reciente de periodos buscando archivos que empiecen con "periodo_" y terminen en ".json". "save_avance_to_gcs" guarda respuestas de avances con nombres que incluyen el tipo de avance, la fecha, y el "peri_idp". Todas estas funciones normalizan rutas, manejan sobrescritura de archivos, y proporcionan logging detallado.

## 8. Formato de Nombres de Archivos

Los archivos JSON se guardan con nombres específicos que incluyen información de trazabilidad. Los archivos de periodos tienen formato "periodo_YYYYMMDD.json" donde la fecha viene de "fecha_consulta" en la respuesta de la API o de la fecha actual. Los archivos de avances tienen formato "{tipo_avance}_{fecha}_peri_idp_{peri_idp}.json" donde el tipo de avance se normaliza a minúsculas con guiones bajos. Este formato permite identificar fácilmente qué datos contiene cada archivo y cuándo fueron consultados.

## 9. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de Evaplan. Los DAGs primero autentican con la API, obtienen los periodos, identifican el más reciente, obtienen datos de todos los endpoints (incluyendo AvanceSubprogramas, AvanceProgramas y SectorMP), y guardan todo en GCS. Los DAGs pueden leer los JSON más recientes desde GCS en pasos posteriores para cargar los datos a BigQuery. El módulo proporciona una interfaz completa para interactuar con la API y almacenar los datos en el Data Lake. El mapeo de nombres de endpoints incluye "AvanceSubprogramas", "AvanceProgramas" y "SectorMP" para mantener consistencia con los nombres de carpetas en GCS. La función "save_avance_to_gcs" maneja el caso especial de SectorMP donde "fecha_consulta" está en el nivel raíz del JSON dentro de "data".

## 10. Manejo de Errores

El código incluye manejo de errores robusto en varios puntos. Si las credenciales no están configuradas, se lanza una excepción con mensaje claro. Si la autenticación falla, se proporciona información sobre el error. Si las peticiones a la API fallan, se capturan excepciones de requests y se proporcionan mensajes descriptivos. Si hay errores al parsear JSON, se capturan y se proporcionan mensajes claros. El código también incluye pruebas de conectividad básica para diagnosticar problemas de red antes de intentar autenticar.

## 11. Notas Importantes

Es crítico que las credenciales estén correctamente configuradas en "config.yaml" en la sección "evaplan.credentials". El módulo requiere que la API esté accesible desde el entorno donde se ejecuta, lo cual puede requerir configuración de VPC o firewall en Cloud Composer. Los timeouts están configurados a 240 segundos para dar tiempo a conexiones desde Composer que pueden ser más lentas. El módulo almacena las respuestas completas de la API en JSON, preservando toda la estructura original para referencia futura. Los nombres de archivos incluyen fechas y IDs para facilitar la identificación y el versionado de los datos.

---

Última actualización: 2025-12-17  
Archivo documentado: "modules/evaplan/evaplan_ingest.py"  
Versión del archivo: 3.1

