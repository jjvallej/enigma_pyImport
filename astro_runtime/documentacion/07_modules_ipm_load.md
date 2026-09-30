# Documentación: modules/ipm/ipm_load.py

## 1. Información General

El archivo "modules/ipm/ipm_load.py" es un módulo Python que se encarga de extraer datos desde Google Cloud Storage, transformarlos mínimamente y cargarlos en la capa bronze de BigQuery. Este módulo forma parte del proceso de carga de datos del IPM y proporciona funcionalidades para leer archivos Excel desde GCS, aplicar transformaciones básicas como eliminar filas de totales y renombrar columnas, y cargar los datos transformados en BigQuery con un esquema predefinido. Su propósito principal es preparar los datos raw para su almacenamiento en la capa bronze, preservando los valores originales como strings para que las transformaciones completas se realicen posteriormente en la capa silver con dbt.

## 2. Propósito y Funcionalidad

El archivo "ipm_load.py" cumple varias funciones importantes en el proceso de carga. En primer lugar, asegura que el dataset bronze exista en BigQuery, creándolo si es necesario. En segundo lugar, obtiene el archivo Excel más reciente desde una carpeta en GCS, ordenando por fecha de creación. En tercer lugar, descarga el archivo Excel a un archivo temporal local para procesamiento. En cuarto lugar, aplica transformaciones mínimas al Excel, incluyendo eliminar la primera fila que suele contener totales, renombrar columnas según un esquema esperado, y convertir todos los valores a strings para preservar los datos originales. Finalmente, carga el DataFrame transformado a BigQuery con un esquema predefinido que especifica que todas las columnas son STRING excepto "fecha_lectura" que es TIMESTAMP.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports y configuración. La segunda sección contiene funciones helper como "ensure_dataset" para crear datasets, "get_latest_excel_from_gcs_folder" para obtener el archivo más reciente, y "download_excel_from_gcs" para descargar archivos. La tercera sección contiene la función "transform_excel" que aplica las transformaciones al Excel. La cuarta sección contiene funciones para cargar DataFrames a BigQuery. Finalmente, la quinta sección contiene funciones de limpieza para eliminar archivos temporales.

## 4. Función ensure_dataset

La función "ensure_dataset" crea el dataset bronze en BigQuery si no existe. La función toma dos parámetros: "dataset_id" que es el ID del dataset, y "location" que es la ubicación del dataset, con valor por defecto "None" que usa "LOCATION" de "config.yaml". La función obtiene el cliente de BigQuery usando "get_bq_client". Construye el nombre completo del dataset como "PROJECT_ID.dataset_id". Intenta obtener el dataset existente. Si existe, imprime un mensaje de confirmación si está en modo debug. Si no existe, crea un nuevo dataset con la ubicación especificada, una descripción indicando que es la capa bronze para IPM v2, y lo crea usando el cliente. Esta función asegura que el dataset esté disponible antes de intentar cargar datos.

## 5. Función get_latest_excel_from_gcs_folder

La función "get_latest_excel_from_gcs_folder" obtiene la URI del último archivo Excel subido a una carpeta en GCS. La función toma dos parámetros: "bucket_name" que es el nombre del bucket, y "folder_path" que es la ruta de la carpeta. La función obtiene el cliente de GCS usando "get_gcs_client". Normaliza la ruta de la carpeta asegurando que termine con "/". Obtiene el bucket y lista todos los blobs en la carpeta. Filtra solo archivos que terminen en ".xlsx" y que no sean carpetas. Si no encuentra archivos, lanza una excepción "ValueError". Ordena los archivos por tiempo de creación en orden descendente y toma el más reciente. Retorna la URI completa del archivo en formato "gs://bucket/folder/file.xlsx". Si está en modo debug, imprime información sobre los archivos encontrados.

## 6. Función download_excel_from_gcs

La función "download_excel_from_gcs" descarga el archivo Excel desde GCS hacia un archivo temporal y devuelve la ruta local generada. La función toma un parámetro "gcs_uri" que es la URI completa del archivo. Parsea la URI para extraer el nombre del bucket y el nombre del blob. Obtiene el blob del bucket. Crea un archivo temporal con extensión ".xlsx" usando "tempfile.mkstemp". Descarga el archivo al archivo temporal usando "download_to_filename". Retorna la ruta del archivo temporal. Esta función es necesaria porque pandas necesita un archivo local para leer archivos Excel.

## 7. Función transform_excel

La función "transform_excel" aplica las transformaciones esperadas al Excel de IPM y devuelve un DataFrame listo para cargarse a BigQuery en bronze. La función toma dos parámetros: "local_path" que es la ruta local del archivo Excel, y "sheet_index" que es el índice de la hoja a leer con valor por defecto 0. La función lee el Excel usando "pd.read_excel" especificando la hoja y que la primera fila es el encabezado. Elimina la primera fila de datos que suele contener totales o una fila guía. Define una lista de columnas esperadas que incluye identificadores como "cod_mpio" y "Municipio", columnas de totales, columnas de IPM con sufijos "_Abs" y "_Porc", y columnas de indicadores I1 a I15, cada una con cuatro variantes. Verifica que el archivo tenga al menos el número esperado de columnas, lanzando un error si tiene menos. Si tiene más columnas, toma solo las primeras esperadas y muestra una advertencia en modo debug. Renombra las columnas del DataFrame según la lista esperada. Convierte todas las columnas a STRING para preservar los valores originales en bronze, incluyendo columnas que normalmente serían numéricas. Agrega una columna "fecha_lectura" con el timestamp actual en UTC. Retorna el DataFrame transformado.

## 8. Función _load_df_to_bq

La función "_load_df_to_bq" es una función interna que carga un DataFrame a BigQuery con un esquema predefinido. La función toma tres parámetros: "df" que es el DataFrame, "dataset_id" que es el ID del dataset, y "table_name" que es el nombre de la tabla. La función obtiene el cliente de BigQuery. Construye el nombre completo de la tabla. Define un esquema donde todas las columnas son STRING excepto "fecha_lectura" que es TIMESTAMP. El esquema incluye las columnas base como "cod_mpio", "Municipio", "Total", columnas de IPM, y todas las columnas de indicadores I1 a I15. Crea una configuración de job con "write_disposition" como "WRITE_TRUNCATE" para reemplazar la tabla si existe, y el esquema definido. Carga el DataFrame a BigQuery usando "load_table_from_dataframe" y espera a que termine. Imprime un mensaje de confirmación con el número de filas cargadas.

## 9. Función load_dataframe_to_bq

La función "load_dataframe_to_bq" es una función pública que llama a "_load_df_to_bq". Esta función proporciona una interfaz pública para cargar DataFrames a BigQuery sin exponer los detalles internos de la implementación.

## 10. Función cleanup_temp_paths

La función "cleanup_temp_paths" elimina los archivos temporales indicados, ignorando valores "None" o paths vacíos. La función toma un parámetro "paths" que es un iterable de rutas de archivos. Itera sobre cada path y si no es "None" o vacío, intenta eliminarlo usando "os.unlink". Si hay un error al eliminar un archivo, imprime una advertencia pero no detiene la ejecución. Esta función es importante para limpiar recursos y evitar que se acumulen archivos temporales en el sistema.

## 11. Filosofía de Transformación Mínima

El módulo sigue una filosofía de transformación mínima en la capa bronze. Todas las columnas se convierten a STRING para preservar los valores originales, incluso si normalmente serían numéricas. Esto permite que valores problemáticos como letras, espacios, o caracteres especiales se preserven tal como están en el archivo original. Las transformaciones y limpiezas completas, incluyendo conversiones de tipos y validaciones, se realizan posteriormente en la capa silver usando dbt. Esta separación de responsabilidades asegura que los datos originales siempre estén disponibles en bronze para auditoría y debugging, mientras que silver contiene datos limpios y transformados listos para análisis.

## 12. Esquema de BigQuery

El esquema de BigQuery para la tabla bronze está predefinido y especifica que todas las columnas son STRING excepto "fecha_lectura" que es TIMESTAMP. El esquema incluye sesenta y siete columnas en total: dos identificadores, una columna de total, cuatro columnas de IPM, y sesenta columnas de indicadores. Cada indicador tiene cuatro variantes: dos con sufijo "_Abs" para valores absolutos y dos con sufijo "_Porc" para porcentajes, y cada una tiene variantes "Con_Privacion" y "Sin_Privacion". Este esquema está diseñado para coincidir exactamente con la estructura esperada del archivo Excel de IPM.

## 13. Cómo se Usa en el Código

El módulo se usa principalmente desde los DAGs de Airflow que orquestan el proceso de carga. Los DAGs importan las funciones necesarias y las llaman en secuencia: primero "ensure_dataset" para asegurar que el dataset existe, luego "get_latest_excel_from_gcs_folder" para obtener el archivo más reciente, luego "download_excel_from_gcs" para descargarlo, luego "transform_excel" para transformarlo, y finalmente "load_dataframe_to_bq" para cargarlo a BigQuery. Los archivos temporales se limpian usando "cleanup_temp_paths" al final del proceso.

## 14. Notas Importantes

Es importante que el archivo Excel tenga exactamente la estructura esperada con al menos el número mínimo de columnas. Si el archivo tiene una estructura diferente, la función "transform_excel" puede fallar o producir resultados incorrectos. El código asume que la primera fila después del encabezado contiene totales y debe ser eliminada. Si la estructura del archivo cambia, esta lógica puede necesitar ajustes. El esquema de BigQuery está hardcodeado en el código, por lo que si cambia la estructura del archivo Excel, el esquema también debe actualizarse. La función siempre usa "WRITE_TRUNCATE" para reemplazar la tabla completa, por lo que cada ejecución sobrescribe los datos anteriores. Esto es intencional para la capa bronze donde se mantiene solo la versión más reciente de los datos.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/ipm/ipm_load.py"  
Versión del archivo: 3.0

