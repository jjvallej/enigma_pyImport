# Documentación: config/config.yaml

## 1. Información General

El archivo "config/config.yaml" es un archivo de configuración en formato YAML que sirve como el punto central de configuración de toda la plantilla. Este archivo es cargado automáticamente por el módulo "config_loader.py" y se accede en el código mediante "modules.config.CONF". Su propósito principal es eliminar valores hardcodeados del código, permitiendo que todo el sistema sea configurable mediante este único archivo.

## 2. Propósito y Funcionalidad

El archivo "config.yaml" cumple varias funciones críticas en el sistema. En primer lugar, elimina la necesidad de valores hardcodeados en el código fuente, permitiendo ajustar el comportamiento del sistema sin modificar código. Todos los parámetros configurables están definidos aquí, desde URLs de fuentes de datos hasta nombres de tablas y configuraciones de ambientes.

En segundo lugar, proporciona soporte multi-ambiente, definiendo configuraciones específicas para diferentes ambientes como desarrollo, QA y producción. Esto permite que el mismo código funcione en diferentes contextos simplemente cambiando la configuración, sin necesidad de modificar el código fuente.

En tercer lugar, centraliza toda la configuración de las fuentes de datos. Cada fuente tiene su propia sección en el archivo que contiene URLs, rutas en Google Cloud Storage, nombres de tablas en BigQuery, y parámetros específicos necesarios para su procesamiento.

Finalmente, facilita la portabilidad entre proyectos. Para migrar la plantilla a un nuevo proyecto de Google Cloud Platform, solo se necesita actualizar los valores en este archivo, específicamente en la sección environments, sin requerir modificaciones al código fuente.

## 3. Estructura del Archivo

El archivo está organizado en secciones principales. La primera sección es "global_config", que contiene configuración global que aplica a todo el sistema, como el modo debug. La segunda sección es "environments", que define las configuraciones específicas para cada ambiente, incluyendo "project_id", "location", "bucket_name", y los nombres de los datasets para las capas bronze, silver y gold.

Luego vienen las secciones específicas de cada fuente de datos. La sección "evaplan" contiene la configuración para la fuente de datos Evaplan, que es una API REST. La sección "idc" contiene la configuración para el Índice de Desempeño de la Competitividad, que incluye datos principales y un diccionario. La sección "ipm" contiene la configuración para el Índice de Pobreza Multidimensional. La sección "ipm_sisben" contiene la configuración para datos del SISBEN relacionados con IPM, que utiliza tablas externas. Finalmente, la sección "idi" contiene la configuración para el Índice de Desempeño Integral, que soporta múltiples años.

## 4. Sección global_config

La sección "global_config" contiene configuración global que aplica a todo el sistema. Esta sección actualmente contiene una sola variable.

La variable "debug" es un valor booleano que controla el modo debug del sistema. Cuando está en "true", el sistema imprime mensajes de depuración adicionales en los logs durante la ejecución de los DAGs y módulos. Esto incluye información sobre la carga de configuración, rutas de archivos encontradas, valores de variables, y otros detalles técnicos que ayudan a diagnosticar problemas. Cuando está en "false", solo se imprimen mensajes de error y advertencias críticas. Esta configuración se lee al inicio del sistema y afecta el comportamiento general de logging en todos los módulos.

## 5. Sección environments

La sección "environments" es una de las más importantes del archivo, ya que define las configuraciones específicas para cada ambiente. Esta sección contiene tres subsecciones: "dev" para el ambiente de desarrollo, "prod" para el ambiente de producción, y "local" para el ambiente local de desarrollo.

Cada ambiente debe tener definidos los siguientes campos. La variable "project_id" es un string que contiene el ID único del proyecto de Google Cloud Platform donde se ejecutan los recursos. Este ID se usa para todas las operaciones con BigQuery y Cloud Storage, como crear datasets, cargar datos, y ejecutar queries. Por ejemplo, "datagov-473122" identifica un proyecto específico en GCP. Este valor debe coincidir exactamente con el ID del proyecto en la consola de GCP.

La variable "location" es un string que especifica la región geográfica de Google Cloud Platform donde se encuentran los recursos como BigQuery datasets y Cloud Storage buckets. Este valor determina dónde se almacenan físicamente los datos y afecta la latencia y los costos. Ejemplos comunes son "us-central1" para la región central de Estados Unidos, "us-east1" para la costa este de Estados Unidos, o "us-west1" para la costa oeste. Esta región debe ser consistente con la ubicación real de los recursos en GCP.

La variable "bucket_name" es un string con el nombre del bucket de Google Cloud Storage que se usa como Data Lake para almacenar archivos raw. Este bucket contiene todas las carpetas y archivos de las diferentes fuentes de datos antes de ser cargados a BigQuery. Por ejemplo, "datalake_gdv_dev" es el nombre del bucket de desarrollo. Este bucket debe existir en GCP y el Service Account debe tener permisos de lectura y escritura.

La variable "dataset_bronze" es un string con el nombre del dataset de BigQuery para la capa Bronze, donde se almacenan los datos en su forma original sin transformaciones. Este dataset contiene todas las tablas raw de las diferentes fuentes. Por ejemplo, "bronze_dpt_planeacion_municipal_dev" es el dataset Bronze del ambiente de desarrollo. Este dataset se crea automáticamente si no existe cuando se ejecutan los DAGs de carga.

La variable "dataset_silver" es un string con el nombre del dataset de BigQuery para la capa Silver, donde se almacenan los datos limpiados y normalizados después de las transformaciones. Este dataset contiene vistas y tablas que resultan de las transformaciones dbt. Por ejemplo, "silver_dpt_planeacion_municipal_dev" es el dataset Silver del ambiente de desarrollo. Las transformaciones dbt escriben en este dataset.

La variable "dataset_gold" es un string con el nombre del dataset de BigQuery para la capa Gold, donde se almacenan los datos agregados y optimizados para consumo analítico. Este dataset contiene tablas de hechos y dimensiones listas para dashboards y reportes. Por ejemplo, "gold_dpt_planeacion_municipal_dev" es el dataset Gold del ambiente de desarrollo. Las transformaciones finales de dbt escriben en este dataset.

El ambiente se selecciona mediante la variable de entorno "ENVIRONMENT" o el valor por defecto en "modules/config.py". El código lee automáticamente la sección correspondiente según el ambiente seleccionado. Es importante que cada ambiente tenga todos los campos definidos y que los valores sean consistentes con los recursos reales en GCP. Para migrar a un nuevo proyecto, solo se necesita actualizar estos valores en la sección correspondiente.

## 6. Sección evaplan

La sección "evaplan" contiene la configuración completa de la fuente de datos Evaplan, que es una API REST que proporciona información sobre evaluación y planeación municipal.

La variable "auth_endpoint" contiene la URL completa del endpoint de autenticación de la API Evaplan. Esta URL se usa para obtener un token de autenticación antes de hacer peticiones a los otros endpoints. El sistema hace una petición POST a esta URL con las credenciales para obtener el token. Por ejemplo, "http://207.246.89.62/ApiEvaplan/auth/login" es el endpoint donde se autentica el sistema.

La variable "api_base_url" contiene la URL base de la API sin endpoints específicos. Esta URL se combina con las rutas de los endpoints individuales para formar las URLs completas. Por ejemplo, "http://207.246.89.62/ApiEvaplan" es la URL base, y se combina con "/datos/periodos" para formar la URL completa del endpoint de periodos.

La variable "endpoints" es un objeto que contiene un diccionario con todos los endpoints disponibles de la API. Cada clave es un nombre descriptivo del endpoint y el valor es la ruta relativa que se concatena con "api_base_url". La clave "periodos" tiene el valor "/datos/periodos" y se usa para obtener información sobre periodos. La clave "avance_mr" tiene el valor "/datos/AvanceMR" y se usa para obtener avances de metas de resultado. La clave "avance_mp" tiene el valor "/datos/AvanceMP" y se usa para obtener avances de metas de producto. La clave "avance_x_subprograma" tiene el valor "/datos/AvanceXSubprograma" y se usa para obtener avances por subprograma. La clave "avance_general" tiene el valor "/datos/AvanceGeneral" y se usa para obtener avances generales.

La variable "gcs_base_folder" contiene la ruta base en Google Cloud Storage donde se almacenan todos los archivos JSON descargados de la API Evaplan. Esta ruta se usa como carpeta raíz para organizar los archivos por endpoint. Por ejemplo, "data_staging/dpt_planeacion_municipal/api_evaplan" es la carpeta base donde se guardan todos los archivos de Evaplan.

La variable "gcs_folders" es un objeto que define carpetas específicas dentro de "gcs_base_folder" para cada endpoint. Cada clave corresponde a un endpoint y el valor es el nombre de la subcarpeta donde se almacenan los archivos de ese endpoint. La clave "periodos" tiene el valor "periodos", creando la ruta completa "data_staging/dpt_planeacion_municipal/api_evaplan/periodos". La clave "avance_mr" tiene el valor "avance_mr", la clave "avance_mp" tiene "avance_mp", la clave "avance_x_subprograma" tiene "avance_x_subprograma", y la clave "avance_general" tiene "avance_general".

La variable "fuentes" es un array que contiene la lista de nombres de fuentes que se procesarán durante la ejecución del DAG de ingestión. Esta lista determina qué endpoints se consultarán y qué datos se descargarán. Cada elemento del array debe coincidir exactamente con una clave del objeto "endpoints". El array incluye "periodos", "avance_mr", "avance_mp", "avance_x_subprograma", y "avance_general". Si se quiere desactivar temporalmente un endpoint, se puede eliminar de este array.

La variable "tables" es un objeto que contiene los nombres de tablas en BigQuery organizados por capa de datos. El subcampo "bronze" contiene un objeto con los nombres de tablas en BigQuery para la capa Bronze, donde se almacenan los datos raw descargados de la API. Cada clave corresponde a un endpoint y el valor es el nombre de la tabla. La clave "periodos" tiene el valor "evaplan_api_periodos_raw_data", que es la tabla donde se cargan los datos raw del endpoint de periodos. La clave "avance_mr" tiene "evaplan_api_avance_mr_raw_data", la clave "avance_mp" tiene "evaplan_api_avance_mp_raw_data", la clave "avance_x_subprograma" tiene "evaplan_api_avance_x_subprograma_raw_data", y la clave "avance_general" tiene "evaplan_api_avance_general_raw_data".

El subcampo "silver" contiene un objeto similar con los nombres de tablas para la capa Silver, donde se almacenan los datos transformados y limpiados. La clave "periodos" tiene el valor "evaplan_api_periodos_transformed_data", la clave "avance_mr" tiene "evaplan_api_avance_mr_transformed_data", la clave "avance_mp" tiene "evaplan_api_avance_mp_transformed_data", la clave "avance_x_subprograma" tiene "evaplan_api_avance_x_subprograma_transformed_data", y la clave "avance_general" tiene "evaplan_api_avance_general_transformed_data".

El subcampo "gold" contiene un objeto con los nombres de tablas para la capa Gold, donde se almacenan los datos agregados y optimizados para análisis. Nota que "periodos" no tiene tabla Gold, solo los avances. La clave "avance_mr" tiene el valor "evaplan_api_avance_mr_processed_data", la clave "avance_mp" tiene "evaplan_api_avance_mp_processed_data", la clave "avance_x_subprograma" tiene "evaplan_api_avance_x_subprograma_processed_data", y la clave "avance_general" tiene "evaplan_api_avance_general_processed_data".

La variable "credentials" es un objeto que contiene las credenciales de autenticación necesarias para acceder a la API Evaplan. El subcampo "usuario" contiene el nombre de usuario que se usa para autenticarse en la API. Este valor se envía en el cuerpo de la petición POST al endpoint de autenticación. Por ejemplo, "usuario_api" es el usuario configurado. El subcampo "password" contiene la contraseña o hash de contraseña que se usa para autenticarse. Este valor también se envía en el cuerpo de la petición de autenticación. El formato puede ser texto plano o hash según lo que requiera la API. Por ejemplo, "ef92b778bafe771e89245b89ecbc08a44a4e166c06659911881f383d4473e94f" es un hash de contraseña.

Es importante que los endpoints definidos en el objeto "endpoints" coincidan exactamente con los nombres en el array "fuentes", y que las credenciales estén configuradas correctamente para que la API funcione. Los nombres de tablas deben seguir las convenciones de nomenclatura de BigQuery, que no permiten guiones ni espacios.

## 7. Sección idc

La sección "idc" contiene la configuración para el Índice de Desempeño de la Competitividad. Esta fuente requiere dos componentes: los datos principales desde Google Sheets y un diccionario desde Google Drive en formato CSV que se usa para normalizar los valores.

La variable "drive_url" contiene la URL completa de Google Sheets que contiene los datos principales de IDC. Esta URL debe ser una URL de edición o visualización de Google Sheets que el sistema puede acceder usando la API de Google Drive. El sistema descarga este archivo como Excel y luego lo procesa. Por ejemplo, "https://docs.google.com/spreadsheets/d/1ABWswq3dHMP74u-cKW9OaCTI3X3bfegF/edit?usp=sharing&ouid=114741032353429683392&rtpof=true&sd=true" es la URL del Google Sheet con los datos principales.

La variable "dictionary_drive_url" contiene la URL de Google Drive del archivo CSV del diccionario de datos. Este diccionario contiene mapeos entre valores originales y valores normalizados, y se usa para estandarizar los datos durante la transformación. El sistema descarga este archivo CSV desde Google Drive. Por ejemplo, "https://drive.google.com/file/d/1Np3t7wFletxcMxqoeDrNhlvxqzaOQpLr/view?usp=sharing" es la URL del archivo CSV del diccionario.

La variable "gcs_folder" contiene la ruta en Google Cloud Storage donde se almacenan los archivos descargados de IDC antes de ser cargados a BigQuery. Esta carpeta contiene tanto el archivo Excel de datos principales como el archivo CSV del diccionario. Por ejemplo, "data_staging/dpt_planeacion_municipal/idc" es la carpeta donde se guardan los archivos de IDC.

La variable "tables" es un objeto que contiene los nombres de tablas en BigQuery organizados por tipo y capa. El subcampo "dictionary" contiene el nombre de la tabla en BigQuery que almacena el diccionario de datos. Esta tabla se crea desde el archivo CSV del diccionario y se usa como referencia durante las transformaciones. Por ejemplo, "dim_idc" es el nombre de la tabla del diccionario, que actúa como una tabla de dimensiones.

El subcampo "raw_data_mapping" contiene un objeto con los nombres de tablas de mapeo de datos raw en la capa Bronze. Estas tablas almacenan los datos originales organizados por tipo de mapeo. La clave "Dato_original" tiene el valor "idc_raw_data_dato_original", que es la tabla donde se almacenan los datos originales sin normalizar. La clave "Valor_normalizado" tiene el valor "idc_raw_data_valor_normalizado", que es la tabla donde se almacenan los valores normalizados. La clave "Valor_ranking" tiene el valor "idc_raw_data_valor_ranking", que es la tabla donde se almacenan los valores de ranking.

El subcampo "silver" contiene un objeto con los nombres de tablas en la capa Silver, donde se almacenan los datos transformados. Estas tablas contienen los datos después de aplicar las transformaciones y normalizaciones usando el diccionario. La clave "dato_original" tiene el valor "idc_transformed_data_dato_original", que es la tabla Silver con datos originales transformados. La clave "valor_normalizado" tiene el valor "idc_transformed_data_valor_normalizado", que es la tabla Silver con valores normalizados transformados. La clave "valor_ranking" tiene el valor "idc_transformed_data_valor_ranking", que es la tabla Silver con valores de ranking transformados.

El subcampo "gold" contiene el nombre de la tabla de hechos en la capa Gold, donde se almacenan los datos agregados y listos para análisis. Esta tabla combina y agrega información de las diferentes tablas Silver para crear una vista consolidada. Por ejemplo, "fact_idc" es el nombre de la tabla de hechos que contiene los datos finales de IDC para consumo analítico.

El diccionario se usa para normalizar y mapear los valores de los datos principales, por lo que es esencial que ambos componentes estén configurados correctamente. Sin el diccionario, los datos no se pueden normalizar adecuadamente.

## 8. Sección ipm

La sección "ipm" contiene la configuración para el Índice de Pobreza Multidimensional del DANE. Esta es una fuente relativamente simple que solo requiere un Google Sheet con los datos de IPM.

La variable "drive_url" contiene la URL completa de Google Sheets que contiene los datos de IPM. Esta URL debe ser accesible y el sistema la usa para descargar el archivo como Excel. El sistema procesa este archivo y carga los datos a BigQuery. Por ejemplo, "https://docs.google.com/spreadsheets/d/1qrwNEM4NWgVwLPiVKTA_rGNcp9y5PMou/edit?usp=sharing&ouid=114741032353429683392&rtpof=true&sd=true" es la URL del Google Sheet con los datos de IPM.

La variable "gcs_folder" contiene la ruta en Google Cloud Storage donde se almacena el archivo Excel descargado de IPM antes de ser procesado y cargado a BigQuery. Esta carpeta sirve como almacenamiento temporal del archivo raw. Por ejemplo, "data_staging/dpt_planeacion_municipal/ipm" es la carpeta donde se guarda el archivo de IPM.

La variable "tables" es un objeto que contiene los nombres de tablas en BigQuery por capa. El subcampo "bronze" contiene el nombre de la tabla en BigQuery para la capa Bronze, donde se almacenan los datos raw tal como vienen del Google Sheet sin transformaciones. Por ejemplo, "ipm_raw_data" es la tabla donde se cargan los datos originales de IPM.

El subcampo "silver" contiene el nombre de la tabla en BigQuery para la capa Silver, donde se almacenan los datos después de aplicar limpieza, normalización y validación de tipos de datos. Esta tabla contiene los datos listos para análisis pero aún no agregados. Por ejemplo, "ipm_transformed_data" es la tabla Silver con los datos transformados de IPM.

El subcampo "gold" contiene el nombre de la tabla de hechos en la capa Gold, donde se almacenan los datos agregados y optimizados para consumo analítico. Esta tabla contiene métricas y agregaciones finales listas para dashboards y reportes. Por ejemplo, "FACT_DANE" es la tabla Gold que contiene los datos finales de IPM para análisis.

La transformación de IPM incluye limpieza de datos, normalización de formatos, validación de tipos de datos, y corrección de errores para preparar los datos para análisis.

## 9. Sección ipm_sisben

La sección "ipm_sisben" contiene la configuración para la fuente IPM SISBEN, que utiliza archivos Excel con múltiples hojas que se convierten a CSV y luego se crea una tabla externa en BigQuery que apunta directamente a los archivos en Google Cloud Storage.

La variable "drive_url" contiene la URL de Google Drive del archivo Excel que contiene los datos de IPM SISBEN. Este archivo Excel tiene múltiples hojas que se convierten a archivos CSV separados. El sistema descarga este archivo desde Google Drive. Por ejemplo, "https://docs.google.com/spreadsheets/d/1tUZvT4fC2PmeQrvTUHdUvjN8j3fdD53k/edit?usp=sharing&ouid=114741032353429683392&rtpof=true&sd=true" es la URL del archivo Excel.

La variable "gcs_folder" contiene la carpeta principal en Google Cloud Storage donde se almacenan los archivos CSV generados desde el Excel. Esta carpeta contiene los archivos CSV finales que se usan para crear la tabla externa. Por ejemplo, "data_staging/dpt_planeacion_municipal/ipm/sisben" es la carpeta donde se guardan los CSVs de IPM SISBEN.

La variable "gcs_temp_folder" contiene la carpeta temporal en Google Cloud Storage que se usa para almacenamiento intermedio durante el procesamiento. Esta carpeta puede contener archivos temporales mientras se procesa el Excel y se generan los CSVs. Por ejemplo, "data_staging/dpt_planeacion_municipal/tmp_sisben" es la carpeta temporal.

La variable "table_name" contiene un nombre de tabla que puede ser legacy y no usarse en todas las implementaciones actuales. Este valor puede estar presente por compatibilidad con versiones anteriores del código. Por ejemplo, "ipm_sisben_raw" es un nombre de tabla que puede no usarse.

La variable "excel_file_path" contiene la ruta del archivo Excel en el bucket sin incluir el nombre del archivo. El sistema busca el archivo más reciente en esta ruta basándose en la fecha de modificación. Esto permite que el sistema siempre use la versión más actualizada del archivo sin necesidad de actualizar la configuración cada vez que se sube un nuevo archivo. Por ejemplo, "data_staging/dpt_planeacion_municipal/ipm/sisben" es la ruta donde se busca el archivo Excel más reciente.

La variable "csv_files" es un array que contiene la lista de nombres de archivos CSV que se generan desde las hojas del Excel. Cada hoja del Excel se convierte en un archivo CSV con el nombre especificado aquí. El array incluye "hoja1_ipm_sisben.csv" que corresponde a la primera hoja del Excel, "hoja2_ipm_sisben.csv" que corresponde a la segunda hoja, y "hoja3_ipm_sisben.csv" que corresponde a la tercera hoja. Estos nombres deben coincidir con los nombres de los archivos CSV que realmente se generan.

La variable "external_table" es un objeto que contiene la configuración necesaria para crear la tabla externa en BigQuery que apunta a los archivos CSV en Google Cloud Storage. El subcampo "gcs_path" contiene la ruta en Google Cloud Storage con un wildcard que permite leer todos los archivos CSV que coincidan con el patrón. Esta ruta se usa en la sentencia CREATE EXTERNAL TABLE de BigQuery. Por ejemplo, "data_staging/dpt_planeacion_municipal/ipm/sisben/*.csv" hace que la tabla externa lea todos los archivos CSV en esa carpeta. El asterisco es un comodín que coincide con cualquier nombre de archivo que termine en punto csv.

El subcampo "skip_leading_rows" contiene el número de filas a saltar al inicio de cada archivo CSV al leerlo. Generalmente se usa 1 para saltar la fila de encabezados, pero puede ser mayor si hay múltiples filas de encabezado o información que no es datos. Por ejemplo, 1 significa que se salta la primera fila de cada CSV.

El subcampo "field_delimiter" contiene el carácter que se usa como delimitador de campos en los archivos CSV. Este valor se usa para indicar a BigQuery cómo separar las columnas en los archivos CSV. Por ejemplo, coma indica que los campos están separados por comas.

El subcampo "allow_quoted_newlines" es un valor booleano que indica si se permiten saltos de línea dentro de campos que están entrecomillados en los CSVs. Cuando está en true, BigQuery interpreta correctamente los saltos de línea dentro de campos entrecomillados. Cuando está en false, los saltos de línea dentro de campos causan errores. Generalmente se usa true para manejar datos de texto que pueden contener saltos de línea.

La variable "tables" es un objeto que contiene los nombres de tablas en BigQuery por capa. El subcampo "bronze" contiene el nombre de la tabla externa en BigQuery para la capa Bronze. Esta tabla externa apunta directamente a los archivos CSV en Google Cloud Storage sin cargar los datos a BigQuery. Por ejemplo, "ipm_sisben_raw" es el nombre de la tabla externa que lee los CSVs directamente desde GCS.

El subcampo "silver" contiene el nombre de la tabla en BigQuery para la capa Silver, donde se almacenan los datos transformados después de leer desde la tabla externa Bronze. Esta tabla contiene los datos limpiados y normalizados. Por ejemplo, "ipm_sisben_transformed_data" es la tabla Silver con los datos transformados.

El subcampo "gold" contiene el nombre de la tabla de hechos en la capa Gold, donde se almacenan los datos agregados y optimizados para análisis. Por ejemplo, "FACT_SISBEN" es la tabla Gold que contiene los datos finales de IPM SISBEN para consumo analítico.

Es importante notar que IPM SISBEN utiliza una tabla externa en BigQuery que apunta directamente a los archivos CSV en Google Cloud Storage, en lugar de cargar los datos directamente a BigQuery. Esto significa que los datos se leen desde GCS cada vez que se consulta la tabla, lo que es útil para datos que se actualizan frecuentemente. El wildcard asterisco punto csv en "external_table.gcs_path" permite que la tabla externa lea automáticamente todos los archivos CSV nuevos que se agreguen a la carpeta. Los archivos CSV se generan desde un archivo Excel con múltiples hojas, donde cada hoja se convierte en un CSV separado.

## 10. Sección idi

La sección "idi" contiene la configuración para el Índice de Desempeño Integral, que soporta múltiples años de datos y crea una tabla consolidada que combina información de todos los años.

La variable "config_drive_url" contiene la URL de Google Sheets que actúa como archivo de configuración y contiene links a los datos de IDI organizados por año. Este Google Sheet no contiene los datos directamente, sino que contiene enlaces o referencias a otros archivos o hojas que contienen los datos de cada año. El sistema lee este archivo para encontrar los links a los datos de cada año. Por ejemplo, "https://docs.google.com/spreadsheets/d/1_mMOzghd048cmvasxStauXFXpImlaPod/edit?usp=sharing&ouid=114741032353429683392&rtpof=true&sd=true" es la URL del Google Sheet de configuración.

La variable "gcs_base_folder" contiene la carpeta base en Google Cloud Storage donde se almacenan todos los archivos de IDI organizados por año. Esta carpeta sirve como raíz para almacenar los archivos descargados de cada año. Por ejemplo, "data_staging/dpt_planeacion_municipal/idi" es la carpeta base donde se guardan los archivos de IDI.

La variable "link_name_keywords" es un array que contiene palabras clave que se usan para identificar y filtrar los links relevantes en el Google Sheet de configuración. El sistema busca en el Google Sheet links o referencias que contengan estas palabras clave en su nombre o descripción, y solo procesa esos links. Esto permite que el Google Sheet contenga múltiples links pero solo se procesen los relevantes. El array incluye "Resultados Territorio" que identifica links relacionados con resultados por territorio, y "Resultados consolidados" que identifica links relacionados con resultados consolidados. Cualquier link que no contenga estas palabras clave será ignorado.

La variable "tables" es un objeto que contiene los nombres de tablas en BigQuery. El subcampo "bronze_prefix" contiene el prefijo que se usa para generar los nombres de las tablas Bronze. A este prefijo se le agrega automáticamente el año al final para crear nombres únicos por año. Por ejemplo, con el valor "idi_raw_data_territorio" el sistema genera tablas como "idi_raw_data_territorio_2024" para los datos de 2024, "idi_raw_data_territorio_2023" para los datos de 2023, y así sucesivamente para cada año encontrado en los links.

El subcampo "silver" contiene un array con la lista completa de nombres de tablas en la capa Silver. A diferencia de Bronze donde se genera dinámicamente, aquí se deben especificar explícitamente todos los nombres de tablas Silver que existen. El array incluye "idi_transformed_data_2023" que es la tabla Silver con datos transformados del año 2023, "idi_transformed_data_2024" que es la tabla Silver con datos transformados del año 2024, y "idi_transformed_data_consolidated" que es la tabla Silver consolidada que combina datos de todos los años. Esta lista debe mantenerse actualizada cuando se agregan nuevos años.

El subcampo "gold" contiene el nombre de la tabla de hechos en la capa Gold, donde se almacenan los datos finales agregados y optimizados para análisis. Esta tabla combina y agrega información de todas las tablas Silver para crear una vista consolidada final. Por ejemplo, "idi_processed_data" es el nombre de la tabla Gold que contiene los datos finales de IDI para consumo analítico.

La variable "transform_config" es un objeto que contiene parámetros de configuración específicos para las transformaciones de IDI. El subcampo "skip_rows" contiene el número de filas a saltar al inicio de cada archivo al leerlo. Estas filas generalmente contienen encabezados, información de metadatos, o información que no es parte de los datos. Por ejemplo, 2 significa que se saltan las primeras dos filas de cada archivo.

El subcampo "sheet_name" contiene el nombre de la hoja específica a leer dentro de los archivos Excel. Si el valor es "null", el sistema lee automáticamente la primera hoja del archivo. Si se especifica un nombre, el sistema busca y lee solo esa hoja específica. Por ejemplo, "null" significa que siempre se lee la primera hoja disponible.

El subcampo "columns_to_drop" contiene una lista de nombres de columnas que se deben eliminar durante el procesamiento. Estas columnas se eliminan antes de cargar los datos a BigQuery. Si el valor es "null", no se elimina ninguna columna. Si se especifica una lista como array, todas las columnas con esos nombres se eliminan. Por ejemplo, "null" significa que se conservan todas las columnas del archivo original.

IDI soporta múltiples años, generando tablas separadas por año en Bronze y Silver. Se crea una tabla consolidada que combina datos de todos los años para análisis histórico. Los links a los datos se identifican usando las palabras clave definidas en "link_name_keywords", lo que permite que el sistema procese automáticamente nuevos años cuando se agregan links al Google Sheet de configuración.

## 11. Cómo se Usa en el Código

El archivo "config.yaml" es cargado automáticamente por el módulo "config_loader.py" al importar "modules.config". El proceso funciona de la siguiente manera. Primero, "config_loader.py" busca el archivo en múltiples ubicaciones posibles, incluyendo rutas relativas para desarrollo local y rutas comunes en Cloud Composer. Luego, carga el archivo YAML y lo convierte en un objeto "SimpleNamespace" que permite acceso mediante notación de puntos. Finalmente, expone la configuración globalmente como "modules.config.CONF".

En el código, se accede a la configuración usando notación de puntos. Por ejemplo, para acceder a la configuración de ambiente se usa "CONF.environments.dev.project_id" para obtener el "project_id" del ambiente de desarrollo, o "CONF.environments.dev.bucket_name" para obtener el nombre del bucket. Para acceder a la configuración de una fuente se usa "CONF.evaplan.api_base_url" para obtener la URL base de la API de Evaplan, o "CONF.ipm.tables.bronze" para obtener el nombre de la tabla Bronze de IPM.

El ambiente se selecciona mediante la variable de entorno "ENVIRONMENT" o el valor por defecto en "modules/config.py". El código lee automáticamente la sección correspondiente según el ambiente seleccionado, permitiendo que el mismo código funcione en diferentes ambientes sin modificaciones.

## 12. Ejemplos de Uso

Para migrar a un nuevo proyecto de Google Cloud Platform, solo se necesita actualizar la sección "environments". Por ejemplo, para configurar un nuevo ambiente de producción se actualizarían los valores de "project_id" con el ID del nuevo proyecto, "location" con la región deseada, "bucket_name" con el nombre del nuevo bucket, y los datasets con los nombres correspondientes. No se requiere modificar código fuente, solo actualizar los valores en "config.yaml".

Para agregar una nueva fuente de datos, se agrega una nueva sección al archivo. Por ejemplo, se podría agregar una sección "nueva_fuente" con campos como "source_url" para la URL de la fuente, "gcs_folder" para la carpeta en Google Cloud Storage, y "tables" con los nombres de tablas para cada capa. Luego se accede en el código como "CONF.nueva_fuente.source_url".

Para cambiar nombres de tablas, solo se modifica el valor en "config.yaml". Por ejemplo, para cambiar el nombre de la tabla Gold de IPM, se modificaría el valor de "ipm.tables.gold" a "FACT_DANE_NUEVO". El código leerá automáticamente el nuevo nombre sin necesidad de modificaciones adicionales.

## 13. Validación y Errores Comunes

Uno de los errores más comunes es que el archivo no se encuentre. Para resolverlo, se debe asegurar que "config.yaml" esté en "config/config.yaml" relativo a la raíz del proyecto. Otro error común es YAML inválido, que generalmente se debe a indentación incorrecta o valores sin comillas cuando contienen caracteres especiales. Se debe verificar que la indentación use espacios y no tabs, y que los valores estén entre comillas si contienen caracteres especiales.

Si el ambiente no está definido, el código fallará. Esto ocurre cuando "ENVIRONMENT" está configurado a un valor que no existe en la sección "environments". Se debe asegurar que exista la sección correspondiente para el ambiente seleccionado. También es importante que cada ambiente tenga todos los campos definidos, incluyendo "project_id", "location", "bucket_name", "dataset_bronze", "dataset_silver", y "dataset_gold".

Antes de desplegar, se recomienda validar que todos los ambientes tengan todos los campos requeridos, que los valores de "project_id", "bucket_name" y datasets existan realmente en Google Cloud Platform, que las URLs de fuentes sean accesibles, y que los nombres de tablas sigan las convenciones de nomenclatura de BigQuery, que no permiten guiones ni espacios.

## 14. Notas Importantes

Es fundamental no hardcodear valores en el código. Si se necesita un valor configurable, se debe agregar a "config.yaml" en lugar de hardcodearlo en el código. El archivo "config.yaml" debe estar versionado en el control de versiones, pero se debe considerar usar variables de entorno o secretos para credenciales sensibles en lugar de almacenarlas directamente en el archivo.

Se pueden usar comentarios en YAML, indicados con el símbolo numeral, para documentar valores que puedan no ser obvios. Es importante mantener una convención de nomenclatura consistente para tablas, carpetas y recursos, lo que facilita el mantenimiento y la comprensión del sistema.

---

Última actualización: 2025-01-XX  
Archivo documentado: "config/config.yaml"  
Versión del archivo: 3.0
