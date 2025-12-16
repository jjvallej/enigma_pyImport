# Documentación: modules/evaplan/evaplan_load.py

## 1. Información General

El archivo "modules/evaplan/evaplan_load.py" es un módulo Python que se encarga de extraer datos JSON desde Google Cloud Storage y cargarlos en la capa bronze de BigQuery para las fuentes de Evaplan. Este módulo es más complejo que otros módulos de carga porque maneja múltiples JSON por carpeta con fechas diferentes, extrae datos de estructuras JSON anidadas, normaliza los datos a DataFrames de pandas, y aplica lógica inteligente de carga que elimina registros duplicados del mismo día y "peri_idp" antes de cargar nuevos datos. Su propósito principal es cargar los datos JSON de la API de Evaplan a BigQuery de manera eficiente y sin duplicados.

## 2. Propósito y Funcionalidad

El archivo "evaplan_load.py" cumple varias funciones críticas. En primer lugar, asegura que el dataset bronze exista. En segundo lugar, obtiene todos los archivos JSON de la fecha actual desde las carpetas de cada fuente, buscando archivos de las últimas dos fechas posibles para evitar problemas de zona horaria. En tercer lugar, para cada JSON, extrae los datos relevantes según el tipo de fuente, normaliza estructuras JSON anidadas a DataFrames planos, agrega "fecha_lectura" y "peri_idp" a cada registro, y combina todos los registros de todos los JSON de la fecha actual. En cuarto lugar, si la tabla existe, elimina registros del mismo día y mismo "peri_idp" antes de cargar para evitar duplicados. Finalmente, carga los datos combinados a BigQuery con esquema apropiado donde "peri_idp" es INT64, "fecha_lectura" es TIMESTAMP, y todas las demás columnas son STRING.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports, configuración, y funciones helper para obtener clientes. La segunda sección contiene funciones helper como "ensure_dataset", "extract_date_from_filename", "extract_peri_idp_from_filename", y "get_json_files_from_current_date". La tercera sección contiene funciones de procesamiento como "download_json_from_gcs", "extract_data_from_json", "normalize_json_records", y "get_table_schema_from_dataframe". La cuarta sección contiene funciones de carga como "table_exists", "delete_records_by_date_and_peri_idp", "load_dataframe_to_bq", y "load_json_files_to_bq". Finalmente, la quinta sección contiene funciones helper para obtener rutas de carpetas y nombres de tablas desde "config.yaml".

## 4. Función get_json_files_from_current_date

La función "get_json_files_from_current_date" obtiene todos los archivos JSON más recientes en una carpeta de GCS, buscando archivos de las últimas dos fechas posibles para evitar problemas de zona horaria. La función lista todos los blobs en la carpeta, filtra archivos JSON, extrae la fecha del nombre del archivo usando expresiones regulares, extrae el "peri_idp" del nombre del archivo si está presente, filtra solo archivos de las fechas de hoy o ayer, ordena por fecha de creación en orden descendente, y retorna una lista de tuplas con URI, fecha, y "peri_idp". Esta función es importante porque permite cargar solo los datos más recientes sin procesar archivos antiguos.

## 5. Función extract_data_from_json

La función "extract_data_from_json" extrae los datos relevantes del JSON según el tipo de fuente. La función valida que el JSON tenga "success" como "True", obtiene el "peri_idp" del JSON si no se proporciona en el nombre del archivo, accede a la sección "data" del JSON, y extrae el array correspondiente según la fuente: "periodos" para periodos, "AvanceMR" para avance_mr, "AvanceMP" para avance_mp, "AvanceXSubprograma" para avance_x_subprograma, "AvanceGeneral" para avance_general, "avanceSubProgramas" para avance_subprogramas, y "avanceProgramas" para avance_programas. Retorna una tupla con la lista de registros y el "peri_idp". Esta función es crítica porque cada tipo de fuente tiene una estructura JSON diferente y necesita extraer los datos del lugar correcto.

## 6. Función normalize_json_records

La función "normalize_json_records" normaliza una lista de diccionarios JSON a un DataFrame de pandas, manejando estructuras anidadas expandiéndolas y agregando "fecha_lectura" y "peri_idp". La función usa "pd.json_normalize" para expandir estructuras anidadas automáticamente, maneja "peri_idp" convirtiéndolo a INT64 si ya existe en los datos o agregándolo si se proporciona externamente, convierte todas las demás columnas a STRING para preservar valores originales en bronze, y agrega "fecha_lectura" con timestamp UTC. Esta función es importante porque los JSON de la API pueden tener estructuras anidadas que necesitan ser aplanadas para BigQuery.

## 7. Función delete_records_by_date_and_peri_idp

La función "delete_records_by_date_and_peri_idp" elimina registros de una tabla que tengan la misma fecha de lectura y mismo "peri_idp". La función verifica que la tabla exista, convierte "fecha_lectura" a formato DATE para comparación por día, construye una query DELETE que filtra por fecha y "peri_idp" si se proporciona, o solo por fecha si no hay "peri_idp", y ejecuta la query. Esta función es importante para evitar duplicados cuando se ejecuta el DAG múltiples veces en el mismo día, asegurando que solo haya una versión de los datos por día y "peri_idp".

## 8. Función load_json_files_to_bq

La función "load_json_files_to_bq" es la función principal que carga JSON de la fecha actual a BigQuery. La función obtiene la ruta de la carpeta para la fuente desde "config.yaml", obtiene todos los JSON de la fecha actual, verifica si la tabla existe, procesa todos los JSON extrayendo datos y "peri_idp", normaliza y combina todos los registros en un solo DataFrame, elimina registros existentes del mismo día y "peri_idp" si la tabla existe, y carga los datos combinados a BigQuery usando "WRITE_APPEND" si la tabla existe o "WRITE_TRUNCATE" si no existe. Esta función maneja toda la lógica compleja de carga inteligente.

## 9. Lógica de Carga Inteligente

El módulo implementa lógica de carga inteligente que evita duplicados y maneja múltiples JSON del mismo día. Si hay múltiples JSON con el mismo "peri_idp" y fecha, los combina en un solo DataFrame antes de cargar. Si la tabla ya existe y hay registros del mismo día y "peri_idp", los elimina antes de cargar los nuevos para evitar duplicados. Esta lógica asegura que cada "peri_idp" tenga solo una versión de datos por día, incluso si el DAG se ejecuta múltiples veces o si hay múltiples JSON con el mismo "peri_idp".

## 10. Manejo de peri_idp

El módulo maneja "peri_idp" de manera especial porque es una columna crítica para identificar periodos. "peri_idp" se mantiene como INT64 en lugar de STRING como las demás columnas, lo cual es importante para joins y filtros eficientes. El módulo intenta obtener "peri_idp" del nombre del archivo primero, y si no está disponible, lo obtiene del JSON. Si tampoco está en el JSON, se agrega desde el parámetro de la función. Esta flexibilidad permite manejar diferentes estructuras de archivos y JSON.

## 11. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de Evaplan. Los DAGs llaman a "load_json_files_to_bq" para cada fuente, pasando el nombre de la fuente como "periodos", "avance_mr", "avance_mp", "avance_x_subprograma", "avance_general", "avance_subprogramas", o "avance_programas", el nombre del bucket desde "DEFAULT_BUCKET_NAME", el dataset desde "DATASET_ID_BRONZE", y el nombre de la tabla que se obtiene automáticamente desde "config.yaml" usando "get_table_name_for_fuente". La función maneja toda la lógica de carga automáticamente.

## 12. Notas Importantes

El módulo busca solo archivos de la fecha actual o ayer para evitar procesar archivos antiguos. Si se necesita procesar archivos de otras fechas, la lógica debe ajustarse. El módulo combina todos los JSON del mismo día antes de cargar, lo cual es eficiente pero significa que si hay JSON con datos conflictivos, se combinarán sin validación. El módulo elimina registros existentes del mismo día y "peri_idp" antes de cargar, lo cual es apropiado para datos que se actualizan diariamente pero puede no ser apropiado para datos históricos que deben acumularse. El esquema se infiere automáticamente del DataFrame, pero "peri_idp" y "fecha_lectura" tienen tipos específicos que se manejan explícitamente.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/evaplan/evaplan_load.py"  
Versión del archivo: 3.0

