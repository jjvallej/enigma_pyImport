DOCUMENTACION DETALLADA DEL PIPELINE DE TRANSFORMACION IPM V2

Este documento describe en detalle el funcionamiento de los archivos principales del pipeline de transformacion de datos IPM Version 2. El pipeline procesa archivos Excel que contienen datos del Indice de Pobreza Multidimensional IPM desde Google Cloud Storage, los transforma y los carga en BigQuery siguiendo una arquitectura de capas bronze, silver y gold.

ARCHIVO: load_rawdata_ipmv2.py

Este archivo contiene todas las funciones auxiliares y de procesamiento que se utilizan en el pipeline. Es un modulo Python que proporciona las capacidades de extraccion, transformacion y carga de datos.

CONFIGURACION INICIAL

El archivo comienza con la importacion de librerias necesarias. Utiliza google.cloud para interactuar con BigQuery y Google Cloud Storage, pandas para manipulacion de datos, y librerias estandar de Python para manejo de archivos temporales y fechas.

Las constantes principales son:
- PROJECT_ID: Identificador del proyecto de Google Cloud, actualmente "datagov-473122"
- SA_PATH: Ruta al archivo de credenciales de servicio, ubicado en "/opt/airflow/include/sa.json"
- DEBUG: Variable booleana que controla si se muestran mensajes de depuracion detallados

FUNCIONES DE CLIENTES

El archivo define dos funciones privadas para crear clientes de Google Cloud:

_bq_client: Crea y retorna un cliente de BigQuery. Esta funcion establece la variable de entorno GOOGLE_APPLICATION_CREDENTIALS con la ruta del archivo de credenciales y luego crea un cliente de BigQuery asociado al proyecto configurado.

_gcs_client: Crea y retorna un cliente de Google Cloud Storage. Similar a la funcion anterior, establece las credenciales y crea un cliente de Storage para interactuar con buckets y archivos en GCS.

FUNCION ensure_dataset

Esta funcion verifica si existe un dataset en BigQuery y lo crea si no existe. Recibe como parametros el identificador del dataset y opcionalmente la ubicacion geografica, que por defecto es "us-central1".

El proceso es el siguiente: primero construye el nombre completo del dataset usando el PROJECT_ID y el dataset_id proporcionado. Luego intenta obtener el dataset usando el cliente de BigQuery. Si el dataset existe, imprime un mensaje de confirmacion si DEBUG esta activado. Si no existe, crea un nuevo dataset con la ubicacion especificada, le asigna una descripcion y lo crea en BigQuery. Finalmente imprime un mensaje indicando que el dataset fue creado exitosamente.

FUNCION get_latest_excel_from_gcs_folder

Esta funcion busca y retorna la URI del archivo Excel mas reciente en una carpeta especifica de Google Cloud Storage. Es una funcion critica porque permite que el pipeline procese automaticamente el archivo mas actualizado sin necesidad de especificar manualmente el nombre del archivo.

El proceso funciona asi: primero normaliza la ruta de la carpeta asegurandose de que termine con una barra diagonal. Luego obtiene el bucket de GCS usando el nombre proporcionado. Si hay un error al acceder al bucket, lanza una excepcion con un mensaje descriptivo.

Despues lista todos los blobs objetos en la carpeta especificada usando el prefijo de la ruta. Filtra los archivos para quedarse solo con aquellos que terminan en .xlsx y que no son directorios. Si no encuentra ningun archivo Excel, lanza una excepcion indicando que no se encontraron archivos.

Si encuentra archivos, los ordena por fecha de creacion de manera descendente, es decir, el mas reciente primero. Toma el primer archivo de la lista ordenada y construye su URI completa en formato gs://bucket/nombre_archivo.

Si DEBUG esta activado, imprime informacion sobre cuantos archivos se encontraron y muestra los primeros cinco con sus fechas de creacion, ademas de indicar cual archivo se selecciono. Finalmente retorna la URI completa del archivo mas reciente.

FUNCION download_excel_from_gcs

Esta funcion descarga un archivo Excel desde Google Cloud Storage a un archivo temporal en el sistema de archivos local. Recibe como parametro la URI completa del archivo en formato gs://bucket/nombre_archivo.

El proceso es: primero extrae el nombre del bucket y el nombre del blob desde la URI, dividiendo la cadena por las barras diagonales. Luego obtiene el blob especifico desde el bucket. Crea un archivo temporal usando tempfile.mkstemp con extension .xlsx, cierra el descriptor de archivo inmediatamente, y descarga el contenido del blob al archivo temporal. Si DEBUG esta activado, imprime la ruta donde se descargo el archivo. Finalmente retorna la ruta del archivo temporal descargado.

FUNCION transform_excel

Esta es una de las funciones mas importantes del modulo. Aplica las transformaciones necesarias al archivo Excel descargado y retorna un DataFrame de pandas listo para ser cargado en BigQuery.

El proceso de transformacion tiene varios pasos:

Primero lee el archivo Excel usando pandas.read_excel, especificando la hoja por indice y usando la primera fila como encabezados.

Segundo, elimina la primera fila de datos porque generalmente contiene totales o una fila guia que no es parte de los datos reales. Esto se hace usando iloc para seleccionar desde la fila 1 en adelante y luego reseteando el indice.

Tercero, si DEBUG esta activado, imprime los nombres de las columnas originales para inspeccion.

Cuarto, define las columnas esperadas en el orden correcto. El formato esperado es: cod_mpio, Municipio, Total, luego cuatro columnas para IPM IPM_Pobre_Abs, IPM_No_Pobre_Abs, IPM_Pobre_Porc, IPM_No_Pobre_Porc, y luego para cada indicador del I1 al I15, cuatro columnas: Con_Privacion_Abs, Sin_Privacion_Abs, Con_Privacion_Porc, Sin_Privacion_Porc.

Quinto, valida que el archivo tenga al menos el numero de columnas esperadas. Si tiene menos columnas, lanza una excepcion indicando cuantas columnas se esperaban y cuantas tiene el archivo. Si tiene mas columnas, emite una advertencia si DEBUG esta activado y toma solo las primeras columnas necesarias.

Sexto, renombra las columnas del DataFrame con los nombres esperados.

Septimo, y esto es muy importante, convierte todas las columnas a tipo STRING. Esta es una decision de diseno critica: en la capa bronze no se deben hacer conversiones de tipos porque se quiere preservar los datos originales exactamente como vienen, incluso si tienen letras, espacios, caracteres especiales o valores invalidos. Las transformaciones y limpiezas se haran posteriormente en la capa silver usando dbt. Especificamente, convierte cod_mpio y Municipio a string, y todas las columnas numericas tanto las que deberian ser enteros como las que deberian ser flotantes a string.

Octavo, agrega una columna llamada fecha_lectura con el timestamp actual en UTC. Esta columna permite rastrear cuando se procesaron los datos.

Finalmente retorna el DataFrame transformado.

FUNCION _load_df_to_bq

Esta funcion privada carga un DataFrame de pandas a una tabla en BigQuery. Recibe el DataFrame, el identificador del dataset y el nombre de la tabla.

El proceso es: primero crea un cliente de BigQuery y construye el nombre completo de la tabla usando el PROJECT_ID, dataset_id y table_name.

Segundo, define el esquema de la tabla. Es importante notar que en la capa bronze, todas las columnas excepto fecha_lectura son de tipo STRING para preservar los datos originales. El esquema incluye: cod_mpio como STRING, Municipio como STRING, Total como STRING, las cuatro columnas de IPM como STRING, y para cada indicador del I1 al I15, cuatro columnas todas como STRING. La unica columna que no es STRING es fecha_lectura que es TIMESTAMP.

Tercero, crea una configuracion de trabajo de carga que especifica que se debe usar WRITE_TRUNCATE, lo que significa que si la tabla ya existe, se reemplazara completamente con los nuevos datos. Tambien especifica el esquema definido.

Cuarto, ejecuta el trabajo de carga usando load_table_from_dataframe, pasando el DataFrame, el nombre completo de la tabla y la configuracion. Espera a que el trabajo termine usando job.result.

Quinto, imprime un mensaje indicando cuantas filas se cargaron exitosamente.

FUNCION load_dataframe_to_bq

Esta es una funcion publica que simplemente llama a _load_df_to_bq. Proporciona una interfaz publica para cargar DataFrames a BigQuery.

FUNCION cleanup_temp_paths

Esta funcion elimina archivos temporales del sistema de archivos. Recibe una lista iterable de rutas de archivos opcionales.

El proceso es: itera sobre cada ruta en la lista. Si la ruta es None o vacia, la ignora. Para cada ruta valida, intenta eliminar el archivo usando os.unlink. Si DEBUG esta activado, imprime un mensaje indicando que el archivo fue eliminado. Si hay alguna excepcion al intentar eliminar un archivo, imprime una advertencia pero no detiene el proceso.

FUNCION process_and_load_from_gcs

Esta funcion es una funcion de alto nivel que combina varios pasos del proceso ETL. Recibe la URI del archivo en GCS, el identificador del dataset, el nombre de la tabla y opcionalmente el indice de la hoja del Excel.

El proceso es: primero descarga el archivo Excel desde GCS a un archivo temporal local. Luego, dentro de un bloque try-finally para asegurar la limpieza, transforma el Excel usando transform_excel y carga el DataFrame resultante a BigQuery usando _load_df_to_bq. Finalmente, en el bloque finally, elimina el archivo temporal descargado usando cleanup_temp_paths, garantizando que los archivos temporales se eliminen incluso si ocurre un error durante el procesamiento.










ARCHIVO: load_rawdata_ipmv2_dag.py

Este archivo define el DAG Directed Acyclic Graph de Apache Airflow que orquesta todo el proceso de transformacion. Un DAG es una coleccion de tareas con dependencias que definen el orden de ejecucion.

CONFIGURACION Y CONSTANTES

El archivo comienza importando las librerias necesarias de Airflow y Python estandar. Luego importa las funciones del modulo load_rawdata_ipmv2 que se describieron anteriormente.

Define varias constantes de configuracion:
- GCS_BUCKET_NAME: Nombre del bucket en Google Cloud Storage, actualmente "datalake_gdv"
- GCS_FOLDER_PATH: Ruta de la carpeta dentro del bucket donde se encuentran los archivos Excel, actualmente "data_staging/dpt_planeacion_municipal/ipm"
- DATASET_ID_BRONZE: Identificador del dataset de BigQuery para la capa bronze, "bronze_dpt_planeacion_municipal_dev"
- DATASET_ID_SILVER: Identificador del dataset para la capa silver, "silver_dpt_planeacion_municipal_dev"
- DATASET_ID_GOLD: Identificador del dataset para la capa gold, "gold_dpt_planeacion_municipal_dev"
- TABLE_NAME_BRONZE: Nombre de la tabla en bronze, "bronze_dpt_planeacion_municipal_dev_ipm"
- TABLE_NAME_SILVER: Nombre de la tabla en silver, "silver_dpt_planeacion_municipal_dev_ipm"
- TABLE_NAME_GOLD: Nombre de la tabla en gold, "gold_dpt_planeacion_municipal_dev_ipm"
- SHEET_INDEX: Indice de la hoja del Excel a procesar, actualmente 0 primera hoja
- DBT_PROJECT_DIR: Ruta al directorio del proyecto dbt, "/opt/airflow/dags/gdv_general_dbt_dag/dbt"

FUNCIONES DE TAREAS PARA BRONZE

_ensure_dataset_bronze_task: Esta funcion llama a ensure_dataset con el DATASET_ID_BRONZE para asegurar que el dataset de bronze existe en BigQuery.

_download_excel_task: Esta funcion implementa la logica para descargar el archivo Excel. Primero intenta obtener la configuracion del bucket y la carpeta desde Variables de Airflow, que permiten configurar estos valores sin modificar el codigo. Si las variables no existen, usa los valores por defecto hardcodeados. Luego busca el ultimo archivo Excel en la carpeta usando get_latest_excel_from_gcs_folder, imprime informacion sobre el proceso, y finalmente descarga el archivo usando download_excel_from_gcs, retornando la ruta local del archivo descargado.

_transform_task: Esta funcion recibe el contexto de la tarea ti para acceder a XCom, que es el mecanismo de Airflow para compartir datos entre tareas. Obtiene la ruta del Excel descargado desde XCom usando el task_id "bronze.download_excel". Si no recibe la ruta, lanza una excepcion. Luego transforma el Excel usando transform_excel. Crea un archivo temporal con extension .pkl, guarda el DataFrame transformado en ese archivo usando to_pickle, y retorna la ruta del archivo pickle para que la siguiente tarea pueda accederlo.

_load_task: Similar a la anterior, obtiene la ruta del archivo pickle desde XCom usando el task_id "bronze.transform_dataframe". Lee el DataFrame desde el pickle, y lo carga a BigQuery usando load_dataframe_to_bq con el dataset y tabla de bronze.

_cleanup_temp_files_task: Obtiene ambas rutas el Excel y el pickle desde XCom y las elimina usando cleanup_temp_paths.

DEFINICION DEL DAG

El DAG se define con las siguientes caracteristicas:
- dag_id: "scr_planeacion_transf_ipm_manual" identificador unico del DAG
- start_date: Fecha de inicio, 1 de enero de 2024
- schedule_interval: None, lo que significa que el DAG no se ejecuta automaticamente, solo manualmente
- catchup: False, no ejecuta ejecuciones pasadas
- tags: Etiquetas para categorizar el DAG: planeacion, transformacion, ipm, manual
- description: Descripcion detallada del proposito del DAG

GRUPO DE TAREAS BRONZE

El DAG organiza las tareas en grupos logicos usando TaskGroup. El grupo bronze contiene cinco tareas:

t1_ensure_dataset: PythonOperator que ejecuta _ensure_dataset_bronze_task para asegurar que el dataset existe.

t2_download_excel: PythonOperator que ejecuta _download_excel_task para descargar el archivo Excel desde GCS.

t3_transform_dataframe: PythonOperator que ejecuta _transform_task para transformar el Excel en un DataFrame.

t4_load_to_bq: PythonOperator que ejecuta _load_task para cargar los datos a BigQuery.

t5_cleanup_temp_files: PythonOperator que ejecuta _cleanup_temp_files_task para limpiar archivos temporales. Esta tarea tiene trigger_rule ALL_DONE, lo que significa que se ejecutara incluso si alguna tarea anterior fallo, asegurando la limpieza de archivos temporales.

Las dependencias se definen como: t1_ensure_dataset ejecuta primero, luego t2_download_excel, luego t3_transform_dataframe, luego t4_load_to_bq, y finalmente t5_cleanup_temp_files.

GRUPO DE TAREAS SILVER

El grupo silver contiene las tareas que ejecutan los modelos dbt para transformar los datos de bronze a silver. Todas las tareas son BashOperator que ejecutan comandos dbt.

s1_ensure_dataset: Asegura que el dataset de silver existe.

s2_dbt_run_stg: Ejecuta el modelo dbt rawdata_ipmv2_stg. Este modelo lee los datos de bronze y normaliza los nombres de columnas a snake_case, convirtiendo nombres como "Municipio" a "municipio", "Total" a "total", etc. Se materializa como una vista.

s3_dbt_run_normalize_text: Ejecuta el modelo rawdata_ipmv2_normalize_text. Este modelo normaliza el texto de los identificadores cod_mpio y municipio, eliminando acentos y convirtiendo a mayusculas. Esto es importante para estandarizar los nombres y facilitar las comparaciones y joins posteriores.

s4_dbt_run_transform_types: Ejecuta el modelo rawdata_ipmv2_transform_types. Este modelo intenta convertir los tipos de datos de STRING a los tipos apropiados INT64 para valores absolutos y FLOAT64 para porcentajes. Usa SAFE_CAST para manejar valores invalidos, que se convierten en NULL.

s5_dbt_run_clean_numbers: Ejecuta el modelo rawdata_ipmv2_clean_numbers. Este modelo limpia las columnas numericas eliminando letras, espacios y caracteres especiales, dejando solo numeros, comas, puntos y signos menos. Luego convierte estos valores limpios a INT64. Este es un paso critico porque los datos originales pueden venir con caracteres no numericos mezclados.

s6_dbt_run_detect_negatives: Ejecuta el modelo rawdata_ipmv2_detect_negatives. Este modelo detecta si hay valores negativos en cualquier columna numerica y crea flags de validacion. Tambien detecta si el campo total es NULL o cero despues de la limpieza. Estos flags se usaran en el siguiente paso para aplicar reglas de validacion.

s7_dbt_run_apply_validations: Ejecuta el modelo rawdata_ipmv2_apply_validations. Este modelo aplica las validaciones finales: si hay algun valor negativo en un registro, pone TODAS las columnas numericas de ese registro en cero. Si el total es cero o NULL, tambien pone todas las columnas numericas en cero. Los porcentajes se convierten de STRING a FLOAT64 usando SAFE_CAST.

s8_dbt_run_clean: Ejecuta el modelo rawdata_ipmv2_clean. Este es el modelo final de silver que convierte la columna fecha_lectura a tipo DATE eliminando la parte de hora, y materializa los datos como una tabla con el alias especificado.

s9_dbt_test: Ejecuta las pruebas dbt sobre el modelo rawdata_ipmv2_clean para validar la calidad de los datos.

Las dependencias son: s1_ensure_dataset ejecuta primero, luego s2_dbt_run_stg, luego s3_dbt_run_normalize_text. Despues de normalize_text, tanto s4_dbt_run_transform_types como s5_dbt_run_clean_numbers pueden ejecutarse en paralelo porque ambos dependen solo de normalize_text. Luego s5_dbt_run_clean_numbers debe ejecutarse antes de s6_dbt_run_detect_negatives, que debe ejecutarse antes de s7_dbt_run_apply_validations, que debe ejecutarse antes de s8_dbt_run_clean, que finalmente debe ejecutarse antes de s9_dbt_test.

GRUPO DE TAREAS GOLD

El grupo gold contiene las tareas finales que crean la capa gold lista para consumo.

g1_ensure_dataset: Asegura que el dataset de gold existe.

g2_dbt_run_gold: Ejecuta el modelo rawdata_ipmv2_gold. Este modelo lee los datos de la tabla final de silver y los transforma para el formato final de consumo: convierte los nombres de columnas a mayusculas, elimina los sufijos _abs de los nombres, elimina las columnas de porcentajes, y elimina la columna fecha_lectura. El resultado es una tabla simplificada con solo los valores absolutos de los indicadores.

g3_dbt_test: Ejecuta las pruebas dbt sobre el modelo gold para validar la calidad de los datos finales.

Las dependencias son: g1_ensure_dataset ejecuta primero, luego g2_dbt_run_gold, y finalmente g3_dbt_test.

DEPENDENCIAS ENTRE GRUPOS

Finalmente, se define que el grupo bronze debe ejecutarse primero, luego el grupo silver, y finalmente el grupo gold. Esto refleja la arquitectura de capas donde cada capa depende de la anterior.

FLUJO COMPLETO DEL PIPELINE

El flujo completo del pipeline es el siguiente:

1. BRONZE: Se descarga el archivo Excel mas reciente desde GCS, se transforma basicamente renombrando columnas y convirtiendo todo a STRING para preservar datos originales, y se carga en BigQuery.

2. SILVER: Se ejecutan una serie de modelos dbt que progresivamente limpian y validan los datos: primero se normalizan nombres, luego se normaliza texto, luego se intentan convertir tipos, luego se limpian numeros, se detectan valores negativos, se aplican validaciones, y finalmente se materializa como tabla limpia.

3. GOLD: Se crea la capa final de consumo con el formato simplificado para los usuarios finales.

Cada paso esta disenado para ser idempotente y reproducible, y el uso de dbt permite versionar y documentar las transformaciones de manera clara.

