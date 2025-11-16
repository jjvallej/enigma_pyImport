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


