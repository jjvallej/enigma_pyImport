DOCUMENTACION DETALLADA DEL MODULO ipm_load.py

Este documento describe en detalle el funcionamiento del modulo ipm_load.py, que proporciona funcionalidades para extraer archivos Excel desde Google Cloud Storage, transformarlos minimamente y cargarlos en la capa bronze de BigQuery. Este modulo encapsula toda la logica relacionada con la extraccion y carga inicial de datos en la capa bronze.

PROPOSITO DEL MODULO

El modulo ipm_load.py esta disenado para facilitar la extraccion de archivos Excel desde Google Cloud Storage y su carga en BigQuery en la capa bronze. Este proceso es el segundo paso en el pipeline de datos IPM, despues de que el archivo ha sido movido desde Google Drive a GCS por el modulo ipm_extract.py.

La filosofia de la capa bronze es preservar los datos exactamente como vienen de la fuente, sin hacer transformaciones complejas o limpiezas profundas. Todas las transformaciones y validaciones se realizan posteriormente en las capas silver y gold usando dbt.

CONFIGURACION INICIAL

El archivo comienza con la importacion de las librerias necesarias. Utiliza google.cloud.bigquery y google.cloud.storage para interactuar con los servicios de Google Cloud, pandas para manipulacion de datos, y librerias estandar de Python para manejo de archivos temporales, fechas y expresiones regulares.

Las constantes principales son:
- PROJECT_ID: Identificador del proyecto de Google Cloud, actualmente "datagov-473122"
- SA_PATH: Ruta al archivo de credenciales de servicio, ubicado en "/opt/airflow/include/sa.json"
- DEBUG: Variable booleana que controla si se muestran mensajes de depuracion detallados
- GCS_BUCKET_NAME: Nombre del bucket de GCS por defecto, "datalake_gdv_dev"
- GCS_FOLDER_PATH: Ruta de la carpeta por defecto en GCS, "data_staging/dpt_planeacion_municipal/ipm"

FUNCIONES DE CLIENTES

El modulo define dos funciones privadas para crear clientes de Google Cloud:

_bq_client: Crea y retorna un cliente de BigQuery. Esta funcion establece la variable de entorno GOOGLE_APPLICATION_CREDENTIALS con la ruta del archivo de credenciales y luego crea un cliente de BigQuery asociado al proyecto configurado. Este cliente se utiliza para todas las operaciones relacionadas con BigQuery, como crear datasets y cargar datos.

_gcs_client: Crea y retorna un cliente de Google Cloud Storage. Similar a la funcion anterior, establece las credenciales y crea un cliente de Storage para interactuar con buckets y archivos en GCS. Este cliente se utiliza para descargar archivos desde GCS.

FUNCION ensure_dataset

Esta funcion verifica si existe un dataset en BigQuery y lo crea si no existe. Recibe como parametros el identificador del dataset y opcionalmente la ubicacion geografica, que por defecto es "us-central1".

El proceso es el siguiente: primero construye el nombre completo del dataset usando el PROJECT_ID y el dataset_id proporcionado. Luego intenta obtener el dataset usando el cliente de BigQuery. Si el dataset existe, imprime un mensaje de confirmacion si DEBUG esta activado. Si no existe, crea un nuevo dataset con la ubicacion especificada, le asigna la descripcion "Bronze layer para IPM v2" y lo crea en BigQuery. Finalmente imprime un mensaje indicando que el dataset fue creado exitosamente.

Esta funcion es utilizada para asegurar que el dataset bronze_dpt_planeacion_municipal_dev existe antes de intentar cargar datos.

FUNCION get_latest_excel_from_gcs_folder

Esta funcion busca y retorna la URI del archivo Excel mas reciente en una carpeta especifica de Google Cloud Storage. Es una funcion critica porque permite que el pipeline procese automaticamente el archivo mas actualizado sin necesidad de especificar manualmente el nombre del archivo.

El proceso funciona asi: primero normaliza la ruta de la carpeta asegurandose de que termine con una barra diagonal. Luego obtiene el bucket de GCS usando el nombre proporcionado. Si hay un error al acceder al bucket, lanza una excepcion con un mensaje descriptivo.

Despues lista todos los blobs (objetos) en la carpeta especificada usando el prefijo de la ruta. Filtra los archivos para quedarse solo con aquellos que terminan en .xlsx y que no son directorios (evitando carpetas virtuales). Si no encuentra ningun archivo Excel, lanza una excepcion indicando que no se encontraron archivos.

Si encuentra archivos, los ordena por fecha de creacion de manera descendente, es decir, el mas reciente primero. Toma el primer archivo de la lista ordenada y construye su URI completa en formato gs://bucket/nombre_archivo.

Si DEBUG esta activado, imprime informacion sobre cuantos archivos se encontraron y muestra los primeros cinco con sus fechas de creacion, ademas de indicar cual archivo se selecciono. Finalmente retorna la URI completa del archivo mas reciente.

FUNCION download_excel_from_gcs

Esta funcion descarga un archivo Excel desde Google Cloud Storage a un archivo temporal en el sistema de archivos local. Recibe como parametro la URI completa del archivo en formato gs://bucket/nombre_archivo.

El proceso es: primero extrae el nombre del bucket y el nombre del blob desde la URI, dividiendo la cadena por las barras diagonales. El bucket es el tercer elemento (indice 2) despues de dividir por "/", y el blob name es el resto de la ruta. Luego obtiene el blob especifico desde el bucket. Crea un archivo temporal usando tempfile.mkstemp con extension .xlsx, cierra el descriptor de archivo inmediatamente, y descarga el contenido del blob al archivo temporal usando download_to_filename. Si DEBUG esta activado, imprime la ruta donde se descargo el archivo. Finalmente retorna la ruta del archivo temporal descargado.

Esta funcion garantiza que el archivo se descargue de manera eficiente y que el descriptor de archivo se cierre correctamente para evitar problemas de recursos.

FUNCION transform_excel

Esta es una de las funciones mas importantes del modulo. Aplica las transformaciones minimas necesarias al archivo Excel descargado y retorna un DataFrame de pandas listo para ser cargado en BigQuery en la capa bronze.

El proceso de transformacion tiene varios pasos:

Primero lee el archivo Excel usando pandas.read_excel, especificando la hoja por indice (por defecto 0, la primera hoja) y usando la primera fila como encabezados (header=0).

Segundo, elimina la primera fila de datos porque generalmente contiene totales o una fila guia que no es parte de los datos reales. Esto se hace usando iloc para seleccionar desde la fila 1 en adelante y luego reseteando el indice.

Tercero, si DEBUG esta activado, imprime los nombres de las columnas originales para inspeccion.

Cuarto, define las columnas esperadas en el orden correcto. El formato esperado es: cod_mpio, Municipio, Total, luego cuatro columnas para IPM (IPM_Pobre_Abs, IPM_No_Pobre_Abs, IPM_Pobre_Porc, IPM_No_Pobre_Porc), y luego para cada indicador del I1 al I15, cuatro columnas: Con_Privacion_Abs, Sin_Privacion_Abs, Con_Privacion_Porc, Sin_Privacion_Porc.

Quinto, valida que el archivo tenga al menos el numero de columnas esperadas. Si tiene menos columnas, lanza una excepcion indicando cuantas columnas se esperaban y cuantas tiene el archivo. Si tiene mas columnas, emite una advertencia si DEBUG esta activado y toma solo las primeras columnas necesarias.

Sexto, renombra las columnas del DataFrame con los nombres esperados.

Septimo, y esto es muy importante, convierte todas las columnas a tipo STRING. Esta es una decision de diseno critica: en la capa bronze no se deben hacer conversiones de tipos porque se quiere preservar los datos originales exactamente como vienen, incluso si tienen letras, espacios, caracteres especiales o valores invalidos. Las transformaciones y limpiezas se haran posteriormente en la capa silver usando dbt. Especificamente, convierte cod_mpio y Municipio a string, y todas las columnas numericas tanto las que deberian ser enteros como las que deberian ser flotantes a string.

Octavo, agrega una columna llamada fecha_lectura con el timestamp actual en UTC usando datetime.now(timezone.utc). Esta columna permite rastrear cuando se procesaron los datos.

Finalmente retorna el DataFrame transformado.

FUNCION _load_df_to_bq

Esta funcion privada carga un DataFrame de pandas a una tabla en BigQuery. Recibe el DataFrame, el identificador del dataset y el nombre de la tabla.

El proceso es: primero crea un cliente de BigQuery y construye el nombre completo de la tabla usando el PROJECT_ID, dataset_id y table_name.

Segundo, define el esquema de la tabla. Es importante notar que en la capa bronze, todas las columnas excepto fecha_lectura son de tipo STRING para preservar los datos originales. El esquema incluye: cod_mpio como STRING, Municipio como STRING, Total como STRING, las cuatro columnas de IPM como STRING, y para cada indicador del I1 al I15, cuatro columnas todas como STRING. La unica columna que no es STRING es fecha_lectura que es TIMESTAMP.

Tercero, crea una configuracion de trabajo de carga que especifica que se debe usar WRITE_TRUNCATE, lo que significa que si la tabla ya existe, se reemplazara completamente con los nuevos datos. Tambien especifica el esquema definido.

Cuarto, ejecuta el trabajo de carga usando load_table_from_dataframe, pasando el DataFrame, el nombre completo de la tabla y la configuracion. Espera a que el trabajo termine usando job.result().

Quinto, imprime un mensaje indicando cuantas filas se cargaron exitosamente.

FUNCION load_dataframe_to_bq

Esta es una funcion publica que simplemente llama a _load_df_to_bq. Proporciona una interfaz publica para cargar DataFrames a BigQuery, ocultando la implementacion interna de la funcion privada.

FUNCION cleanup_temp_paths

Esta funcion elimina archivos temporales del sistema de archivos. Recibe una lista iterable de rutas de archivos opcionales.

El proceso es: itera sobre cada ruta en la lista. Si la ruta es None o vacia, la ignora. Para cada ruta valida, intenta eliminar el archivo usando os.unlink. Si DEBUG esta activado, imprime un mensaje indicando que el archivo fue eliminado. Si hay alguna excepcion al intentar eliminar un archivo, imprime una advertencia pero no detiene el proceso.

Esta funcion es critica para mantener el sistema limpio y evitar la acumulacion de archivos temporales que podrian consumir espacio en disco.

CARACTERISTICAS IMPORTANTES DEL MODULO

El modulo tiene varias caracteristicas importantes que lo hacen robusto y facil de usar:

Preservacion de datos originales: La decision de convertir todas las columnas a STRING en la capa bronze garantiza que los datos se preserven exactamente como vienen de la fuente. Esto permite auditar y depurar problemas posteriormente.

Manejo automatico del archivo mas reciente: La funcion get_latest_excel_from_gcs_folder permite que el pipeline procese automaticamente el archivo mas actualizado sin intervencion manual, reduciendo errores humanos.

Limpieza automatica: La funcion cleanup_temp_paths garantiza que los archivos temporales se eliminen siempre, incluso si ocurre un error, evitando acumulacion de archivos temporales en el sistema.

Mensajes de depuracion: Cuando DEBUG esta activado, el modulo proporciona mensajes detallados sobre cada paso del proceso, lo que facilita la depuracion y el monitoreo.

Idempotencia: La configuracion WRITE_TRUNCATE garantiza que si el proceso se ejecuta multiples veces, la tabla se reemplazara con los datos mas recientes, proporcionando un comportamiento consistente.

CASOS DE USO

El modulo se puede usar en diferentes escenarios:

Caso 1: Extraccion y carga directa. El modulo puede usarse directamente para extraer un archivo de GCS y cargarlo a BigQuery en bronze. Esto es lo que hace el DAG src_planeacion_load_ipm.

Caso 2: Integracion en DAGs de Airflow. El modulo esta disenado para ser usado en DAGs de Airflow, donde cada funcion puede ser una tarea diferente del DAG, permitiendo mejor control, logging y manejo de errores.

INTEGRACION CON EL PIPELINE

Este modulo es el segundo paso en el pipeline de datos IPM:

1. El modulo ipm_extract.py transfiere el archivo desde Google Drive a GCS.
2. Este modulo (ipm_load.py) extrae el archivo de GCS, aplica transformaciones minimas y lo carga en BigQuery en la capa bronze (bronze_dpt_planeacion_municipal_dev.ipm_raw_data).
3. El DAG src_planeacion_transf_ipm utiliza modelos dbt para transformar los datos desde bronze a silver (silver_dpt_planeacion_municipal_dev.ipm_transformed_data) y gold (gold_dpt_planeacion_municipal_dev.ipm_processed_data).

La separacion de responsabilidades es clara: ipm_extract.py se encarga de la transferencia desde Drive a GCS, ipm_load.py se encarga de la extraccion y carga a bronze, e ipm_transform.py (junto con los modelos dbt) se encarga del procesamiento y transformacion a silver y gold. Esta separacion permite que cada modulo se enfoque en su tarea especifica y facilita el mantenimiento y las pruebas.
