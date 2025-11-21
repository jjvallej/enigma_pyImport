DOCUMENTACION DETALLADA DEL MODULO ipm_extract.py

Este documento describe en detalle el funcionamiento del modulo ipm_extract.py, que proporciona funcionalidades para descargar archivos Excel desde Google Drive usando enlaces publicos y subirlos a Google Cloud Storage. El modulo maneja diversos formatos de URLs de Google Drive.

PROPOSITO DEL MODULO

El modulo ipm_extract.py esta disenado para facilitar la transferencia de archivos Excel desde Google Drive hacia Google Cloud Storage. Este proceso es tipicamente el primer paso en el pipeline de datos, donde los archivos fuente se almacenan en Drive y necesitan ser movidos a GCS para su posterior procesamiento.

El modulo utiliza exclusivamente enlaces publicos de Google Drive, lo que significa que no requiere autenticacion adicional. El archivo debe estar configurado como publico en Google Drive (configurado como "Cualquier persona con el enlace puede ver").

CONFIGURACION INICIAL

El archivo comienza con la importacion de las librerias necesarias. Utiliza google.cloud.storage para interactuar con Google Cloud Storage, requests para descargar archivos desde enlaces publicos, y librerias estandar de Python para manejo de archivos, URLs y expresiones regulares.

Las constantes principales son:
- PROJECT_ID: Identificador del proyecto de Google Cloud, actualmente "datagov-473122"
- SA_PATH: Ruta al archivo de credenciales de servicio, ubicado en "/opt/airflow/include/sa.json" (solo se usa para GCS, no para Drive)
- DEBUG: Variable booleana que controla si se muestran mensajes de depuracion detallados

FUNCIONES DE CLIENTES

El modulo define una funcion para crear clientes de Google Cloud:

_gcs_client: Crea y retorna un cliente de Google Cloud Storage. Esta funcion establece la variable de entorno GOOGLE_APPLICATION_CREDENTIALS con la ruta del archivo de credenciales y luego crea un cliente de Storage asociado al proyecto configurado. Este cliente se utiliza para todas las operaciones de subida y manipulacion de archivos en GCS.

FUNCION extract_file_id_from_url

Esta funcion es fundamental porque extrae el identificador unico del archivo File ID desde diferentes formatos de URLs de Google Drive. El File ID es necesario para todas las operaciones con la API de Drive.

El proceso funciona de la siguiente manera: primero verifica si la cadena proporcionada ya es solo un File ID, es decir, si tiene menos de 50 caracteres y no comienza con http. En ese caso, simplemente retorna el ID limpio sin espacios.

Si es una URL, intenta extraer el File ID usando expresiones regulares que buscan patrones comunes en las URLs de Google Drive. Los patrones que reconoce incluyen:
- URLs de archivos: /file/d/FILE_ID/view
- URLs de hojas de calculo: /spreadsheets/d/FILE_ID/edit
- URLs de documentos: /document/d/FILE_ID
- URLs con parametro id: ?id=FILE_ID
- URLs de carpetas: /folders/FILE_ID

La funcion itera sobre cada patron y si encuentra una coincidencia, retorna el File ID extraido. Si ningun patron coincide, lanza una excepcion indicando que no se pudo extraer el File ID de la URL proporcionada.

FUNCION get_public_download_url

Esta funcion convierte un File ID en una URL de descarga directa para archivos publicos. La URL generada utiliza el formato especial de Google Drive que permite descargar archivos publicos sin necesidad de autenticacion. El formato es: https://drive.google.com/uc?export=download&id=FILE_ID. Esta URL se puede usar directamente con requests o similar para descargar el archivo.

FUNCION download_file_from_public_link

Esta funcion descarga un archivo desde Google Drive usando un enlace publico. Es el metodo mas simple porque no requiere autenticacion, solo que el archivo este configurado como publico en Google Drive.

El proceso es el siguiente: primero extrae el File ID de la URL proporcionada usando extract_file_id_from_url. Si DEBUG esta activado, imprime el File ID extraido.

Segundo, obtiene la URL de descarga directa usando get_public_download_url.

Tercero, crea una sesion de requests y hace una peticion GET a la URL de descarga con stream=True para manejar archivos grandes de manera eficiente, y allow_redirects=True para seguir redirecciones.

Cuarto, maneja un caso especial: cuando Google Drive detecta que se esta intentando descargar un archivo grande, muestra primero una pagina de advertencia en lugar de descargar directamente. La funcion detecta esto verificando si el Content-Type de la respuesta es text/html. Si es asi, busca en el contenido HTML el enlace real de descarga usando una expresion regular que busca el patron href="/uc?export=download...". Si encuentra el enlace, lo extrae, corrige las entidades HTML como &amp; a &, y hace una nueva peticion a esa URL.

Quinto, verifica que la peticion fue exitosa usando raise_for_status.

Sexto, determina el nombre original del archivo. Si se proporciono un nombre en el parametro file_name, lo usa. Si no, intenta extraerlo del header Content-Disposition de la respuesta HTTP. Si el header contiene filename=, extrae el nombre usando una expresion regular. Limpia el nombre removiendo caracteres problematicos como saltos de linea. Si no puede obtener el nombre del header, usa un nombre por defecto basado en el File ID.

Septimo, crea un archivo temporal con la extension apropiada. Extrae la extension del nombre del archivo o usa .xlsx por defecto. Usa tempfile.mkstemp para crear el archivo temporal y cierra el descriptor inmediatamente.

Octavo, descarga el archivo escribiendolo en chunks de 8192 bytes. Esto permite manejar archivos grandes de manera eficiente sin cargar todo el archivo en memoria. Si DEBUG esta activado y se conoce el tamano total del archivo, muestra el progreso de la descarga cada 25 por ciento.

Noveno, si DEBUG esta activado, imprime mensajes de confirmacion con la ruta del archivo descargado y el nombre original.

Finalmente, retorna una tupla con la ruta del archivo temporal descargado y el nombre original del archivo.

FUNCION download_file_from_drive

Esta funcion es una funcion de alto nivel que descarga un archivo desde Google Drive usando un enlace publico.

El proceso es simple: llama directamente a download_file_from_public_link con los parametros proporcionados.

Esta funcion proporciona una interfaz unificada y simplificada para la descarga de archivos desde Google Drive.

FUNCION upload_file_to_gcs

Esta funcion sube un archivo local a Google Cloud Storage. Maneja automaticamente la creacion de carpetas si no existen, y puede sobrescribir archivos existentes si se solicita.

El proceso es el siguiente: primero crea un cliente de GCS usando _gcs_client.

Segundo, obtiene el bucket usando el nombre proporcionado. Si hay un error al acceder al bucket, lanza una excepcion con un mensaje descriptivo.

Tercero, normaliza la ruta de destino eliminando barras dobles y espacios, asegurandose de que el formato sea consistente. Esto se hace dividiendo la ruta por barras, eliminando espacios y partes vacias, y volviendo a unir.

Cuarto, si DEBUG esta activado, verifica si la carpeta destino ya existe listando objetos con ese prefijo. Si encuentra objetos, imprime que la carpeta ya existe. Si no encuentra objetos, imprime que la carpeta se creara automaticamente. Nota importante: en GCS no existen realmente las carpetas, son solo prefijos en los nombres de los objetos, pero esta verificacion ayuda a entender el estado.

Quinto, obtiene o crea el blob objeto en GCS usando el nombre de destino normalizado.

Sexto, verifica si el archivo ya existe usando blob.exists(). Si existe y overwrite es True, elimina el archivo existente antes de subir el nuevo. Si DEBUG esta activado, imprime mensajes informativos sobre esta operacion. Si overwrite es False, imprime una advertencia pero no sube el nuevo archivo.

Septimo, sube el archivo usando blob.upload_from_filename(). Esta operacion crea automaticamente la estructura de carpetas si no existe, ya que GCS maneja las carpetas como parte del nombre del objeto.

Octavo, construye la URI completa del archivo en formato gs://bucket/ruta.

Noveno, si DEBUG esta activado, imprime un mensaje de confirmacion con la URI completa.

Finalmente, retorna la URI completa del archivo en GCS.

FUNCION move_file_from_drive_to_gcs

Esta es la funcion principal y mas completa del modulo. Combina la descarga desde Google Drive (usando enlace publico) y la subida a GCS en una sola operacion, manejando automaticamente la limpieza de archivos temporales.

El proceso es el siguiente: primero descarga el archivo desde Google Drive usando download_file_from_drive, que internamente utiliza download_file_from_public_link. Esta funcion retorna una tupla con la ruta local del archivo descargado y el nombre original del archivo.

Segundo, dentro de un bloque try-finally para asegurar la limpieza, determina el nombre del archivo de destino. Si no se proporciono destination_file_name, usa el nombre original extraido de Drive.

Tercero, construye la ruta completa de destino en GCS. Normaliza los nombres de carpeta y archivo eliminando barras y espacios, y construye la ruta como folder_name/destination_file_name.

Cuarto, sube el archivo a GCS usando upload_file_to_gcs con la ruta construida.

Quinto, retorna la URI completa del archivo en GCS.

Finalmente, en el bloque finally, elimina el archivo temporal local usando os.unlink. Si hay algun error al eliminar el archivo, imprime una advertencia pero no detiene el proceso. Si DEBUG esta activado, imprime un mensaje confirmando que el archivo temporal fue eliminado.

Esta funcion garantiza que los archivos temporales siempre se eliminen, incluso si ocurre un error durante el proceso de subida.

CARACTERISTICAS IMPORTANTES DEL MODULO

El modulo tiene varias caracteristicas importantes que lo hacen robusto y facil de usar:

Manejo de diferentes formatos de URL: La funcion extract_file_id_from_url puede manejar una amplia variedad de formatos de URLs de Google Drive, lo que hace que el modulo sea flexible y no requiera que el usuario formatee la URL de una manera especifica.

Manejo de archivos grandes: La descarga desde enlaces publicos se hace en chunks, lo que permite manejar archivos grandes sin cargar todo el archivo en memoria.

Manejo de advertencias de Google Drive: Cuando se descarga un archivo grande desde un enlace publico, Google Drive muestra primero una pagina de advertencia. El modulo detecta esto y extrae automaticamente el enlace real de descarga.

Preservacion de nombres originales: El modulo intenta preservar el nombre original del archivo tanto al descargarlo como al subirlo a GCS, lo que facilita el rastreo y la identificacion de archivos.

Creacion automatica de carpetas: Al subir un archivo a GCS, si la carpeta no existe, se crea automaticamente. Esto simplifica el proceso y evita errores.

Manejo de archivos existentes: La funcion upload_file_to_gcs permite controlar si se deben sobrescribir archivos existentes o mantener los anteriores, proporcionando flexibilidad segun las necesidades del caso de uso.

Limpieza automatica: La funcion move_file_from_drive_to_gcs garantiza que los archivos temporales se eliminen siempre, incluso si ocurre un error, evitando acumulacion de archivos temporales en el sistema.

Mensajes de depuracion: Cuando DEBUG esta activado, el modulo proporciona mensajes detallados sobre cada paso del proceso, lo que facilita la depuracion y el monitoreo.

CASOS DE USO

El modulo se puede usar en los siguientes escenarios:

Caso 1: Archivo publico en Google Drive. Este es el unico metodo soportado. Se necesita la URL publica del archivo, y el modulo lo descarga y sube a GCS sin necesidad de configuracion adicional ni autenticacion.

Caso 2: Automatizacion mediante DAG. El modulo esta disenado para ser usado en DAGs de Airflow, donde se puede automatizar la transferencia de archivos desde Drive a GCS como parte de un pipeline mas grande.

REQUISITOS

Para que el modulo funcione correctamente, el archivo en Google Drive debe estar configurado como publico, es decir, debe tener la configuracion de compartir establecida como "Cualquier persona con el enlace puede ver". Si el archivo no es publico, el modulo no podra descargarlo.

INTEGRACION CON EL PIPELINE

Este modulo es tipicamente el primer paso en el pipeline de datos. Los archivos se almacenan en Google Drive por los usuarios, y este modulo los transfiere a GCS. Una vez en GCS, el modulo ipm_load.py puede procesarlos y cargarlos a BigQuery.

La separacion de responsabilidades es clara: ipm_extract.py se encarga de la transferencia desde Drive a GCS, mientras que ipm_load.py se encarga de la extraccion y carga a bronze. Esta separacion permite que cada modulo se enfoque en su tarea especifica y facilita el mantenimiento y las pruebas.
