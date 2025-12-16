# Documentación: modules/gcp_utils.py

## 1. Información General

El archivo "modules/gcp_utils.py" es un módulo Python que proporciona funciones helper para crear clientes de los servicios de Google Cloud Platform, específicamente Google Cloud Storage y BigQuery. Este módulo centraliza la creación de clientes y simplifica el acceso a estos servicios desde otros módulos y DAGs. Su propósito principal es proporcionar una interfaz uniforme y reutilizable para interactuar con GCS y BigQuery, manejando automáticamente la autenticación mediante Application Default Credentials.

## 2. Propósito y Funcionalidad

El archivo "gcp_utils.py" cumple varias funciones importantes en el sistema. En primer lugar, centraliza la creación de clientes de Google Cloud Storage y BigQuery, evitando que cada módulo tenga que crear sus propios clientes y manejar la configuración de autenticación individualmente.

En segundo lugar, simplifica el acceso a estos servicios proporcionando funciones simples que retornan clientes listos para usar. Esto reduce la duplicación de código y asegura que todos los módulos usen la misma configuración de proyecto.

En tercer lugar, maneja automáticamente la autenticación mediante Application Default Credentials, que funciona tanto en Cloud Composer usando la cuenta de servicio del entorno, como en desarrollo local usando las credenciales configuradas en "GOOGLE_APPLICATION_CREDENTIALS".

Finalmente, usa el "PROJECT_ID" de "config.py" para asegurar que todos los clientes se creen con el proyecto correcto según el ambiente activo, manteniendo la consistencia en todo el sistema.

## 3. Estructura del Archivo

El archivo está organizado de manera simple. Primero importa las librerías necesarias de Google Cloud y el módulo de configuración. Luego define dos funciones: "get_gcs_client" que crea y retorna un cliente de Google Cloud Storage, y "get_bq_client" que crea y retorna un cliente de BigQuery. Ambas funciones son muy similares en estructura, solo difieren en el tipo de cliente que crean.

## 4. Imports y Dependencias

El archivo importa "storage" y "bigquery" del paquete "google.cloud". Estos son los módulos oficiales de Google Cloud que proporcionan las clases "Client" necesarias para interactuar con Google Cloud Storage y BigQuery respectivamente. Estas librerías deben estar instaladas en el entorno, lo cual se especifica en "requirements.txt".

También importa "PROJECT_ID" desde ".config", que es una importación relativa que se refiere al módulo "config.py" en el mismo directorio "modules". El punto antes de "config" indica que es una importación relativa desde el mismo paquete. "PROJECT_ID" contiene el ID del proyecto de Google Cloud Platform que se usa para crear los clientes.

## 5. Función get_gcs_client

La función "get_gcs_client" crea y retorna un cliente de Google Cloud Storage que puede ser usado para leer y escribir archivos en buckets de GCS. Esta función no toma parámetros y retorna un objeto de tipo "storage.Client".

La función crea el cliente llamando a "storage.Client" y pasando el parámetro "project" con el valor de "PROJECT_ID". Esto asegura que el cliente se cree con el proyecto correcto según el ambiente activo, ya sea desarrollo, producción, o local.

La autenticación se maneja automáticamente por el entorno. En Cloud Composer, el cliente usa automáticamente la cuenta de servicio del entorno de Airflow, que tiene los permisos necesarios para acceder a los buckets y archivos. En desarrollo local, el cliente usa las credenciales especificadas en la variable de entorno "GOOGLE_APPLICATION_CREDENTIALS", que apunta a un archivo JSON de cuenta de servicio.

El cliente retornado puede ser usado para realizar operaciones como listar buckets, leer archivos, escribir archivos, copiar archivos, y eliminar archivos. Todas estas operaciones se realizan en el contexto del proyecto especificado en "PROJECT_ID".

## 6. Función get_bq_client

La función "get_bq_client" crea y retorna un cliente de BigQuery que puede ser usado para ejecutar queries, cargar datos, crear datasets y tablas, y realizar otras operaciones en BigQuery. Esta función no toma parámetros y retorna un objeto de tipo "bigquery.Client".

La función crea el cliente llamando a "bigquery.Client" y pasando el parámetro "project" con el valor de "PROJECT_ID". Al igual que "get_gcs_client", esto asegura que el cliente se cree con el proyecto correcto según el ambiente activo.

La autenticación también se maneja automáticamente por el entorno. En Cloud Composer, el cliente usa la cuenta de servicio del entorno, y en desarrollo local usa las credenciales de "GOOGLE_APPLICATION_CREDENTIALS".

El cliente retornado puede ser usado para realizar operaciones como ejecutar queries SQL, cargar datos desde archivos o DataFrames de pandas, crear y eliminar datasets y tablas, obtener información sobre datasets y tablas existentes, y realizar otras operaciones de administración de BigQuery. Todas estas operaciones se realizan en el contexto del proyecto especificado en "PROJECT_ID".

## 7. Cómo se Usa en el Código

El módulo "gcp_utils.py" se usa en los módulos de ingestión y carga para interactuar con Google Cloud Storage y BigQuery. La forma típica de usar estas funciones es importándolas y llamándolas cuando se necesita un cliente.

Para usar el cliente de Google Cloud Storage, se importa con "from modules.gcp_utils import get_gcs_client", y luego se llama a la función cuando se necesita el cliente. Por ejemplo, "gcs_client = get_gcs_client()" crea un cliente que puede ser usado para operaciones como "gcs_client.bucket('bucket_name').blob('file_path').download_as_string()" para leer un archivo, o "gcs_client.bucket('bucket_name').blob('file_path').upload_from_string(data)" para escribir un archivo.

Para usar el cliente de BigQuery, se importa con "from modules.gcp_utils import get_bq_client", y luego se llama a la función cuando se necesita el cliente. Por ejemplo, "bq_client = get_bq_client()" crea un cliente que puede ser usado para operaciones como "bq_client.query(query_string).result()" para ejecutar una query, o "bq_client.load_table_from_dataframe(dataframe, table_ref)" para cargar datos desde un DataFrame de pandas.

Estas funciones se llaman dentro de las funciones de los módulos específicos de cada fuente, como en "evaplan_load.py", "idc_load.py", "ipm_load.py", etcétera. Cada vez que se necesita interactuar con GCS o BigQuery, se llama a la función correspondiente para obtener un cliente fresco.

## 8. Autenticación Automática

Una de las características más importantes de estas funciones es que manejan la autenticación automáticamente. No es necesario pasar credenciales explícitamente a las funciones, ya que los clientes de Google Cloud detectan automáticamente las credenciales disponibles en el entorno.

En Cloud Composer, los clientes usan automáticamente la cuenta de servicio asociada al entorno de Airflow. Esta cuenta de servicio tiene los permisos necesarios para acceder a los recursos de GCP según las políticas de IAM configuradas. No se requiere configuración adicional.

En desarrollo local, los clientes buscan automáticamente las credenciales en la variable de entorno "GOOGLE_APPLICATION_CREDENTIALS", que debe apuntar a un archivo JSON de cuenta de servicio. Si esta variable está configurada, los clientes la usan automáticamente. Si no está configurada, los clientes intentan usar las credenciales por defecto de la aplicación, que pueden estar en ubicaciones estándar del sistema.

Este enfoque de autenticación automática simplifica el código y hace que sea más fácil trabajar en diferentes entornos sin necesidad de cambiar código.

## 9. Uso del PROJECT_ID

Ambas funciones usan "PROJECT_ID" de "config.py" para especificar el proyecto de Google Cloud Platform. Esto asegura que todas las operaciones se realicen en el proyecto correcto según el ambiente activo.

El "PROJECT_ID" se determina automáticamente basándose en el ambiente seleccionado mediante la variable de entorno "ENVIRONMENT" o el valor por defecto en "config.py". Si el ambiente es "dev", se usa el "project_id" de la sección "dev" en "config.yaml". Si el ambiente es "prod", se usa el "project_id" de la sección "prod". Si el ambiente es "local", se usa el "project_id" de la sección "local".

Esto significa que los clientes creados por estas funciones siempre operan en el proyecto correcto sin necesidad de especificarlo manualmente en cada llamada. Esto reduce errores y asegura consistencia en todo el sistema.

## 10. Ventajas de Centralizar la Creación de Clientes

Centralizar la creación de clientes en "gcp_utils.py" tiene varias ventajas. En primer lugar, reduce la duplicación de código. En lugar de que cada módulo tenga su propio código para crear clientes, todos usan las mismas funciones helper.

En segundo lugar, facilita el mantenimiento. Si en el futuro se necesita cambiar cómo se crean los clientes, por ejemplo agregando configuraciones adicionales o manejando errores de manera diferente, solo se necesita modificar estas dos funciones en lugar de buscar y modificar código en múltiples archivos.

En tercer lugar, asegura consistencia. Todos los módulos usan clientes creados de la misma manera, con la misma configuración de proyecto y autenticación. Esto reduce la posibilidad de errores de configuración.

Finalmente, simplifica el código de los módulos que usan estos clientes. En lugar de tener que importar las librerías de Google Cloud, obtener el "PROJECT_ID", y crear el cliente cada vez, simplemente llaman a una función helper.

## 11. Ejemplos de Uso en Módulos

En los módulos de carga, estas funciones se usan frecuentemente. Por ejemplo, en un módulo de carga de datos a BigQuery, se podría usar "get_bq_client" para obtener un cliente y luego usarlo para cargar datos desde un DataFrame de pandas a una tabla de BigQuery. El código sería algo como "bq_client = get_bq_client()" seguido de "bq_client.load_table_from_dataframe(df, table_ref)".

En los módulos de ingestión, se podría usar "get_gcs_client" para obtener un cliente y luego usarlo para subir archivos descargados a un bucket de GCS. El código sería algo como "gcs_client = get_gcs_client()" seguido de "bucket = gcs_client.bucket(bucket_name)" y luego "blob = bucket.blob(file_path)" y "blob.upload_from_filename(local_file_path)".

Estos ejemplos muestran cómo las funciones simplifican el código y hacen que sea más legible y mantenible.

## 12. Manejo de Errores

Las funciones en "gcp_utils.py" no incluyen manejo de errores explícito, ya que delegan el manejo de errores a los clientes de Google Cloud y al código que las llama. Si hay un problema con la autenticación, los clientes lanzarán excepciones apropiadas que deben ser manejadas por el código que llama a estas funciones.

Si "PROJECT_ID" no está definido o es inválido, los clientes pueden fallar al crearse o al realizar operaciones. Sin embargo, como "PROJECT_ID" se valida en "config.py" antes de que estas funciones se usen, este caso generalmente no ocurre en la práctica.

Si las credenciales no están disponibles o son inválidas, los clientes lanzarán excepciones de autenticación cuando se intenten usar para realizar operaciones. El código que llama a estas funciones debe manejar estas excepciones apropiadamente.

## 13. Notas Importantes

Es importante entender que estas funciones crean nuevos clientes cada vez que se llaman. Los clientes de Google Cloud son objetos ligeros y crear nuevos clientes no tiene un costo significativo, pero si se necesita reutilizar un cliente múltiples veces, se puede almacenar en una variable en lugar de llamar a la función repetidamente.

Los clientes retornados por estas funciones son thread-safe y pueden ser usados de manera segura en contextos multi-threaded si es necesario. Sin embargo, en el contexto de Airflow, generalmente cada tarea se ejecuta en su propio proceso, por lo que esto no es un problema común.

Las funciones asumen que la autenticación está configurada correctamente en el entorno. Si se ejecuta el código en un entorno donde las credenciales no están disponibles, las operaciones fallarán con excepciones de autenticación. Es responsabilidad del desarrollador asegurar que las credenciales estén configuradas antes de usar estas funciones.

El "PROJECT_ID" usado por estas funciones viene de "config.py", que a su vez lo obtiene de "config.yaml" según el ambiente activo. Esto significa que cambiar el ambiente automáticamente cambia el proyecto usado por estos clientes, lo cual es el comportamiento deseado.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/gcp_utils.py"  
Versión del archivo: 3.0

