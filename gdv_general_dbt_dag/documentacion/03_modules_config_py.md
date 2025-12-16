# Documentación: modules/config.py

## 1. Información General

El archivo "modules/config.py" es un módulo Python que centraliza y expone toda la configuración del sistema para uso en los DAGs y módulos. Este archivo carga la configuración desde "config.yaml" mediante "config_loader.py", selecciona el ambiente apropiado, y expone variables globales que pueden ser importadas y usadas en cualquier parte del código. Su propósito principal es proporcionar un punto único de acceso a la configuración del sistema, eliminando la necesidad de que cada módulo cargue y procese la configuración individualmente.

## 2. Propósito y Funcionalidad

El archivo "config.py" cumple varias funciones críticas en el sistema. En primer lugar, carga la configuración desde "config.yaml" usando el módulo "config_loader.py", que busca el archivo en múltiples ubicaciones y lo convierte en un objeto Python accesible mediante notación de puntos.

En segundo lugar, determina el ambiente activo mediante la variable de entorno "ENVIRONMENT" o un valor por defecto, y selecciona automáticamente la configuración correspondiente del archivo "config.yaml".

En tercer lugar, expone variables globales que contienen los valores de configuración más usados, como "PROJECT_ID", "LOCATION", "DEFAULT_BUCKET_NAME", y los nombres de datasets de BigQuery. Estas variables pueden ser importadas directamente en los DAGs y módulos sin necesidad de acceder a la estructura completa de configuración.

Finalmente, proporciona una función helper "get_dbt_command" que genera comandos bash completos para ejecutar dbt con todas las variables de entorno y parámetros necesarios configurados correctamente.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección configura los paths de Python para permitir imports correctos. La segunda sección carga la configuración desde "config_loader.py" y la expone globalmente. La tercera sección determina el ambiente activo y selecciona la configuración correspondiente. La cuarta sección extrae y expone las variables de configuración más importantes como variables globales. Finalmente, la quinta sección define la función helper "get_dbt_command" para generar comandos dbt.

## 4. Configuración de Paths de Python

Al inicio del archivo se encuentra código que asegura que los directorios necesarios estén en el path de Python para permitir imports correctos. Este código obtiene el directorio actual del archivo usando "os.path.dirname" y "os.path.abspath", luego obtiene el directorio padre. Si el directorio padre no está en "sys.path", lo agrega al inicio de la lista. Si el directorio actual no está en "sys.path", también lo agrega. Esto asegura que los módulos puedan importarse correctamente tanto en desarrollo local como en Cloud Composer, independientemente de dónde se ejecute el código.

## 5. Carga de Configuración desde config_loader

El archivo intenta importar "config_loader" de dos maneras diferentes para mayor robustez. Primero intenta importar directamente "config_loader" como un módulo local. Si esto falla, intenta importar desde "modules.config_loader". Una vez importado, obtiene el objeto de configuración usando "config_loader.config", que es la instancia global de configuración cargada por "config_loader.py". Esta configuración se asigna a la variable "sources_config".

La variable "CONF" se crea asignándole el valor de "sources_config". Esta variable es la que se expone globalmente y puede ser importada en otros módulos usando "from modules.config import CONF". A través de "CONF" se puede acceder a toda la configuración del archivo "config.yaml" usando notación de puntos, por ejemplo "CONF.environments.dev.project_id" o "CONF.evaplan.api_base_url".

## 6. Determinación del Ambiente Activo

La variable "ENV" contiene el ambiente activo que determina qué configuración se usará. Esta variable se obtiene de la variable de entorno "ENVIRONMENT" usando "os.getenv". Si la variable de entorno no está definida, se usa un valor por defecto. El valor por defecto actual es "dev", lo que significa que si no se configura la variable de entorno "ENVIRONMENT", el sistema usará la configuración de desarrollo por defecto. Este valor puede cambiarse según las necesidades, usando "dev" para desarrollo o "prod" para producción.

El código incluye comentarios que explican cómo cambiar este valor por defecto. Para desarrollo se debe usar "dev", y para producción se debe usar "prod". Es importante notar que en Cloud Composer se recomienda configurar la variable de entorno "ENVIRONMENT" en lugar de cambiar el valor por defecto en el código, ya que esto permite cambiar el ambiente sin modificar código.

## 7. Selección de Configuración por Ambiente

Una vez determinado el ambiente activo en "ENV", el código selecciona la configuración correspondiente del objeto "CONF.environments". El código usa "getattr" para obtener la configuración del ambiente, pasando "CONF.environments" y "ENV" como argumentos. Esto permite acceder dinámicamente a la sección correcta según el valor de "ENV".

Si el ambiente especificado no existe en "CONF.environments", se captura la excepción "AttributeError" y se imprime una advertencia indicando que el ambiente no fue encontrado y que se usará "dev" como fallback. Luego se asigna "CONF.environments.dev" a "current_config". Esto asegura que el sistema siempre tenga una configuración válida, incluso si se especifica un ambiente incorrecto.

La variable "current_config" contiene la configuración completa del ambiente seleccionado, incluyendo "project_id", "location", "bucket_name", "dataset_bronze", "dataset_silver", y "dataset_gold". Esta configuración se usa para extraer los valores que se expondrán como variables globales.

## 8. Extracción de Valores de Configuración de GCP

El código extrae los valores de "project_id" y "location" de "current_config" usando "getattr" con un valor por defecto de "None". La variable "project_id_from_config" contiene el ID del proyecto de Google Cloud Platform del ambiente seleccionado. La variable "location_from_config" contiene la región de GCP del ambiente seleccionado.

Si "project_id_from_config" es "None" o está vacío, el código lanza un "ValueError" con un mensaje que indica que "project_id" no está definido en "config.yaml" para el ambiente especificado y que se debe verificar la sección "environments" correspondiente. Esto asegura que el sistema no continúe con valores inválidos.

De manera similar, si "location_from_config" es "None" o está vacío, el código lanza un "ValueError" indicando que "location" no está definido en "config.yaml" para el ambiente especificado. Estos errores son intencionales y evitan que el sistema funcione con configuraciones incompletas.

## 9. Variables Globales de GCP

La variable "PROJECT_ID" contiene el ID del proyecto de Google Cloud Platform que se usará en todas las operaciones. Esta variable se obtiene primero de la variable de entorno "GCP_PROJECT" usando "os.getenv". Si la variable de entorno no está definida, se usa el valor de "project_id_from_config" como fallback. Esto permite sobrescribir el proyecto mediante variables de entorno si es necesario, pero por defecto usa el valor de "config.yaml".

La variable "LOCATION" contiene la región de Google Cloud Platform donde se encuentran los recursos. Esta variable se obtiene primero de la variable de entorno "GCP_LOCATION" usando "os.getenv". Si la variable de entorno no está definida, se usa el valor de "location_from_config" como fallback. Al igual que "PROJECT_ID", esto permite sobrescribir mediante variables de entorno.

Estas variables se usan en todas las operaciones con BigQuery y Cloud Storage para especificar el proyecto y la región correctos. Son importadas frecuentemente en los DAGs y módulos usando "from modules.config import PROJECT_ID, LOCATION".

## 10. Variable de Configuración de Storage

La variable "DEFAULT_BUCKET_NAME" contiene el nombre del bucket de Google Cloud Storage que se usa como Data Lake. Esta variable se obtiene primero de la variable de entorno "GCS_BUCKET_NAME" usando "os.getenv". Si la variable de entorno no está definida, se usa el valor de "current_config.bucket_name" como fallback. Este bucket es donde se almacenan todos los archivos raw antes de ser cargados a BigQuery.

Esta variable se usa en los módulos de ingestión y carga para especificar dónde guardar y leer archivos en Google Cloud Storage. Es importada frecuentemente usando "from modules.config import DEFAULT_BUCKET_NAME".

## 11. Variables de Configuración de BigQuery

La variable "DATASET_ID_BRONZE" contiene el nombre del dataset de BigQuery para la capa Bronze, donde se almacenan los datos raw. Esta variable se obtiene primero de la variable de entorno "BQ_DATASET_BRONZE" usando "os.getenv". Si la variable de entorno no está definida, se usa el valor de "current_config.dataset_bronze" como fallback.

La variable "DATASET_ID_SILVER" contiene el nombre del dataset de BigQuery para la capa Silver, donde se almacenan los datos transformados. Esta variable se obtiene primero de la variable de entorno "BQ_DATASET_SILVER" usando "os.getenv". Si la variable de entorno no está definida, se usa el valor de "current_config.dataset_silver" como fallback.

La variable "DATASET_ID_GOLD" contiene el nombre del dataset de BigQuery para la capa Gold, donde se almacenan los datos agregados y optimizados. Esta variable se obtiene primero de la variable de entorno "BQ_DATASET_GOLD" usando "os.getenv". Si la variable de entorno no está definida, se usa el valor de "current_config.dataset_gold" como fallback.

Estas variables se usan en los módulos de carga y transformación para especificar en qué datasets de BigQuery se deben crear las tablas. También se pasan a dbt como variables de entorno para que los modelos SQL sepan en qué datasets escribir. Son importadas frecuentemente usando "from modules.config import DATASET_ID_BRONZE, DATASET_ID_SILVER, DATASET_ID_GOLD".

## 12. Variable de Ruta de DAGs

La variable "DAGS_FOLDER" contiene la ruta del directorio donde Airflow busca los DAGs. Esta variable se obtiene de la variable de entorno "AIRFLOW__CORE__DAGS_FOLDER" usando "os.environ.get". Si la variable de entorno no está definida, se usa el valor por defecto "/home/airflow/gcs/dags", que es la ruta estándar en Cloud Composer.

En desarrollo local, esta variable puede tener un valor diferente, pero en Cloud Composer generalmente es "/home/airflow/gcs/dags". Esta variable puede ser útil si algún módulo necesita conocer la ubicación de los DAGs, aunque no se usa frecuentemente en el código actual.

## 13. Función get_dbt_command

La función "get_dbt_command" es una función helper que genera un comando bash completo para ejecutar dbt con todas las variables de entorno y parámetros necesarios configurados correctamente. Esta función toma dos parámetros: "dbt_command" que es el comando dbt a ejecutar como string, por ejemplo "dbt run --select model_name", y "dbt_project_dir" que es la ruta del directorio del proyecto dbt.

La función crea un diccionario "env_vars" que contiene todas las variables de entorno que dbt necesita. La clave "DBT_PROJECT_ID" tiene el valor de "PROJECT_ID", que es el ID del proyecto de GCP. La clave "DBT_DATASET_BRONZE" tiene el valor de "DATASET_ID_BRONZE", que es el nombre del dataset Bronze. La clave "DBT_DATASET_SILVER" tiene el valor de "DATASET_ID_SILVER", que es el nombre del dataset Silver. La clave "DBT_DATASET_GOLD" tiene el valor de "DATASET_ID_GOLD", que es el nombre del dataset Gold. La clave "DBT_LOCATION" tiene el valor de "LOCATION", que es la región de GCP.

Luego, la función construye una cadena "export_vars" que contiene comandos "export" para cada variable de entorno, unidos con " && ". Esto crea una serie de comandos que exportan todas las variables de entorno antes de ejecutar dbt.

Además, la función construye argumentos "--vars" para pasar variables directamente a dbt mediante la línea de comandos. Esto asegura que las variables estén disponibles incluso si "env_var()" no funciona correctamente dentro de la sección "vars:" de "dbt_project.yml". La función crea un diccionario "dbt_vars" con las variables que dbt necesita: "project_id", "bronze_dataset", "silver_dataset", y "gold_dataset". Este diccionario se convierte a JSON usando "json.dumps", y luego se formatea como argumento "--vars" con el JSON entre comillas simples.

Finalmente, la función construye el comando completo que incluye "set -e" para que el script falle en caso de error, los comandos "export" para las variables de entorno, un cambio al directorio del proyecto dbt, el comando dbt con los argumentos "--vars", y argumentos adicionales "--project-dir" y "--profiles-dir" para especificar los directorios del proyecto y perfiles. El comando también redirige la salida estándar y de error, y maneja errores mostrando el código de salida si el comando falla.

La función retorna el comando bash completo como string, que puede ser ejecutado directamente en un "BashOperator" de Airflow o en cualquier shell de bash.

## 14. Cómo se Usa en el Código

El archivo "config.py" se importa en los DAGs y módulos para acceder a la configuración del sistema. La forma más común de importar es usando "from modules.config import CONF, PROJECT_ID, LOCATION, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE, DATASET_ID_SILVER, DATASET_ID_GOLD". Esto permite acceder directamente a las variables globales sin necesidad de acceder a la estructura completa de "CONF".

Para acceder a la configuración de una fuente específica, se usa "CONF" con notación de puntos. Por ejemplo, "CONF.evaplan.api_base_url" obtiene la URL base de la API de Evaplan, o "CONF.ipm.tables.bronze" obtiene el nombre de la tabla Bronze de IPM.

Para usar la función "get_dbt_command", se importa con "from modules.config import get_dbt_command", y luego se llama pasando el comando dbt y el directorio del proyecto. Por ejemplo, "get_dbt_command('dbt run --select model_name', '/path/to/dbt')" retorna un comando bash completo listo para ejecutar.

## 15. Flujo de Ejecución

Cuando el módulo se importa por primera vez, se ejecuta todo el código a nivel de módulo. Primero se configuran los paths de Python. Luego se carga la configuración desde "config_loader.py" y se asigna a "CONF". Después se determina el ambiente activo y se selecciona la configuración correspondiente. Luego se extraen y validan los valores de configuración, y se crean las variables globales. Finalmente, la función "get_dbt_command" está disponible para ser llamada cuando sea necesario.

Todo este proceso ocurre automáticamente cuando se importa el módulo, por lo que las variables globales están disponibles inmediatamente después de la importación. No es necesario llamar ninguna función de inicialización.

## 16. Manejo de Errores

El código incluye varios mecanismos de manejo de errores. Si el ambiente especificado no existe en "config.yaml", se captura la excepción y se usa "dev" como fallback, imprimiendo una advertencia. Si "project_id" o "location" no están definidos en "config.yaml" para el ambiente seleccionado, se lanza un "ValueError" con un mensaje descriptivo que indica exactamente qué está mal y cómo corregirlo.

Estos errores son intencionales y aseguran que el sistema no funcione con configuraciones incompletas o incorrectas. Es mejor que el sistema falle claramente al inicio que funcionar con valores incorrectos que causen problemas más adelante.

## 17. Variables de Entorno como Override

Todas las variables de configuración importantes pueden ser sobrescritas mediante variables de entorno. Esto es útil en situaciones donde se necesita cambiar temporalmente un valor sin modificar "config.yaml", o cuando se quiere usar un valor diferente en un ambiente específico de Cloud Composer.

Las variables de entorno que pueden sobrescribir valores son "GCP_PROJECT" para "PROJECT_ID", "GCP_LOCATION" para "LOCATION", "GCS_BUCKET_NAME" para "DEFAULT_BUCKET_NAME", "BQ_DATASET_BRONZE" para "DATASET_ID_BRONZE", "BQ_DATASET_SILVER" para "DATASET_ID_SILVER", y "BQ_DATASET_GOLD" para "DATASET_ID_GOLD". Si estas variables de entorno están definidas, sus valores tienen prioridad sobre los valores en "config.yaml".

## 18. Notas Importantes

Es importante entender que este módulo se ejecuta cuando se importa, no cuando se llama una función. Esto significa que el ambiente se determina en el momento de la importación, no en el momento de usar las variables. Si se cambia la variable de entorno "ENVIRONMENT" después de importar el módulo, no tendrá efecto hasta que se reinicie el proceso de Python.

La función "get_dbt_command" es esencial para ejecutar comandos dbt correctamente, ya que configura todas las variables de entorno y parámetros necesarios. Sin esta función, los comandos dbt podrían no tener acceso a las variables correctas y fallar.

El código está diseñado para no usar valores hardcodeados como fallback. Si un valor requerido no está en "config.yaml" ni en variables de entorno, el sistema lanza un error en lugar de usar un valor por defecto. Esto asegura que todas las configuraciones sean explícitas y no haya sorpresas con valores inesperados.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/config.py"  
Versión del archivo: 3.0

