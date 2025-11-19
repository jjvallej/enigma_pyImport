DOCUMENTACION DETALLADA DEL MODULO ipm_transform.py

Este documento describe en detalle el funcionamiento del modulo ipm_transform.py, que proporciona funciones auxiliares para la gestion de datasets en BigQuery utilizadas durante el proceso de transformacion de datos desde la capa bronze hacia las capas silver y gold. Este modulo es complementario a los modelos dbt que realizan las transformaciones reales de los datos.

PROPOSITO DEL MODULO

El modulo ipm_transform.py esta disenado para proporcionar funcionalidades auxiliares relacionadas con la gestion de datasets en BigQuery para las capas silver y gold del pipeline de datos IPM. A diferencia del modulo ipm_extract.py que maneja la extraccion y carga a bronze, este modulo se enfoca exclusivamente en asegurar que los datasets necesarios para las transformaciones existan antes de ejecutar los modelos dbt.

IMPORTANTE: Las transformaciones reales de los datos se realizan mediante modelos dbt ubicados en gdv_general_dbt_dag/dbt/models/silver/ y gdv_general_dbt_dag/dbt/models/gold/. Este modulo solo proporciona funciones de soporte para la infraestructura.

CONFIGURACION INICIAL

El archivo comienza con la importacion de las librerias necesarias. Utiliza google.cloud.bigquery para interactuar con BigQuery, y librerias estandar de Python para manejo de variables de entorno y operaciones del sistema.

Las constantes principales son:
- PROJECT_ID: Identificador del proyecto de Google Cloud, actualmente "datagov-473122"
- SA_PATH: Ruta al archivo de credenciales de servicio, ubicado en "/opt/airflow/include/sa.json"
- DEBUG: Variable booleana que controla si se muestran mensajes de depuracion detallados

FUNCIONES DE CLIENTES

El modulo define una funcion privada para crear clientes de Google Cloud:

_bq_client: Crea y retorna un cliente de BigQuery. Esta funcion establece la variable de entorno GOOGLE_APPLICATION_CREDENTIALS con la ruta del archivo de credenciales y luego crea un cliente de BigQuery asociado al proyecto configurado. Este cliente se utiliza para todas las operaciones relacionadas con BigQuery, especificamente para verificar y crear datasets.

FUNCION ensure_dataset

Esta funcion verifica si existe un dataset en BigQuery y lo crea si no existe. Recibe como parametros el identificador del dataset y opcionalmente la ubicacion geografica, que por defecto es "us-central1".

El proceso es el siguiente: primero construye el nombre completo del dataset usando el PROJECT_ID y el dataset_id proporcionado. Luego intenta obtener el dataset usando el cliente de BigQuery. Si el dataset existe, imprime un mensaje de confirmacion si DEBUG esta activado. Si no existe, crea un nuevo dataset con la ubicacion especificada.

La descripcion del dataset se asigna automaticamente segun el tipo:
- Si el dataset_id contiene "silver", se asigna la descripcion "Silver layer para IPM v2"
- Si el dataset_id contiene "gold", se asigna la descripcion "Gold layer para IPM v2"
- En otros casos, se asigna la descripcion "Dataset para IPM v2"

Finalmente imprime un mensaje indicando que el dataset fue creado exitosamente.

Esta funcion es utilizada por el DAG src_planeacion_transf_ipm para asegurar que los datasets silver_dpt_planeacion_municipal_dev y gold_dpt_planeacion_municipal_dev existan antes de ejecutar los modelos dbt.

CARACTERISTICAS IMPORTANTES DEL MODULO

El modulo tiene varias caracteristicas importantes:

Simplicidad y enfoque: El modulo se enfoca exclusivamente en la gestion de datasets, manteniendo la logica simple y clara. Las transformaciones complejas se delegan a dbt.

Descripciones automaticas: Las descripciones de los datasets se asignan automaticamente segun el tipo de capa (silver o gold), facilitando la identificacion en la consola de BigQuery.

Mensajes de depuracion: Cuando DEBUG esta activado, el modulo proporciona mensajes detallados sobre el estado de los datasets, lo que facilita la depuracion y el monitoreo.

Idempotencia: La funcion ensure_dataset puede ejecutarse multiples veces sin causar errores. Si el dataset ya existe, simplemente imprime un mensaje de confirmacion.

INTEGRACION CON EL PIPELINE

Este modulo se utiliza en el tercer paso del pipeline de datos IPM:

1. El modulo ipm_load.py transfiere el archivo desde Google Drive a GCS.
2. El modulo ipm_extract.py extrae el archivo de GCS, aplica transformaciones minimas y lo carga en BigQuery en la capa bronze (bronze_dpt_planeacion_municipal_dev.ipm_raw_data).
3. El DAG src_planeacion_transf_ipm utiliza este modulo (ipm_transform.py) para asegurar que los datasets silver y gold existan, y luego ejecuta los modelos dbt para transformar los datos desde bronze a silver (silver_dpt_planeacion_municipal_dev.ipm_transformed_data) y gold (gold_dpt_planeacion_municipal_dev.ipm_processed_data).

La separacion de responsabilidades es clara:
- ipm_load.py: Transferencia desde Drive a GCS
- ipm_extract.py: Extraccion y carga a bronze
- ipm_transform.py: Gestion de datasets para silver y gold (infraestructura)
- Modelos dbt: Transformaciones reales de los datos de bronze a silver y gold

Esta separacion permite que cada modulo se enfoque en su tarea especifica y facilita el mantenimiento y las pruebas.

CASOS DE USO

El modulo se puede usar en diferentes escenarios:

Caso 1: Verificacion previa en DAGs. El modulo puede usarse en DAGs de Airflow antes de ejecutar modelos dbt para asegurar que los datasets necesarios existan.

Caso 2: Inicializacion de infraestructura. El modulo puede usarse para inicializar los datasets cuando se configura el pipeline por primera vez.


