DOCUMENTACION DETALLADA DEL PROYECTO DBT

Este documento describe en detalle la estructura y funcionamiento del proyecto dbt utilizado para transformar los datos del Indice de Pobreza Multidimensional IPM desde la capa bronze hasta la capa gold en BigQuery. El proyecto dbt implementa una serie de transformaciones progresivas que limpian, validan y estructuran los datos para su consumo final.

ESTRUCTURA GENERAL DEL PROYECTO

El proyecto dbt se organiza en varias carpetas principales que siguen las convenciones estandar de dbt:

- models: Contiene todos los modelos SQL que definen las transformaciones de datos. Se subdivide en carpetas por capa: silver y gold.
- macros: Contiene macros reutilizables de dbt, funciones SQL personalizadas que se pueden usar en los modelos.
- sources.yml: Define las fuentes de datos externas, principalmente las tablas de la capa bronze que se leen como entrada.
- dbt_project.yml: Archivo de configuracion principal del proyecto que define metadatos, rutas y configuraciones globales.

El proyecto utiliza dbt version 2, lo que significa que usa la sintaxis moderna de dbt con config-version: 2.

ARCHIVO: dbt_project.yml

Este archivo es el corazon de la configuracion del proyecto dbt. Define el nombre del proyecto, la version, y todas las rutas y configuraciones necesarias.

El nombre del proyecto es gdv_general_dbt_dag y la version es 1.0.0. El perfil utilizado es gdv_general_dbt_dag, que debe estar definido en el archivo profiles.yml que se encuentra en la ruta especificada por DBT_PROFILES_DIR.

Las rutas configuradas son:
- model-paths: Indica que los modelos se encuentran en la carpeta models
- analysis-paths: Para analisis SQL que no se materializan
- test-paths: Para pruebas de calidad de datos
- seed-paths: Para archivos de datos semilla
- macro-paths: Para macros reutilizables
- snapshot-paths: Para snapshots de datos

Los directorios de limpieza clean-targets incluyen target y dbt_packages, que son directorios generados automaticamente por dbt y que se pueden eliminar de forma segura.

La seccion models define configuraciones especificas para diferentes grupos de modelos:
- silver: Los modelos en la carpeta silver se materializan como vistas views y usan la base de datos datagov-473122. Esta configuracion aplica a todos los modelos intermedios de transformacion en la capa silver.
- gold: Los modelos en la carpeta gold se materializan como tablas tables y tambien usan la base de datos datagov-473122. Esta configuracion aplica al modelo final de consumo.

Nota importante: Aunque la configuracion de silver especifica que los modelos se materializan como vistas, el modelo final rawdata_ipmv2_clean sobrescribe esta configuracion y se materializa como tabla table, como se especifica en su configuracion individual.

ARCHIVO: sources.yml

Este archivo define las fuentes de datos externas que los modelos dbt pueden leer. En dbt, las fuentes permiten referenciar tablas que no son parte del proyecto dbt, como las tablas de la capa bronze que se crean mediante Python.

El archivo define una unica fuente principal:

La fuente bronze_ipmv2 es la fuente principal y unica para este pipeline. Se refiere al schema bronze_dpt_planeacion_municipal_dev y contiene la tabla ipm_raw_data. Esta es la tabla que se crea en la capa bronze por el modulo ipm_transform.py y que contiene los datos originales en formato STRING, preservando todos los valores tal como vienen del archivo Excel sin transformaciones.

Esta fuente se referencia en el modelo rawdata_ipmv2_stg usando la sintaxis source('bronze_ipmv2', 'ipm_raw_data'), que es el punto de entrada del pipeline de transformaciones dbt.

ARCHIVO: macros/generate_schema_name.sql

Este macro personaliza como dbt genera los nombres de los schemas para los modelos. Por defecto, dbt tiene su propia logica para generar nombres de schemas, pero este macro permite controlar ese comportamiento.

El macro funciona de la siguiente manera: recibe dos parametros, custom_schema_name que es el nombre de schema personalizado especificado en la configuracion del modelo, y node que es el nodo del modelo actual.

Si custom_schema_name es None, es decir, si no se especifico un schema personalizado en la configuracion del modelo, entonces usa el schema del target, que viene de la configuracion del perfil en profiles.yml.

Si custom_schema_name tiene un valor, lo usa directamente, pero primero lo limpia con trim para eliminar espacios en blanco al inicio y final.

Este macro asegura que cuando se especifica un schema en la configuracion de un modelo usando schema, ese schema se use exactamente como se especifico, sin modificaciones adicionales por parte de dbt.

MODELOS DE LA CAPA SILVER

La capa silver contiene siete modelos que transforman progresivamente los datos desde bronze hasta una tabla final limpia y validada. Cada modelo se materializa como una vista view excepto el ultimo que se materializa como tabla table. Todos los modelos se crean en el schema silver_dpt_planeacion_municipal_dev.

MODELO: rawdata_ipmv2_stg

Este es el primer modelo de la capa silver y actua como una capa de staging. Su proposito principal es leer los datos de bronze y normalizar los nombres de las columnas de PascalCase o formato mixto a snake_case, que es el estandar utilizado en el resto del pipeline.

El modelo se materializa como una vista en el schema silver_dpt_planeacion_municipal_dev. Lee directamente de la fuente bronze_ipmv2 usando source('bronze_ipmv2', 'ipm_raw_data').

Las transformaciones que realiza son puramente de renombrado de columnas:
- cod_mpio se mantiene igual
- Municipio se convierte a municipio
- Total se convierte a total
- IPM_Pobre_Abs se convierte a ipm_pobre_abs
- IPM_No_Pobre_Abs se convierte a ipm_no_pobre_abs
- IPM_Pobre_Porc se convierte a ipm_pobre_porc
- IPM_No_Pobre_Porc se convierte a ipm_no_pobre_porc

Para cada indicador del I1 al I15, se renombran las cuatro columnas correspondientes:
- I1_Con_Privacion_Abs se convierte a i1_con_privacion_abs
- I1_Sin_Privacion_Abs se convierte a i1_sin_privacion_abs
- I1_Con_Privacion_Porc se convierte a i1_con_privacion_porc
- I1_Sin_Privacion_Porc se convierte a i1_sin_privacion_porc

Y asi sucesivamente para todos los indicadores hasta I15.

La columna fecha_lectura se mantiene sin cambios.

Este modelo no realiza ninguna transformacion de datos, solo estandariza los nombres de las columnas para facilitar el trabajo en los modelos posteriores.

MODELO: rawdata_ipmv2_normalize_text

Este modelo normaliza el texto de los campos identificadores cod_mpio y municipio. Su proposito es estandarizar estos campos eliminando acentos y convirtiendo todo a mayusculas, lo que facilita las comparaciones y los joins con otras tablas.

El modelo se materializa como una vista y lee del modelo anterior rawdata_ipmv2_stg usando ref('rawdata_ipmv2_stg').

Para cod_mpio, aplica la siguiente transformacion: primero convierte el valor a STRING usando CAST, luego lo convierte a minusculas con LOWER, luego usa TRANSLATE para reemplazar los caracteres acentuados por sus equivalentes sin acento, y finalmente convierte todo a mayusculas con UPPER. El mapeo de caracteres es: a con acento a a, e con acento a e, i con acento a i, o con acento a o, u con acento a u, n con tilde a n, y lo mismo para las versiones mayusculas.

Para municipio, aplica exactamente la misma transformacion.

Todas las demas columnas se mantienen sin cambios, pasando directamente desde el modelo anterior. Esto incluye total, todas las columnas de IPM, todos los indicadores I1 a I15 tanto absolutos como porcentajes, y fecha_lectura.

La normalizacion de texto es importante porque los nombres de municipios pueden venir con diferentes formatos, acentos, y casos, y esta estandarizacion asegura que se puedan hacer comparaciones y joins de manera confiable.

MODELO: rawdata_ipmv2_transform_types

Este modelo intenta convertir los tipos de datos de STRING a los tipos apropiados. En la capa bronze, todos los valores se guardan como STRING para preservar los datos originales, pero en silver necesitamos convertirlos a tipos numericos para poder hacer calculos y validaciones.

El modelo se materializa como una vista y lee del modelo rawdata_ipmv2_normalize_text usando ref('rawdata_ipmv2_normalize_text').

Para las columnas que deberian ser enteros valores absolutos, usa SAFE_CAST para convertir de STRING a INT64. SAFE_CAST es una funcion de BigQuery que retorna NULL en lugar de lanzar un error si la conversion falla. Esto es importante porque algunos valores pueden tener caracteres no numericos que no se pueden convertir directamente.

Las columnas que se convierten a INT64 son: total, ipm_pobre_abs, ipm_no_pobre_abs, y todos los indicadores I1 a I15 tanto con_privacion_abs como sin_privacion_abs.

Para las columnas que deberian ser flotantes porcentajes, usa SAFE_CAST para convertir de STRING a FLOAT64 y luego aplica ROUND con 2 decimales para estandarizar la precision. Las columnas que se convierten a FLOAT64 son: ipm_pobre_porc, ipm_no_pobre_porc, y todos los indicadores I1 a I15 tanto con_privacion_porc como sin_privacion_porc.

Los campos cod_mpio, municipio y fecha_lectura se mantienen sin cambios.

Es importante notar que en este punto, si un valor tiene caracteres no numericos mezclados con numeros, la conversion fallara y el valor sera NULL. Estos valores NULL se manejaran en los modelos posteriores.

MODELO: rawdata_ipmv2_clean_numbers

Este modelo es uno de los mas complejos y criticos. Su proposito es limpiar las columnas numericas eliminando letras, espacios y caracteres especiales que puedan estar mezclados con los numeros, dejando solo los numeros validos.

El modelo se materializa como una vista y lee del modelo rawdata_ipmv2_normalize_text usando ref('rawdata_ipmv2_normalize_text'). Nota importante: este modelo lee de normalize_text, no de transform_types, lo que significa que trabaja directamente con los valores STRING antes de intentar convertirlos a tipos numericos.

Para cod_mpio, aplica una limpieza especial: primero elimina espacios usando REGEXP_REPLACE con el patron r'\s', luego elimina todos los caracteres que no sean numeros usando REGEXP_REPLACE con el patron r'[^0-9]'. El resultado es cod_mpio_cleaned que contiene solo numeros.

Para total y todas las columnas numericas absolutas, aplica un proceso de limpieza en varias etapas usando REGEXP_REPLACE anidados:

Primera etapa: Maneja valores NULL o cadenas vacias usando COALESCE y TRIM, convirtiendo todo a STRING.

Segunda etapa: Elimina espacios usando REGEXP_REPLACE con el patron r'\s'.

Tercera etapa: Elimina letras y caracteres especiales excepto numeros, comas, puntos y signos menos, usando REGEXP_REPLACE con el patron r'[^0-9.,\-]'. Esto permite preservar numeros con formato decimal o separadores de miles.

Cuarta etapa: Elimina comas o puntos al final de la cadena usando REGEXP_REPLACE con el patron r'[,.]$'. Esto limpia casos donde hay un separador al final que no tiene sentido.

Quinta etapa: Elimina comas y puntos internos usando REGEXP_REPLACE con el patron r'[.,]'. Esto convierte numeros como "1.234" o "1,234" a "1234", que luego se convierte a entero.

Finalmente, usa SAFE_CAST para convertir el resultado a INT64. Si despues de toda la limpieza el valor no se puede convertir, sera NULL.

Este proceso se aplica a: total_cleaned, ipm_pobre_abs_cleaned, ipm_no_pobre_abs_cleaned, y todos los indicadores I1 a I15 tanto con_privacion_abs_cleaned como sin_privacion_abs_cleaned.

Las columnas de porcentajes se mantienen sin cambios en este modelo, pasando directamente desde normalize_text. Esto incluye ipm_pobre_porc, ipm_no_pobre_porc, y todos los indicadores I1 a I15 tanto con_privacion_porc como sin_privacion_porc.

La columna fecha_lectura tambien se mantiene sin cambios.

El resultado de este modelo son valores numericos limpios en las columnas absolutas, con el sufijo _cleaned para indicar que han sido procesadas.

MODELO: rawdata_ipmv2_detect_negatives

Este modelo detecta problemas en los datos y crea flags de validacion que se usaran en el siguiente modelo para aplicar reglas de negocio.

El modelo se materializa como una vista y lee del modelo rawdata_ipmv2_clean_numbers usando ref('rawdata_ipmv2_clean_numbers').

El modelo selecciona todas las columnas del modelo anterior usando SELECT *, y luego agrega dos columnas nuevas de tipo booleano:

La primera columna es has_negative_value. Esta columna detecta si hay algun valor negativo en cualquiera de las columnas numericas absolutas del registro. La logica es: si cualquiera de las siguientes columnas tiene un valor menor que cero, entonces has_negative_value es TRUE, de lo contrario es FALSE. Las columnas que se verifican son: total_cleaned, ipm_pobre_abs_cleaned, ipm_no_pobre_abs_cleaned, y todos los indicadores I1 a I15 tanto con_privacion_abs_cleaned como sin_privacion_abs_cleaned. Se usa COALESCE para manejar valores NULL, tratandolos como 0 para la comparacion.

La segunda columna es is_total_zero_or_empty. Esta columna detecta si el campo total_cleaned es NULL o tiene valor 0. La logica es: si total_cleaned IS NULL o si COALESCE(total_cleaned, 0) = 0, entonces is_total_zero_or_empty es TRUE, de lo contrario es FALSE.

Estos flags se usaran en el siguiente modelo para aplicar reglas de validacion: si hay valores negativos o si el total es cero o vacio, se aplicaran reglas especiales para manejar esos casos.

MODELO: rawdata_ipmv2_apply_validations

Este modelo aplica las validaciones finales y las reglas de negocio basandose en los flags detectados en el modelo anterior.

El modelo se materializa como una vista y lee del modelo rawdata_ipmv2_detect_negatives usando ref('rawdata_ipmv2_detect_negatives').

Para cod_mpio, usa cod_mpio_cleaned del modelo anterior y lo renombra a cod_mpio. Para municipio, lo mantiene sin cambios.

Para total, aplica la siguiente logica usando CASE: si has_negative_value es TRUE, entonces total es 0. Si is_total_zero_or_empty es TRUE, entonces total es 0. De lo contrario, usa COALESCE(total_cleaned, 0), es decir, el valor limpio o 0 si es NULL.

Para todas las columnas numericas absolutas ipm_pobre_abs, ipm_no_pobre_abs, y todos los indicadores I1 a I15 tanto con_privacion_abs como sin_privacion_abs, aplica la misma logica: si has_negative_value es TRUE o si is_total_zero_or_empty es TRUE, entonces el valor es 0. De lo contrario, usa COALESCE de la columna correspondiente _cleaned con 0 como valor por defecto.

Para las columnas de porcentajes ipm_pobre_porc, ipm_no_pobre_porc, y todos los indicadores I1 a I15 tanto con_privacion_porc como sin_privacion_porc, aplica una logica similar pero con conversion a FLOAT64: si is_total_zero_or_empty es TRUE, entonces el valor es 0.0. De lo contrario, intenta convertir el valor de STRING a FLOAT64 usando SAFE_CAST, y si la conversion falla o es NULL, usa 0.0 como valor por defecto.

Nota importante: los porcentajes se convierten aqui porque en el modelo clean_numbers se mantuvieron como STRING. Esta conversion se hace en este punto porque ya tenemos los flags de validacion y podemos aplicar la logica de negocio antes de convertir.

La columna fecha_lectura se mantiene sin cambios.

El resultado de este modelo son datos completamente validados y limpios, con todas las reglas de negocio aplicadas. Los valores negativos se han convertido a 0, los registros con total cero o vacio tienen todas sus columnas numericas en 0, y los porcentajes se han convertido correctamente a FLOAT64.

MODELO: rawdata_ipmv2_clean

Este es el modelo final de la capa silver. Su proposito es materializar los datos limpios como una tabla fisica y realizar la transformacion final de la fecha.

El modelo se materializa como una tabla table en el schema silver_dpt_planeacion_municipal_dev con el alias ipm_transformed_data. El alias permite que la tabla tenga un nombre mas descriptivo y consistente con las convenciones de nombres del proyecto.

El modelo lee del modelo rawdata_ipmv2_apply_validations usando ref('rawdata_ipmv2_apply_validations').

Todas las columnas se pasan directamente sin cambios, excepto fecha_lectura que se transforma usando DATE(fecha_lectura). La funcion DATE extrae solo la parte de fecha del timestamp, eliminando la hora, minutos y segundos. El resultado es un tipo DATE en formato YYYY-MM-DD.

Esta es la unica transformacion que se hace en este modelo, ya que todas las demas transformaciones y validaciones ya se completaron en los modelos anteriores.

El resultado es una tabla fisica en BigQuery que contiene los datos completamente limpios, validados y listos para ser consumidos por la capa gold o por aplicaciones de analisis.

MODELOS DE LA CAPA GOLD

La capa gold contiene un solo modelo que transforma los datos de silver a un formato final simplificado para consumo de usuarios finales.

MODELO: rawdata_ipmv2_gold

Este modelo crea la capa final de consumo con un formato simplificado y estandarizado para los usuarios finales.

El modelo se materializa como una tabla table en el schema gold_dpt_planeacion_municipal_dev con el alias ipm_processed_data.

El modelo lee del modelo final de silver rawdata_ipmv2_clean usando ref('rawdata_ipmv2_clean').

Las transformaciones que realiza son principalmente de renombrado y simplificacion:

- cod_mpio se mantiene igual
- municipio se renombra a Municipio con mayuscula inicial
- total se renombra a Total con mayuscula inicial
- ipm_pobre_abs se renombra a IPM_Pobre eliminando el sufijo _abs
- ipm_no_pobre_abs se renombra a IPM_No_Pobre eliminando el sufijo _abs

Para cada indicador del I1 al I15:
- i1_con_privacion_abs se renombra a I1_CON_PRIVACION eliminando el sufijo _abs y usando mayusculas
- i1_sin_privacion_abs se renombra a I1_SIN_PRIVACION eliminando el sufijo _abs y usando mayusculas

Y asi sucesivamente para todos los indicadores hasta I15.

Importante: Este modelo NO incluye:
- Las columnas de porcentajes ipm_pobre_porc, ipm_no_pobre_porc, ni los porcentajes de los indicadores I1 a I15
- La columna fecha_lectura

El resultado es una tabla simplificada que contiene solo los valores absolutos de los indicadores, con nombres en mayusculas y sin sufijos, lo que facilita su uso en herramientas de visualizacion y reportes.

FLUJO COMPLETO DE TRANSFORMACIONES

El flujo completo de transformaciones en dbt es el siguiente:

1. rawdata_ipmv2_stg: Lee de bronze y normaliza nombres de columnas a snake_case.

2. rawdata_ipmv2_normalize_text: Normaliza texto de cod_mpio y municipio eliminando acentos y convirtiendo a mayusculas.

3. rawdata_ipmv2_transform_types: Intenta convertir tipos de STRING a INT64 para absolutos y FLOAT64 para porcentajes. Nota: Este modelo y el siguiente pueden ejecutarse en paralelo porque ambos dependen solo de normalize_text.

4. rawdata_ipmv2_clean_numbers: Limpia columnas numericas eliminando letras y caracteres especiales, convirtiendo a INT64.

5. rawdata_ipmv2_detect_negatives: Detecta valores negativos y totales cero o vacios, creando flags de validacion.

6. rawdata_ipmv2_apply_validations: Aplica reglas de negocio basadas en los flags, convierte porcentajes a FLOAT64, y valida todos los datos.

7. rawdata_ipmv2_clean: Materializa como tabla y convierte fecha_lectura a tipo DATE.

8. rawdata_ipmv2_gold: Crea la capa final simplificada para consumo.

CARACTERISTICAS IMPORTANTES DEL PROYECTO

El proyecto dbt tiene varias caracteristicas importantes:

Materializacion progresiva: Los modelos intermedios se materializan como vistas, lo que es eficiente en espacio y tiempo de ejecucion, mientras que los modelos finales se materializan como tablas para mejor rendimiento de consultas.

Manejo robusto de errores: El uso de SAFE_CAST en lugar de CAST asegura que los errores de conversion no detengan el proceso completo, sino que se conviertan en NULL que luego se manejan apropiadamente.

Validacion de datos: El sistema de flags y validaciones asegura que los datos cumplan con las reglas de negocio antes de llegar a la capa final.

Separacion de responsabilidades: Cada modelo tiene una responsabilidad clara y especifica, lo que facilita el mantenimiento y la depuracion.

Trazabilidad: El uso de ref() y source() crea un grafo de dependencias que dbt puede rastrear, permitiendo ejecutar solo los modelos afectados por cambios.

Versionado: Los modelos SQL estan versionados en el control de versiones, permitiendo rastrear cambios y colaborar en equipo.

Documentacion: Los comentarios en los modelos SQL proporcionan documentacion inline sobre el proposito de cada transformacion.

INTEGRACION CON EL PIPELINE

El proyecto dbt se integra con el pipeline de Airflow de la siguiente manera:

El DAG src_ipm_transform_dag.py ejecuta los modelos dbt en el orden correcto usando BashOperator que ejecuta comandos dbt run --select modelo. Cada modelo se ejecuta como una tarea separada en Airflow, lo que permite monitoreo granular y manejo de errores.

Las dependencias entre modelos se manejan tanto en dbt usando ref() como en Airflow usando las dependencias de tareas, asegurando que los modelos se ejecuten en el orden correcto.

Los modelos se ejecutan en el contexto del proyecto dbt configurado en dbt_project.yml, usando las credenciales y configuracion del perfil especificado en profiles.yml.

