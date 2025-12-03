# Documentación: modules/ipm/ipm_sisben_load.py

## 1. Información General

El archivo "modules/ipm/ipm_sisben_load.py" es un módulo Python simplificado que se encarga de procesar archivos Excel de IPM SISBEN desde Google Cloud Storage. Este módulo es diferente de "ipm_load.py" porque no carga los datos directamente a BigQuery, sino que lee el Excel, lo convierte a CSV, y lo guarda en GCS. El propósito de este módulo es preparar los datos en formato CSV para que sean cargados a BigQuery como una tabla externa en lugar de cargar los datos directamente. Esta aproximación es útil para archivos grandes donde cargar directamente a BigQuery puede ser ineficiente.

## 2. Propósito y Funcionalidad

El archivo "ipm_sisben_load.py" cumple funciones específicas para IPM SISBEN. En primer lugar, asegura que el dataset bronze exista en BigQuery. En segundo lugar, lee el archivo Excel más reciente desde una carpeta en GCS usando pandas. En tercer lugar, convierte el DataFrame a formato CSV y lo guarda en otra carpeta de GCS. Esta conversión a CSV es necesaria porque BigQuery puede crear tablas externas desde archivos CSV en GCS, lo cual es más eficiente para archivos grandes que cargar los datos directamente. El módulo es intencionalmente simple porque la carga real a BigQuery se hace mediante la creación de una tabla externa en el DAG correspondiente.

## 3. Estructura del Archivo

El archivo está organizado de manera simple. Contiene imports y configuración, función "ensure_dataset" para crear el dataset, función "leer_excel_desde_bucket" que lee el Excel más reciente desde GCS y lo retorna como DataFrame, y función "convertir_excel_a_csv_y_guardar" que convierte el DataFrame a CSV y lo guarda en GCS. El módulo no contiene funciones de carga directa a BigQuery porque esa funcionalidad se maneja en el DAG mediante la creación de una tabla externa.

## 4. Función leer_excel_desde_bucket

La función "leer_excel_desde_bucket" lee el archivo Excel más reciente desde el bucket de GCS. La función normaliza la ruta de la carpeta, obtiene el bucket, lista todos los blobs en la carpeta, filtra solo archivos ".xlsx", ordena por tiempo de creación en orden descendente, toma el más reciente, descarga el archivo a un archivo temporal, lo lee con pandas usando "pd.read_excel", y retorna el DataFrame. Limpia el archivo temporal en un bloque "finally" para asegurar que se elimine incluso si hay errores. Esta función es similar a funciones en otros módulos de carga pero más simple porque no aplica transformaciones complejas.

## 5. Función convertir_excel_a_csv_y_guardar

La función "convertir_excel_a_csv_y_guardar" convierte un DataFrame a CSV y lo guarda en el bucket de GCS. La función normaliza la ruta de la carpeta, genera un nombre de archivo CSV con timestamp si no se proporciona uno, asegura que el nombre termine en ".csv", crea un archivo CSV temporal, guarda el DataFrame como CSV usando "to_csv" con encoding UTF-8, sube el CSV a GCS, y retorna la URI completa del archivo. Limpia el archivo temporal en un bloque "finally". Esta función es importante porque prepara los datos en formato CSV para la creación de la tabla externa en BigQuery.

## 6. Filosofía de Tabla Externa

Este módulo está diseñado para trabajar con tablas externas de BigQuery en lugar de cargar datos directamente. Las tablas externas referencian archivos en GCS sin copiar los datos a BigQuery, lo cual es más eficiente para archivos grandes. El módulo prepara el CSV en GCS, y el DAG correspondiente crea la tabla externa usando SQL "CREATE EXTERNAL TABLE" que apunta al archivo CSV. Esta aproximación es especialmente útil para IPM SISBEN donde los archivos pueden ser muy grandes.

## 7. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IPM SISBEN. Los DAGs primero aseguran que el dataset existe, luego llaman a "leer_excel_desde_bucket" para leer el Excel, luego llaman a "convertir_excel_a_csv_y_guardar" para convertir y guardar el CSV, y finalmente crean la tabla externa en BigQuery usando "BigQueryInsertJobOperator" con una query SQL que crea la tabla externa apuntando al CSV en GCS. Esta separación de responsabilidades permite que el módulo se enfoque en la preparación de datos mientras que el DAG maneja la creación de la tabla externa.

## 8. Notas Importantes

Este módulo es más simple que otros módulos de carga porque no carga datos directamente a BigQuery. La carga real se hace mediante tablas externas que se crean en el DAG. El CSV se guarda con encoding UTF-8 y delimitador por defecto de pandas, que es coma. Si el formato del CSV necesita ser diferente, se puede ajustar en la función "to_csv". El módulo no aplica transformaciones complejas a los datos porque se asume que las transformaciones se harán en la capa silver con dbt o mediante queries SQL sobre la tabla externa. El nombre del archivo CSV incluye un timestamp para evitar conflictos si se ejecuta múltiples veces.

---

Última actualización: 2025-01-XX  
Archivo documentado: "modules/ipm/ipm_sisben_load.py"  
Versión del archivo: 3.0

