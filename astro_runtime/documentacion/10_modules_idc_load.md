# Documentación: modules/idc/idc_load.py

## 1. Información General

El archivo "modules/idc/idc_load.py" es un módulo Python que se encarga de extraer datos desde Google Cloud Storage, transformarlos mínimamente y cargarlos en la capa bronze de BigQuery para IDC. Este módulo es más complejo que "ipm_load.py" porque maneja múltiples hojas del Excel, cada una de las cuales se carga como una tabla separada en BigQuery. El archivo Excel de IDC tiene una estructura especial donde la primera hoja contiene metadatos y estructura, y las hojas siguientes contienen los datos reales que deben procesarse.

## 2. Propósito y Funcionalidad

El archivo "idc_load.py" cumple varias funciones importantes. En primer lugar, asegura que el dataset bronze exista. En segundo lugar, obtiene el archivo Excel más reciente desde GCS. En tercer lugar, descarga el archivo y obtiene la lista de hojas disponibles, siempre omitiendo la primera hoja que contiene metadatos. En cuarto lugar, procesa cada hoja restante aplicando transformaciones mínimas: elimina la primera fila de cada hoja que contiene metadatos, usa la segunda fila como encabezados, normaliza mínimamente los nombres de columnas para que BigQuery los acepte, y convierte todos los valores a STRING. Finalmente, carga cada hoja como una tabla separada en BigQuery con nombres específicos definidos en un mapeo.

## 3. Estructura del Archivo

El archivo está organizado en varias secciones. La primera sección contiene imports y configuración. La segunda sección contiene funciones helper como "ensure_dataset", "get_latest_excel_from_gcs_folder", "download_excel_from_gcs", y "get_excel_sheet_names". La tercera sección contiene funciones de transformación como "_minimal_normalize_for_bq" y "transform_excel_sheet". La cuarta sección contiene funciones para cargar datos a BigQuery como "load_dataframe_to_bq" y "load_all_sheets_to_bq". Finalmente, la quinta sección contiene funciones de limpieza.

## 4. Función get_excel_sheet_names

La función "get_excel_sheet_names" obtiene la lista de nombres de hojas en el archivo Excel, siempre omitiendo la primera hoja que es una hoja de estructura y metadatos. La función lee el archivo Excel usando "pd.ExcelFile" para obtener los nombres de todas las hojas. Si hay al menos una hoja, elimina la primera hoja del índice 0. Retorna la lista de hojas restantes. En modo debug, imprime información sobre las hojas encontradas y el orden esperado. Esta función es crítica porque el archivo Excel de IDC tiene una estructura específica donde la primera hoja no contiene datos reales.

## 5. Función _minimal_normalize_for_bq

La función "_minimal_normalize_for_bq" aplica normalización mínima a nombres de columnas solo para que BigQuery los acepte, manteniendo los nombres lo más similares posible al original. La función reemplaza espacios con guiones bajos, reemplaza caracteres problemáticos comunes con guiones bajos, elimina guiones bajos múltiples consecutivos, elimina guiones bajos al inicio y final, y limita la longitud a 300 caracteres que es el límite de BigQuery. Esta normalización es mínima intencionalmente porque la normalización completa se hace en la capa silver con dbt.

## 6. Función transform_excel_sheet

La función "transform_excel_sheet" lee y transforma mínimamente una hoja específica del Excel de IDC. La función lee la hoja sin usar encabezados automáticamente usando "header=None". Siempre elimina la primera fila que contiene metadatos y estructura. Usa la segunda fila como encabezados, aplicando normalización mínima a los nombres. Elimina la primera y segunda fila del DataFrame y asigna los encabezados normalizados. Convierte todas las columnas a STRING para preservar valores originales. Agrega una columna "fecha_lectura" con timestamp UTC y una columna "nombre_hoja" con el nombre de la hoja para trazabilidad. Retorna el DataFrame transformado.

## 7. Función load_all_sheets_to_bq

La función "load_all_sheets_to_bq" carga todas las hojas especificadas del Excel a BigQuery como tablas separadas. La función toma tres parámetros: "local_path" que es la ruta local del Excel, "dataset_id" que es el ID del dataset, y "sheet_to_table_mapping" que es un diccionario que mapea nombres de hojas a nombres de tablas. La función obtiene todas las hojas disponibles omitiendo siempre la primera. Verifica que haya suficientes hojas. Procesa cada hoja por índice según el orden en el mapeo, no por nombre, para ser más robusto. Para cada hoja, transforma los datos y los carga a BigQuery. Retorna un diccionario con el mapeo de hojas a tablas creadas.

## 8. Mapeo de Hojas a Tablas

El archivo Excel de IDC tiene una estructura específica con cuatro hojas: la primera hoja se elimina siempre, y las tres hojas restantes se mapean a tablas específicas. La segunda hoja generalmente se llama "Dato_original" y se mapea a "idc_raw_data_dato_original". La tercera hoja generalmente se llama "Valor_normalizado" y se mapea a "idc_raw_data_valor_normalizado". La cuarta hoja generalmente se llama "Valor_ranking" y se mapea a "idc_raw_data_valor_ranking". Este mapeo se define en los DAGs que llaman a esta función, no en el módulo mismo, para mantener flexibilidad.

## 9. Filosofía de Transformación Mínima

Al igual que "ipm_load.py", este módulo sigue una filosofía de transformación mínima. Todas las columnas se convierten a STRING para preservar valores originales. La normalización de nombres de columnas es mínima, solo lo necesario para que BigQuery acepte los nombres. La normalización completa, incluyendo conversión a snake_case y limpieza de datos, se realiza en la capa silver con dbt. Esta separación asegura que los datos originales siempre estén disponibles en bronze.

## 10. Cómo se Usa en el Código

El módulo se usa desde los DAGs de Airflow de IDC. Los DAGs primero aseguran que el dataset existe, luego obtienen el archivo más reciente desde GCS, lo descargan, y llaman a "load_all_sheets_to_bq" pasando el mapeo de hojas a tablas. El mapeo se construye basándose en la configuración de "config.yaml" que especifica los nombres de las tablas para cada tipo de dato. Los archivos temporales se limpian al final del proceso.

## 11. Notas Importantes

Es crítico que el archivo Excel tenga exactamente la estructura esperada con cuatro hojas, donde la primera es metadatos y las tres siguientes son datos. Si la estructura cambia, el código puede fallar. El código procesa las hojas por índice, no por nombre, lo cual es más robusto pero requiere que el orden de las hojas sea consistente. La función siempre elimina la primera fila de cada hoja de datos, asumiendo que contiene metadatos. Si esta suposición es incorrecta, los datos pueden procesarse incorrectamente. El esquema de BigQuery se infiere automáticamente del DataFrame, con todas las columnas como STRING excepto "fecha_lectura" que es TIMESTAMP.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/idc/idc_load.py"  
Versión del archivo: 3.0

