RESUMEN EJECUTIVO: TAREAS DEL PIPELINE DE TRANSFORMACION IPM

Este documento resume de forma concisa que hace cada tarea del pipeline de transformacion de datos del IPM y por que es importante para garantizar la calidad y confiabilidad de los datos.

CAPA BRONZE (Ingesta y Preservacion de Datos Originales)

1. ensure_dataset
   Que hace: Crea el dataset en BigQuery si no existe
   Por que es importante: Garantiza que la infraestructura este lista antes de cargar datos. Evita errores por falta de recursos.

2. download_excel
   Que hace: Busca y descarga automaticamente el archivo Excel mas reciente desde Google Cloud Storage
   Por que es importante: Asegura que siempre se procese la version mas actualizada del archivo sin intervencion manual. Reduce errores humanos al seleccionar el archivo correcto.

3. transform_dataframe
   Que hace: Convierte el Excel a un formato estandarizado, renombra columnas y convierte todo a texto para preservar los datos originales
   Por que es importante: Preserva los datos exactamente como vienen de la fuente, sin perdida de informacion. Esto permite auditar y depurar problemas posteriormente.

4. load_to_bq
   Que hace: Carga los datos preservados en BigQuery en la capa bronze
   Por que es importante: Crea una copia de seguridad de los datos originales. Permite reprocesar los datos sin necesidad de volver a descargar el Excel.

5. cleanup_temp_files
   Que hace: Elimina archivos temporales del sistema
   Por que es importante: Evita acumulacion de archivos que consumen espacio en disco y pueden causar problemas de rendimiento.

CAPA SILVER (Limpieza y Validacion de Datos)

6. ensure_dataset (silver)
   Que hace: Crea el dataset de la capa silver en BigQuery
   Por que es importante: Asegura que la infraestructura este lista para las transformaciones.

7. dbt_run_stg (Staging)
   Que hace: Estandariza los nombres de columnas de PascalCase a snake_case (formato estandar)
   Por que es importante: Facilita el trabajo posterior con nombres consistentes. Evita errores por diferencias en mayusculas y minusculas.

8. dbt_run_normalize_text
   Que hace: Normaliza texto eliminando acentos y estandarizando a mayusculas en codigos de municipio y nombres
   Por que es importante: Permite hacer comparaciones y uniones con otras tablas de manera confiable. Evita duplicados por diferencias de formato (ej: "Bogota" vs "Bogotá").

9. dbt_run_transform_types (ejecuta en paralelo con clean_numbers)
   Que hace: Intenta convertir valores de texto a numeros (enteros y decimales)
   Por que es importante: Permite realizar calculos matematicos y validaciones numericas. Identifica valores que no se pueden convertir para su posterior limpieza.

10. dbt_run_clean_numbers (ejecuta en paralelo con transform_types)
    Que hace: Limpia valores numericos eliminando letras, espacios y caracteres especiales mezclados con numeros
    Por que es importante: Corrige errores de captura donde se mezclaron letras con numeros (ej: "123abc" se convierte a "123"). Garantiza que los numeros sean validos para analisis.

11. dbt_run_detect_negatives
    Que hace: Detecta valores negativos y totales vacios o en cero, creando banderas de alerta
    Por que es importante: Identifica datos inconsistentes que no tienen sentido en el contexto del negocio (no puede haber poblacion negativa). Permite aplicar reglas de validacion automaticas.

12. dbt_run_apply_validations
    Que hace: Aplica reglas de negocio: convierte valores negativos a cero y maneja casos especiales
    Por que es importante: Garantiza la integridad de los datos aplicando reglas de negocio consistentes. Evita que datos invalidos lleguen a los reportes finales.

13. dbt_run_clean
    Que hace: Crea la tabla final limpia y valida, convirtiendo fechas al formato correcto
    Por que es importante: Materializa los datos completamente procesados y listos para consumo. Es la fuente de verdad para analisis y reportes.

14. dbt_test
    Que hace: Ejecuta pruebas automaticas de calidad de datos (valores nulos, rangos validos, etc.)
    Por que es importante: Valida automaticamente que los datos cumplan con los estandares de calidad. Detecta problemas antes de que lleguen a los usuarios finales.

CAPA GOLD (Datos Finales para Consumo)

15. ensure_dataset (gold)
    Que hace: Crea el dataset de la capa gold en BigQuery
    Por que es importante: Asegura que la infraestructura este lista para los datos finales.

16. dbt_run_gold
    Que hace: Crea la tabla final simplificada con solo los datos necesarios para consumo, renombra columnas a mayusculas
    Por que es importante: Proporciona una vista simplificada y optimizada para los usuarios finales. Facilita el uso de los datos sin exponer complejidades internas.

17. dbt_test (gold)
    Que hace: Ejecuta pruebas finales de calidad en los datos de consumo
    Por que es importante: Garantiza que los datos finales cumplan con los estandares antes de ser utilizados por los usuarios.

BENEFICIOS GENERALES DEL PIPELINE

- Automatizacion completa: Reduce el trabajo manual y los errores humanos
- Trazabilidad: Cada paso es auditable y se puede rastrear el origen de los datos
- Calidad garantizada: Múltiples capas de validacion aseguran datos confiables
- Eficiencia: Procesamiento paralelo donde es posible acelera la ejecucion
- Escalabilidad: El proceso puede manejar grandes volumenes de datos de manera consistente
- Mantenibilidad: Separacion de responsabilidades facilita el mantenimiento y actualizaciones

FLUJO GENERAL

Bronze: Preserva datos originales → Silver: Limpia y valida → Gold: Simplifica para consumo

Cada capa tiene un proposito claro y garantiza que los datos pasen por todas las validaciones necesarias antes de llegar a los usuarios finales.

