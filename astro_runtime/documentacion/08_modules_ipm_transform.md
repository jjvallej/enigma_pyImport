# Documentación: modules/ipm/ipm_transform.py

## 1. Información General

El archivo "modules/ipm/ipm_transform.py" es un módulo Python que proporciona funciones auxiliares para la gestión de datasets en BigQuery durante el proceso de transformación de datos del IPM desde bronze a silver y gold. Este módulo es relativamente simple y se enfoca principalmente en asegurar que los datasets necesarios existan antes de que dbt ejecute las transformaciones. Su propósito principal es proporcionar una función helper "ensure_dataset" que puede ser usada por los DAGs de transformación para garantizar que los datasets silver y gold estén disponibles antes de ejecutar los modelos de dbt.

## 2. Propósito y Funcionalidad

El archivo "ipm_transform.py" cumple una función específica y limitada en el proceso de transformación. Su función principal es proporcionar la función "ensure_dataset" que crea datasets en BigQuery si no existen. Esta función es útil porque dbt requiere que los datasets existan antes de poder crear tablas o vistas en ellos. A diferencia de otros módulos de transformación que ejecutan transformaciones directamente con SQL, este módulo delega las transformaciones reales a dbt, que se ejecuta mediante comandos bash desde los DAGs. El módulo simplemente prepara el entorno asegurando que los datasets estén disponibles.

## 3. Estructura del Archivo

El archivo está organizado de manera simple. Primero contiene imports y configuración, incluyendo la lectura de variables desde "config.yaml". Luego contiene una sección de funciones helper con la función "ensure_dataset" que es la función principal del módulo. El archivo es intencionalmente simple porque las transformaciones reales se realizan con dbt, no con código Python.

## 4. Función ensure_dataset

La función "ensure_dataset" crea el dataset en BigQuery si no existe. La función toma dos parámetros: "dataset_id" que es el ID del dataset sin el "project_id", y "location" que es la ubicación del dataset con valor por defecto "None" que usa "LOCATION" de "config.yaml". La función obtiene el cliente de BigQuery usando "get_bq_client". Construye el nombre completo del dataset como "PROJECT_ID.dataset_id". Intenta obtener el dataset existente. Si existe, imprime un mensaje de confirmación si está en modo debug. Si no existe, crea un nuevo dataset con la ubicación especificada. La descripción del dataset se actualiza según el tipo: si el dataset contiene "silver" en su nombre, la descripción indica que es la capa silver para IPM v2, si contiene "gold", indica que es la capa gold, y en otros casos usa una descripción genérica. Crea el dataset usando el cliente e imprime un mensaje de confirmación.

## 5. Diferencia con Otros Módulos de Transformación

Este módulo es diferente de otros módulos de transformación como "idc_transform.py" que ejecutan transformaciones directamente con SQL en BigQuery. En el caso de IPM, las transformaciones se realizan completamente con dbt, que se ejecuta desde los DAGs usando comandos bash. Este módulo solo proporciona funciones auxiliares para preparar el entorno, específicamente asegurando que los datasets existan. Esta separación de responsabilidades permite que las transformaciones se definan en archivos SQL de dbt, lo cual es más mantenible y permite versionado de las transformaciones.

## 6. Cómo se Usa en el Código

El módulo se usa principalmente desde los DAGs de Airflow que orquestan el proceso de transformación. Los DAGs importan la función "ensure_dataset" y la llaman antes de ejecutar los comandos dbt para asegurar que los datasets silver y gold existan. Esto es necesario porque dbt no crea datasets automáticamente, solo crea tablas y vistas dentro de datasets existentes. Después de asegurar que los datasets existen, los DAGs ejecutan comandos dbt usando "BashOperator" con los comandos generados por "get_dbt_command" de "modules.config".

## 7. Notas Importantes

Este módulo es intencionalmente simple porque las transformaciones reales se realizan con dbt. Si en el futuro se necesita ejecutar transformaciones directamente con SQL desde Python, se podría extender este módulo con funciones similares a las de "idc_transform.py". La función "ensure_dataset" es idempotente, lo que significa que puede llamarse múltiples veces sin efectos secundarios si el dataset ya existe. La descripción del dataset se determina automáticamente basándose en el nombre del dataset, lo cual es una heurística simple que funciona para los nombres de datasets estándar usados en el proyecto.

---

Última actualización: 2025-12-15  
Archivo documentado: "modules/ipm/ipm_transform.py"  
Versión del archivo: 3.0

