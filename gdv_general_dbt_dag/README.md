# GDV General DBT DAG

Plantilla dirigida a la construcción completa de la canalización IPM (Índice de Pobreza Multidimensional) dentro de Airflow. Esta carpeta reúne el código que define los DAGs de carga, limpieza y modelado, los módulos Python reutilizables que interactúan con Google Cloud Storage y BigQuery, y el proyecto dbt que materializa dimensiones y tablas de hechos. Con ella se controlan las etapas de ingestión desde archivos Excel, normalización de datos en capas RAW/BRONZE y generación analítica en las capas DIMS/GOLD.

Documentación detallada de la carpeta `gdv_general_dbt_dag`, que constituye el conjunto de DAGs y componentes ejecutados por el entorno descrito en `astro_runtime`.

---

## Estructura General

- **DAGs Airflow** (`*.py` en la raíz): orquestan cada fase de la canalización (carga RAW, limpieza BRONZE, construcción de dimensiones y fact).
- **Módulos Python** (`modules/`): implementan la lógica de ingestión y limpieza reutilizada por los DAGs.
- **Proyecto dbt** (`dbt/`): modelos y configuración que generan las tablas analíticas.
- **Subcarpetas auxiliares** (`api/`, `__pycache__`): reservadas para futuras integraciones o artefactos automáticos.

Cada componente cumple un rol específico para mover los datos desde Google Cloud Storage hasta las tablas finales en BigQuery.

---

## DAGs de Airflow

La carpeta raíz contiene los DAGs encargados de orquestar el pipeline. Todos son manuales (`schedule_interval=None`), con `catchup=False` y `start_date=2024-01-01`.

### 1. Capa RAW

- `load_rawdata_ipm_dag.py`
  - Descarga el archivo Excel de IPM desde GCS (`GCS_URI`) y lo requiere para poblar `gdv_ipm_sisben_raw.ipm_sisben_wide`.
  - Inyecta `modules` en `sys.path` para importar `load_ipm_wide_from_gcs_excel`.
  - Task principal: `PythonOperator(load_data_from_gcs)` que delega en el módulo correspondiente.

- `load_rawdata_dictionary_dag.py`
  - Similar al anterior pero para el diccionario de datos (`dictionary_wide`).
  - Limpia cachés `.pyc` al inicio para evitar inconsistencias en entornos donde se edita código en caliente.

Ambos DAGs son esenciales para que BigQuery reciba la información original sin transformaciones, habilitando la capa RAW del proyecto.

### 2. Capa BRONZE

- `clean_rawdata_ipm_dag.py`
  - Ejecuta la función `run_clean_to_bronze` de `modules.clean_rawdata_ipm`.
  - Normaliza códigos municipales, elimina caracteres extraños, asegura datasets en `us-central1` y escribe `gdv_ipm_sisben_bronze.ipm_sisben_clean`.

- `clean_rawdata_dictionary_dag.py`
  - Llama `run_clean_dictionary_to_bronze`.
  - Quita tildes y espacios inconsistentes del diccionario, generando `dictionary_clean`.

Estos DAGs son indispensables para pasar de la capa bruta a una capa estandarizada que facilite el modelado posterior.

### 3. Dimensiones (dbt)

Cada DAG crea (o recrea) el dataset `gdv_ipm_sisben_dims` en `us-central1` y ejecuta dbt sobre un modelo específico:

- `build_dim_municipio_dag.py` → `dbt run/test --select dim_municipio`
- `build_dim_nivel_pobreza_dag.py` → `dbt run/test --select dim_nivel_pobreza`
- `build_dim_categorias_ipm_dag.py` → `dbt run/test --select dim_categoria_ipm`
- `build_dim_tiempo_dag.py`
  - Similar al resto, pero además lee variables de Airflow (`ipm_dim_tiempo_anio`, `ipm_dim_tiempo_periodo`) o usa valores por defecto para ejecutar `dbt run --vars`.

Los DAGs de dimensiones aseguran que cada tabla se genere con las transformaciones correctas y se valide automáticamente con `dbt test`.

### 4. Tabla de hechos (dbt)

- `build_fact_ipm_dag.py`
  - Garantiza la existencia del dataset `gdv_ipm_sisben_gold`.
  - Ejecuta `dbt run/test --select gdv_ipm_sisben_fact` para combinar las dimensiones en una tabla wide final.

---

## Módulos Python (`modules/`)

Implementan la lógica de negocio compartida por los DAGs. Cada script está diseñado para ser invocado desde un `PythonOperator`.

- `load_rawdata_ipm.py`
  - Usa clientes GCS/BigQuery para descargar el Excel de IPM y subirlo a `ipm_sisben_wide`.
  - Detecta dinámicamente la fila de encabezados, normaliza códigos (`_normalize_cod_mpio`) y limpia valores numéricos antes de cargarlos.
  - Incluye diagnósticos (`DEBUG = True`) para inspeccionar problemas de codificación.

- `load_rawdata_dictionary.py`
  - Descarga el Excel del diccionario, detecta encabezados variables y construye una tabla ancha con columnas `Columna`, `Tipo_Valor`, `Valor`, `Descripcion`.
  - Verifica existencia del blob en GCS y lista alternativas en caso de error.

- `clean_rawdata_ipm.py`
  - Genera un SQL que elimina tildes, castea strings a números y filtra registros vacíos para producir `ipm_sisben_clean`.
  - Garantiza la existencia del dataset BRONZE en la región correcta.

- `clean_rawdata_dictionary.py`
  - Normaliza texto mediante `NORMALIZE` y `REGEXP_REPLACE` para remover diacríticos.
  - Crea o recrea datasets en `us-central1` según sea necesario.

- `__init__.py`
  - Marca el directorio como paquete Python.

Los módulos son imprescindibles: sin ellos los DAGs carecerían de lógica de ingestión/limpieza, y se duplicaría código; se diseñaron para ser reutilizables y testeables fuera de Airflow si se desea.

---

## Proyecto dbt (`dbt/`)

Estructura típica de un proyecto dbt concentrado en la carpeta:

- `dbt_project.yml`
  - Configuración principal del proyecto (`profile`, rutas de modelos, materialización por defecto).
  - Define que los modelos de `staging` se materializan como `view` y usan la base de datos `datagov-473122`.

- `macros/generate_schema_name.sql`
  - Macro que respeta el esquema personalizado si se define, o usa el esquema del target de dbt.

- `models/sources.yml`
  - Registra las fuentes de datos (`bronze` y `dims`) para referenciarlas en los modelos.

- `models/staging/*.sql`
  - `dim_municipio.sql`: vista que expone `cod_mpio`, nombre y población.
  - `dim_nivel_pobreza.sql`: vista con métricas de IPM pobre/no pobre por municipio.
  - `dim_categoria_ipm.sql`: despivotado de indicadores I1..I15 en filas por municipio.
  - `dim_tiempo.sql`: dimensión temporal mínima basada en variables de Airflow.

- `models/fact/gdv_ipm_sisben_fact.sql`
  - Tabla materializada que recompone la vista wide final usando las dimensiones.

- `target/`
  - Artefactos generados automáticamente por dbt (`manifest.json`, `run_results.json`, carpetas `compiled/` y `run/`). Útiles para debugging pero no se editan manualmente.

Esta estructura dbt permite versionar las transformaciones SQL y ejecutar `dbt run/test` desde los DAGs para asegurar calidad de datos.

---

## Otros elementos

- `api/`
  - Carpeta vacía incluida para futuros endpoints o integración con Astronomer Cosmos (por ejemplo, si se expusieran DAGs como servicios).

- `__pycache__/`
  - Artefactos generados por Python; no se documentan pero se mantienen por compatibilidad.

---

## Integración con el Entorno Astro Runtime

- Los DAGs y módulos se montan en `/opt/airflow/dags/gdv_general_dbt_dag` dentro de los contenedores definidos en `astro_runtime/docker-compose.yml`.
- Los módulos dependen de la ruta de credenciales `SA_PATH = "/opt/airflow/include/sa.json"` y de la variable `DBT_PROFILES_DIR` configurada en Astro para encontrar `profiles.yml`.
- Los DAGs con BashOperator ejecutan `dbt` gracias a la instalación realizada en la imagen Docker personal.
- Los archivos generados por dbt (`target/`) quedan disponibles para inspección en la máquina host.

---

## Razones por las que cada elemento es necesario

| Elemento | Rol esencial |
| --- | --- |
| DAGs `load_*_dag.py` | Ingesta inicial de datos desde GCS hacia la capa RAW en BigQuery. |
| DAGs `clean_*_dag.py` | Estándar de calidad y normalización antes de modelar. |
| DAGs `build_dim_*_dag.py` | Construcción controlada de dimensiones en la capa DIMS mediante dbt. |
| DAG `build_fact_ipm_dag.py` | Generación de la tabla de hechos final en la capa GOLD. |
| Módulos `load_*` | Lógica reutilizable para leer Excel y subir data; separa la implementación de la orquestación. |
| Módulos `clean_*` | SQL de limpieza empaquetado reutilizable, maneja creación de datasets y normalización de texto/números. |
| Proyecto dbt (`models/`, `macros/`, `dbt_project.yml`) | Declaración de modelos analíticos versionados, ejecutados por los DAGs. |
| Carpeta `api/` | Espacio reservado para extensiones futuras (por ejemplo, endpoints o Cosmos). |

---

## Buenas Prácticas y Mantenimiento

- **Actualización de URIs**: las rutas `GCS_URI` en los DAGs de carga deben mantenerse alineadas con el almacenamiento real.
- **Desactivar DEBUG**: `modules/load_rawdata_ipm.py` tiene `DEBUG = True`; conviene ajustarlo a `False` en producción para reducir ruido.
- **Variables de Airflow**: ajustar `ipm_dim_tiempo_anio` y `ipm_dim_tiempo_periodo` para cada corte temporal deseado.
- **Extensión dbt**: nuevas transformaciones pueden agregarse en `models/staging` y referenciarse en DAGs adicionales.
- **Monitoreo**: los logs generados por los DAGs se almacenan (vía Astro Runtime) en `astro_runtime/logs`, facilitando depuración.

---

Esta documentación cubre la función de cada archivo en `gdv_general_dbt_dag`, su interacción con Astro Runtime y el propósito de la plantilla para ejecutar la canalización IPM completa. Ajustes específicos (URIs, nombres de datasets, regiones) deben adaptarse a los entornos de despliegue donde se utilice.


